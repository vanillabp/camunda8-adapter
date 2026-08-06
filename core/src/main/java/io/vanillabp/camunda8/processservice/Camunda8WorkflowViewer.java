package io.vanillabp.camunda8.processservice;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.camunda.client.api.search.enums.ElementInstanceType;
import io.camunda.client.api.search.response.ElementInstance;
import io.camunda.client.api.search.response.ProcessInstance;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.CallActivity;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeCalledElement;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.deployment.Camunda8DeployedProcesses;
import io.vanillabp.spi.process.ProcessDefinition;
import io.vanillabp.spi.process.WorkflowElementHistory;
import io.vanillabp.spi.process.WorkflowElementType;
import io.vanillabp.spi.process.WorkflowHistory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The Camunda 8 part of VanillaBP's viewer/history API (story 26).
 * <p>
 * <b>Two data sources, deliberately:</b>
 * <ol>
 * <li><b>What this application version deployed</b>
 * ({@link Camunda8DeployedProcesses}) - definitions and BPMN XML are served from
 * the models VanillaBP's deployment pipeline reads at EVERY boot. No cluster
 * round trip, no eventual consistency, works without secondary storage.</li>
 * <li><b>The cluster's query API</b> (secondary storage) - which version a
 * RUNNING workflow actually uses, the workflow timeline, and definitions deployed
 * by PREVIOUS application versions (a long-running workflow surviving a
 * redeployment). Without secondary storage the adapter degrades honestly: the
 * definitions of the currently deployed version are reported and the element
 * history is <code>null</code> (the SPI's documented "not supported by the
 * underlying BPMS"), never an error.</li>
 * </ol>
 * <b>Eventual consistency:</b> the query API lags behind the engine by design. A
 * workflow started moments ago may not be visible yet - this is reported as
 * "no history (yet)", never as a failure. Viewers polling shortly after will see
 * it.
 * <p>
 * The <b>history context</b> of this adapter is the called instance's PROCESS
 * INSTANCE KEY (of a call activity), and the <b>adapter-native definition id</b>
 * is the process definition key.
 */
@Slf4j
@RequiredArgsConstructor
public class Camunda8WorkflowViewer {

  private final String adapterId;

  private final Camunda8ClientFactory clientFactory;

  /**
   * Logged once per adapter: the viewer needs the query API (secondary storage)
   * for instance-related data.
   */
  private final AtomicBoolean noSecondaryStorageWarned = new AtomicBoolean();

  /**
   * The process definitions of the addressed (sub-)workflow.
   *
   * @param workflowModuleId The workflow module id
   * @param bpmnProcessId The BPMN process id of the primary process
   * @param aggregateIdName The name of the process variable holding the aggregate ID
   * @param workflowAggregateId The workflow aggregate's ID
   * @param historyContext <code>null</code> or a called instance's process
   *        instance key
   * @return The definitions or an EMPTY list if the workflow is unknown here
   */
  public List<ProcessDefinition> getProcessDefinitions(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String aggregateIdName,
      final Object workflowAggregateId,
      final String historyContext) {

    final var instance = findInstance(
        workflowModuleId, bpmnProcessId, aggregateIdName, workflowAggregateId, historyContext);

    final Definition definition;
    if (instance != null) {
      definition = definitionOf(instance);
    } else if (historyContext != null) {
      // a context which cannot be resolved (query API unavailable or the instance
      // is gone) must not silently answer with the primary process's definitions
      return List.of();
    } else {
      definition = deployedDefinition(workflowModuleId, bpmnProcessId);
    }
    if (definition == null) {
      return List.of();
    }

    final var definitions = new ArrayList<ProcessDefinition>();
    definitions.add(
        new ProcessDefinition(
            definition.processDefinitionKey(), definition.bpmnProcessId(), String.valueOf(definition.version()), null));
    definitions.addAll(
        calledDefinitions(workflowModuleId, definition));
    return definitions;

  }

  /**
   * The BPMN XML of a process definition: from what this application version
   * deployed or - for definitions of previous application versions - from the
   * cluster.
   *
   * @param processDefinitionId The Camunda 8 process definition key
   * @return The XML or <code>null</code> if unknown
   */
  public InputStream getBpmnXml(
      final String processDefinitionId) {

    final var deployed = clientFactory
        .getDeployedProcesses()
        .byDefinitionKey(processDefinitionId);
    if (deployed != null) {
      return toInputStream(Bpmn.convertToString(deployed.model()));
    }

    // deployed by a PREVIOUS application version: only the cluster still has it
    try {
      final var xml = clientFactory
          .getClient()
          .newProcessDefinitionGetXmlRequest(Long.parseLong(processDefinitionId))
          .send()
          .join();
      return xml == null
          ? null
          : toInputStream(xml);
    } catch (final NumberFormatException e) {
      return null;
    } catch (final Exception e) {
      log.info(
          "Camunda8[{}]: the process definition '{}' was not deployed by this application version "
              + "and could not be read from the cluster - if the cluster runs without secondary "
              + "storage, only definitions of the RUNNING application version are available",
          adapterId,
          processDefinitionId,
          e);
      return null;
    }

  }

  /**
   * The execution history of the addressed (sub-)workflow.
   *
   * @param workflowModuleId The workflow module id
   * @param bpmnProcessId The BPMN process id of the primary process
   * @param aggregateIdName The name of the process variable holding the aggregate ID
   * @param workflowAggregateId The workflow aggregate's ID
   * @param historyContext <code>null</code> or a called instance's process
   *        instance key
   * @return The history or <code>null</code> if the workflow is unknown here
   */
  public WorkflowHistory getWorkflowHistory(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String aggregateIdName,
      final Object workflowAggregateId,
      final String historyContext) {

    final var instance = findInstance(
        workflowModuleId, bpmnProcessId, aggregateIdName, workflowAggregateId, historyContext);
    if (instance == null) {
      if (historyContext != null) {
        return null;
      }
      // no query API (or the workflow is not visible yet): report the definition
      // which would be executed - the element history is unavailable, which the
      // SPI expresses as null (NOT an error, see the class comment)
      final var deployed = deployedDefinition(workflowModuleId, bpmnProcessId);
      return deployed == null
          ? null
          : new WorkflowHistory(deployed.processDefinitionKey(), null, null, null);
    }

    final var elementInstances = elementInstancesOf(instance.getProcessInstanceKey());
    if (elementInstances == null) {
      return new WorkflowHistory(
          String.valueOf(instance.getProcessDefinitionKey()), instance.getStartDate(), instance.getEndDate(), null);
    }

    final var incidentMessages = openIncidentMessagesByElement(instance.getProcessInstanceKey());
    final var elements = elementInstances
        .stream()
        .map(elementInstance -> new WorkflowElementHistory(
            elementInstance.getStartDate(), elementInstance.getEndDate(), elementInstance.getElementId(), elementTypeOf(
                elementInstance.getType()), incidentMessages.get(elementInstance.getElementId()), elementInstance
                    .getState() == io.camunda.client.api.search.enums.ElementInstanceState.TERMINATED, calledInstanceKeyOf(
                        elementInstance)))
        .toList();

    return new WorkflowHistory(
        String.valueOf(instance.getProcessDefinitionKey()), instance.getStartDate(), instance.getEndDate(), elements);

  }

  /**
   * The definition a (sub-)workflow runs on, plus the model needed to determine
   * its call activities.
   *
   * @param processDefinitionKey The adapter-native definition id
   * @param bpmnProcessId The BPMN process id
   * @param version The version
   * @param model The BPMN model or <code>null</code> if unavailable (a definition
   *        of a previous application version on a cluster without secondary
   *        storage)
   */
  private record Definition(
                            String processDefinitionKey,
                            String bpmnProcessId,
                            int version,
                            BpmnModelInstance model) {
  }

  private Definition definitionOf(
      final ProcessInstance instance) {

    final var processDefinitionKey = String.valueOf(instance.getProcessDefinitionKey());
    final var deployed = clientFactory
        .getDeployedProcesses()
        .byDefinitionKey(processDefinitionKey);
    if (deployed != null) {
      return new Definition(
          processDefinitionKey, deployed.bpmnProcessId(), deployed.version(), deployed.model());
    }

    // running on a definition of a PREVIOUS application version: read its XML
    // from the cluster so the call activities stay resolvable
    BpmnModelInstance model = null;
    try (var xml = getBpmnXml(processDefinitionKey)) {
      if (xml != null) {
        model = Bpmn.readModelFromStream(xml);
      }
    } catch (final Exception e) {
      log.debug(
          "Camunda8[{}]: could not read the BPMN of process definition '{}' from the cluster",
          adapterId,
          processDefinitionKey,
          e);
    }
    return new Definition(
        processDefinitionKey, instance.getProcessDefinitionId(), instance.getProcessDefinitionVersion() == null
            ? 0
            : instance.getProcessDefinitionVersion(), model);

  }

  private Definition deployedDefinition(
      final String workflowModuleId,
      final String bpmnProcessId) {

    final var deployed = clientFactory
        .getDeployedProcesses()
        .deployedVersionOf(workflowModuleId, bpmnProcessId);
    return deployed == null
        ? null
        : new Definition(
            deployed.processDefinitionKey(), deployed.bpmnProcessId(), deployed.version(), deployed.model());

  }

  /**
   * The definitions called by the call activities of the given definition, in the
   * version which WOULD be executed next: the version deployed by this
   * application. Call activities addressing their process by a FEEL expression
   * are skipped - the called definition is only known at execution time.
   */
  private List<ProcessDefinition> calledDefinitions(
      final String workflowModuleId,
      final Definition definition) {

    if (definition.model() == null) {
      return List.of();
    }

    final var elementsByCalledProcess = new LinkedHashMap<String, List<String>>();
    for (final var callActivity : definition
        .model()
        .getModelElementsByType(CallActivity.class)) {
      final var calledProcessId = calledProcessIdOf(callActivity);
      if (calledProcessId == null) {
        continue;
      }
      elementsByCalledProcess
          .computeIfAbsent(calledProcessId, key -> new ArrayList<>())
          .add(callActivity.getId());
    }

    final var definitions = new ArrayList<ProcessDefinition>();
    elementsByCalledProcess.forEach((
        calledProcessId,
        elementIds) -> {
      final var deployed = clientFactory
          .getDeployedProcesses()
          .deployedVersionOf(workflowModuleId, calledProcessId);
      if (deployed == null) {
        log.debug(
            "Camunda8[{}]: the called process '{}' of workflow module '{}' was not deployed by "
                + "this application version - the call activities {} are not reported",
            adapterId,
            calledProcessId,
            workflowModuleId,
            elementIds);
        return;
      }
      definitions.add(
          new ProcessDefinition(
              deployed.processDefinitionKey(), deployed.bpmnProcessId(), String.valueOf(deployed.version()), List
                  .copyOf(elementIds)));
    });
    return definitions;

  }

  /**
   * Reads {@code zeebe:calledElement processId} of a call activity - a static
   * process id only; an expression ({@code =...}) is not resolvable here.
   */
  private static String calledProcessIdOf(
      final CallActivity callActivity) {

    final var calledElement = callActivity.getSingleExtensionElement(ZeebeCalledElement.class);
    if (calledElement == null) {
      return null;
    }
    final var processId = calledElement.getProcessId();
    if ((processId == null) || processId.isBlank() || processId.startsWith("=")) {
      return null;
    }
    return processId;

  }

  /**
   * Finds the process instance addressed: the workflow's primary instance (by the
   * aggregate-ID variable) or - for a history context - the called instance,
   * accepted ONLY if its root instance is that very workflow.
   *
   * @return The instance or <code>null</code> (unknown, not visible yet, or no
   *         query API)
   */
  private ProcessInstance findInstance(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String aggregateIdName,
      final Object workflowAggregateId,
      final String historyContext) {

    final var primaryInstance = searchOne(
        filter -> filter
            .processDefinitionId(bpmnProcessId)
            .variables(java.util.Map.of(aggregateIdName, String.valueOf(workflowAggregateId))));
    if (historyContext == null) {
      return primaryInstance;
    }
    if (primaryInstance == null) {
      return null;
    }

    final long calledInstanceKey;
    try {
      calledInstanceKey = Long.parseLong(historyContext);
    } catch (final NumberFormatException e) {
      return null;
    }
    final var calledInstance = searchOne(filter -> filter.processInstanceKey(calledInstanceKey));
    if (calledInstance == null) {
      return null;
    }
    if (!belongsTo(calledInstance, primaryInstance.getProcessInstanceKey())) {
      log.warn(
          "Camunda8[{}]: the history context '{}' does not belong to the workflow of aggregate "
              + "'{}' (BPMN process '{}', workflow module '{}') - ignoring it",
          adapterId,
          historyContext,
          workflowAggregateId,
          bpmnProcessId,
          workflowModuleId);
      return null;
    }
    return calledInstance;

  }

  /**
   * Whether the given instance is (a descendant of) the workflow's primary
   * instance - the parent chain is walked with a sane bound.
   */
  private boolean belongsTo(
      final ProcessInstance instance,
      final Long primaryInstanceKey) {

    var current = instance;
    for (var level = 0; level < 20; ++level) {
      if (primaryInstanceKey.equals(current.getProcessInstanceKey())) {
        return true;
      }
      final var parentKey = current.getParentProcessInstanceKey();
      if (parentKey == null) {
        return false;
      }
      current = searchOne(filter -> filter.processInstanceKey(parentKey));
      if (current == null) {
        return false;
      }
    }
    return false;

  }

  private ProcessInstance searchOne(
      final java.util.function.Consumer<io.camunda.client.api.search.filter.ProcessInstanceFilter> filter) {

    try {
      return clientFactory
          .getClient()
          .newProcessInstanceSearchRequest()
          .filter(filter)
          .send()
          .join()
          .items()
          .stream()
          .findFirst()
          .orElse(null);
    } catch (final Exception e) {
      warnNoSecondaryStorage(e, "process instances");
      return null;
    }

  }

  /**
   * @return The element instances in execution order or <code>null</code> if the
   *         query API is unavailable (the SPI's "history not supported")
   */
  private List<ElementInstance> elementInstancesOf(
      final Long processInstanceKey) {

    try {
      return clientFactory
          .getClient()
          .newElementInstanceSearchRequest()
          .filter(filter -> filter.processInstanceKey(processInstanceKey))
          .sort(sort -> sort
              .startDate()
              .asc())
          .send()
          .join()
          .items();
    } catch (final Exception e) {
      warnNoSecondaryStorage(e, "element instances");
      return null;
    }

  }

  private java.util.Map<String, String> openIncidentMessagesByElement(
      final Long processInstanceKey) {

    final var messages = new java.util.HashMap<String, String>();
    try {
      clientFactory
          .getClient()
          .newIncidentsByProcessInstanceSearchRequest(processInstanceKey)
          .send()
          .join()
          .items()
          .forEach(incident -> {
            if (incident.getState() == io.camunda.client.api.search.enums.IncidentState.ACTIVE) {
              messages.putIfAbsent(incident.getElementId(), incident.getErrorMessage());
            }
          });
    } catch (final Exception e) {
      log.debug(
          "Camunda8[{}]: incidents of process instance '{}' are unavailable - the history is "
              + "reported without error messages",
          adapterId,
          processInstanceKey,
          e);
    }
    return messages;

  }

  /**
   * The called instance's key of a call activity element - the SPI's secondary
   * history context to dig into the sub-process.
   */
  private String calledInstanceKeyOf(
      final ElementInstance elementInstance) {

    if (elementInstance.getType() != ElementInstanceType.CALL_ACTIVITY) {
      return null;
    }
    final var elementInstanceKey = elementInstance.getElementInstanceKey();
    final var calledInstance = searchOne(
        filter -> filter.parentElementInstanceKey(elementInstanceKey));
    return calledInstance == null
        ? null
        : String.valueOf(calledInstance.getProcessInstanceKey());

  }

  private void warnNoSecondaryStorage(
      final Exception exception,
      final String subject) {

    if (noSecondaryStorageWarned.compareAndSet(false, true)) {
      log.warn(
          "Camunda8[{}]: the viewer/history API could not query {} - if the cluster runs WITHOUT "
              + "secondary storage, process definitions are served from what this application "
              + "version deployed and the element history stays unavailable (reported as 'no "
              + "history', never as an error). Configure the query API (secondary storage) for "
              + "the full viewer experience.",
          adapterId,
          subject,
          exception);
    } else {
      log.debug(
          "Camunda8[{}]: the viewer/history API could not query {}",
          adapterId,
          subject,
          exception);
    }

  }

  private static InputStream toInputStream(
      final String xml) {

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));

  }

  /**
   * Maps Camunda 8's element instance types onto the SPI's element types.
   */
  static WorkflowElementType elementTypeOf(
      final ElementInstanceType type) {

    if (type == null) {
      return WorkflowElementType.UNKNOWN;
    }
    return switch (type) {
      case PROCESS -> WorkflowElementType.PROCESS;
      case SUB_PROCESS -> WorkflowElementType.SUB_PROCESS;
      case EVENT_SUB_PROCESS -> WorkflowElementType.EVENT_SUB_PROCESS;
      case AD_HOC_SUB_PROCESS, AD_HOC_SUB_PROCESS_INNER_INSTANCE -> WorkflowElementType.AD_HOC_SUB_PROCESS;
      case START_EVENT -> WorkflowElementType.START_EVENT;
      case INTERMEDIATE_CATCH_EVENT -> WorkflowElementType.INTERMEDIATE_CATCH_EVENT;
      case INTERMEDIATE_THROW_EVENT -> WorkflowElementType.INTERMEDIATE_THROW_EVENT;
      case BOUNDARY_EVENT -> WorkflowElementType.BOUNDARY_EVENT;
      case END_EVENT -> WorkflowElementType.END_EVENT;
      case SERVICE_TASK -> WorkflowElementType.SERVICE_TASK;
      case RECEIVE_TASK -> WorkflowElementType.RECEIVE_TASK;
      case USER_TASK -> WorkflowElementType.USER_TASK;
      case MANUAL_TASK -> WorkflowElementType.MANUAL_TASK;
      case TASK -> WorkflowElementType.TASK;
      case EXCLUSIVE_GATEWAY -> WorkflowElementType.EXCLUSIVE_GATEWAY;
      case INCLUSIVE_GATEWAY -> WorkflowElementType.INCLUSIVE_GATEWAY;
      case PARALLEL_GATEWAY -> WorkflowElementType.PARALLEL_GATEWAY;
      case EVENT_BASED_GATEWAY -> WorkflowElementType.EVENT_BASED_GATEWAY;
      case SEQUENCE_FLOW -> WorkflowElementType.SEQUENCE_FLOW;
      case MULTI_INSTANCE_BODY -> WorkflowElementType.MULTI_INSTANCE;
      case CALL_ACTIVITY -> WorkflowElementType.CALL_ACTIVITY;
      case BUSINESS_RULE_TASK -> WorkflowElementType.BUSINESS_RULE_TASK;
      case SCRIPT_TASK -> WorkflowElementType.SCRIPT_TASK;
      case SEND_TASK -> WorkflowElementType.SEND_TASK;
      default -> WorkflowElementType.UNKNOWN;
    };

  }

}
