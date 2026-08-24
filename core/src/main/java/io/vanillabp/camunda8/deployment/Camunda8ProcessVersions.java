package io.vanillabp.camunda8.deployment;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.camunda.client.CamundaClient;
import io.vanillabp.integration.adapter.spi.version.CachingProcessVersionCatalog;
import io.vanillabp.integration.adapter.spi.version.DeployedProcessVersion;
import lombok.extern.slf4j.Slf4j;

/**
 * The versions of the process definitions of ONE Camunda 8 cluster (= one adapter id):
 * what the core matches <code>&#64;WorkflowTask(version = ...)</code> and its siblings
 * against.
 * <p>
 * The version itself travels with every job ({@code ActivatedJob#getProcessDefinitionVersion}),
 * so nothing here is needed for version specifications made of numbers. Version TAGS
 * are a different matter: a job does not carry one, and the cluster only tells which
 * version carries which tag through the query API (secondary storage). Where a cluster
 * runs without it, the tag stays unknown and is reported once - version specifications
 * made of numbers keep working.
 * <p>
 * What the deployment reported is recorded without any query
 * ({@link #recordDeployed(String, String, int, String)}): the deploy command
 * names the version the cluster assigned, and the model carries its
 * {@code zeebe:versionTag}.
 */
@Slf4j
// no Lombok here: the accessors are the deliberate surface of this class,
// and generating them would hide which of its fields are meant to be read
@SuppressWarnings("LombokSetterMayBeUsed")
public class Camunda8ProcessVersions extends CachingProcessVersionCatalog {

  private final String adapterId;

  private final java.util.function.Supplier<CamundaClient> client;

  /**
   * The BPMN process id as the CLUSTER knows it for a (workflow module, plain BPMN
   * process id) - the identifiers may be prefixed.
   */
  private final BiFunction<String, String, String> scopedProcessIds;

  /**
   * The tenant a workflow module is deployed to, or <code>null</code>.
   */
  private final Function<String, String> tenants;

  private final AtomicBoolean noQueryApiWarned = new AtomicBoolean();

  /**
   * Reads the tasks of a model the cluster holds - the deployment service' own
   * extraction.
   */
  @FunctionalInterface
  public interface TasksOfModel {

    java.util.Collection<io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec> of(
        String workflowModuleId,
        String bpmnProcessId,
        String version,
        io.camunda.zeebe.model.bpmn.BpmnModelInstance model);

  }

  private TasksOfModel tasksOfModel;

  /**
   * @param tasksOfModel How the deployment service reads a model
   */
  public void setTasksOfModel(
      final TasksOfModel tasksOfModel) {

    this.tasksOfModel = tasksOfModel;

  }

  @Override
  public java.util.Collection<io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec> tasksOfVersion(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String version) {

    if (tasksOfModel == null) {
      return null;
    }
    final var definitionKey = definitionKeyOf(workflowModuleId, bpmnProcessId, version);
    if (definitionKey == null) {
      // no query API, or the cluster does not hold that version any more - the core
      // says once that this BPMS cannot tell
      return null;
    }
    try {
      final var xml = client.get().newProcessDefinitionGetXmlRequest(definitionKey).send().join();
      if (xml == null) {
        return java.util.List.of();
      }
      final var model = io.camunda.zeebe.model.bpmn.Bpmn
          .readModelFromStream(new java.io.ByteArrayInputStream(xml.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      return tasksOfModel.of(workflowModuleId, bpmnProcessId, version, model);
    } catch (final RuntimeException e) {
      log.warn(
          "Camunda8[{}]: the model of version {} of BPMN process '{}' (workflow module '{}') could not be read, "
              + "so VanillaBP cannot tell whether this application still serves it",
          adapterId,
          version,
          bpmnProcessId,
          workflowModuleId,
          e);
      return null;
    }

  }

  @Override
  public Long activeInstanceCountOf(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String version) {

    final var definitionKey = definitionKeyOf(workflowModuleId, bpmnProcessId, version);
    if (definitionKey == null) {
      return null;
    }
    try {
      final var found = client
          .get()
          .newProcessInstanceSearchRequest()
          .filter(filter -> filter
              .state(io.camunda.client.api.search.enums.ProcessInstanceState.ACTIVE)
              .processDefinitionKey(definitionKey))
          .send()
          .join();
      return (long) found.items().size();
    } catch (final RuntimeException e) {
      if (isSecondaryStorageMissing(e)) {
        return null;
      }
      throw e;
    }

  }

  /**
   * The cluster's process definition key of ONE version of a process - the handle both
   * the model and the instance count are read by. Needs the query API.
   */
  private Long definitionKeyOf(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String version) {

    if (!version.matches("\\d+")) {
      return null;
    }
    final var scopedProcessId = scopedProcessIds.apply(workflowModuleId, bpmnProcessId);
    final var tenantId = tenants.apply(workflowModuleId);
    try {
      return client
          .get()
          .newProcessDefinitionSearchRequest()
          .filter(filter -> {
            filter.processDefinitionId(scopedProcessId);
            filter.version(Integer.valueOf(version));
            if (tenantId != null) {
              filter.tenantId(tenantId);
            }
          })
          .send()
          .join()
          .items()
          .stream()
          .findFirst()
          .map(definition -> definition.getProcessDefinitionKey())
          .orElse(null);
    } catch (final RuntimeException e) {
      if (isSecondaryStorageMissing(e)) {
        return null;
      }
      throw e;
    }

  }

  public Camunda8ProcessVersions(
      final String adapterId,
      final java.util.function.Supplier<CamundaClient> client,
      final BiFunction<String, String, String> scopedProcessIds,
      final Function<String, String> tenants) {

    this.adapterId = adapterId;
    this.client = client;
    this.scopedProcessIds = scopedProcessIds;
    this.tenants = tenants;

  }

  /**
   * Remembers a version the deploy command reported.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The PLAIN BPMN process ID
   * @param version The version the cluster assigned
   * @param versionTag The <code>zeebe:versionTag</code> of the model or
   *          <code>null</code>
   */
  public void recordDeployed(
      final String workflowModuleId,
      final String bpmnProcessId,
      final int version,
      final String versionTag) {

    record(workflowModuleId, bpmnProcessId, DeployedProcessVersion.of(String.valueOf(version), versionTag));

  }

  @Override
  protected List<DeployedProcessVersion> fetchDeployedVersions(
      final String workflowModuleId,
      final String bpmnProcessId) {

    final var scopedProcessId = scopedProcessIds.apply(workflowModuleId, bpmnProcessId);
    final var tenantId = tenants.apply(workflowModuleId);
    try {
      return client
          .get()
          .newProcessDefinitionSearchRequest()
          .filter(filter -> {
            filter.processDefinitionId(scopedProcessId);
            if (tenantId != null) {
              filter.tenantId(tenantId);
            }
          })
          .sort(sort -> sort.version().asc())
          .send()
          .join()
          .items()
          .stream()
          .map(definition -> DeployedProcessVersion
              .of(String.valueOf(definition.getVersion()), definition.getVersionTag()))
          .toList();
    } catch (final RuntimeException e) {
      if (isSecondaryStorageMissing(e)) {
        if (noQueryApiWarned.compareAndSet(false, true)) {
          log.warn(
              """
                  Camunda8[{}]: the cluster runs WITHOUT secondary storage, so the versions of BPMN \
                  process '{}' (workflow module '{}') cannot be asked for. Version specifications \
                  made of numbers (e.g. '>2') keep working - specifications naming a version TAG \
                  match nothing until the query API is configured (camunda.database.type / \
                  secondary storage).""",
              adapterId,
              bpmnProcessId,
              workflowModuleId);
        }
        return List.of();
      }
      throw e;
    }

  }

  private static boolean isSecondaryStorageMissing(
      final Throwable throwable) {

    var current = throwable;
    while (current != null) {
      final var message = current.getMessage();
      if ((message != null) && message.contains("secondary storage")) {
        return true;
      }
      current = current.getCause();
    }
    return false;

  }

}
