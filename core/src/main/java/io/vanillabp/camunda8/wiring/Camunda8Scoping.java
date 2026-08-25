package io.vanillabp.camunda8.wiring;

import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.Error;
import io.camunda.zeebe.model.bpmn.instance.Escalation;
import io.camunda.zeebe.model.bpmn.instance.FlowElement;
import io.camunda.zeebe.model.bpmn.instance.Message;
import io.camunda.zeebe.model.bpmn.instance.Process;
import io.camunda.zeebe.model.bpmn.instance.Signal;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeCalledElement;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeFormDefinition;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskDefinition;
import io.vanillabp.integration.adapter.spi.NameClashAvoidance;
import io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport;
import lombok.extern.slf4j.Slf4j;

/**
 * Applies {@link NameClashAvoidance#USE_PREFIX} to a Camunda 8 model:
 * every identifier a cluster resolves GLOBALLY is prefixed, so two workflow modules
 * may use the same names without a tenant.
 *
 * <table>
 * <caption>What is rewritten</caption>
 * <tr><th>Element</th><th>Scoped by</th><th>Why</th></tr>
 * <tr><td>{@code bpmn:process id}</td><td>workflow module</td><td>the process id addresses a process definition cluster-wide</td></tr>
 * <tr><td>{@code zeebe:calledElement processId}</td><td>workflow module</td><td>a call activity has to address the renamed process</td></tr>
 * <tr><td>{@code bpmn:message name}</td><td>workflow module</td><td>messages are published and correlated by name</td></tr>
 * <tr><td>{@code bpmn:signal name}, {@code bpmn:escalation escalationCode}</td><td>workflow module</td><td>broadcast by name</td></tr>
 * <tr><td>{@code bpmn:error errorCode}</td><td>workflow module</td><td>completeness - a code is process-local, but the application may throw it via {@code ProcessService#cancelTask}</td></tr>
 * <tr><td>{@code zeebe:taskDefinition type}</td><td>workflow module + process</td><td>job types are what workers subscribe to, cluster-wide</td></tr>
 * <tr><td>{@code zeebe:formDefinition externalReference}</td><td>workflow module + process</td><td>it IS the user task's task definition and becomes a listener job type</td></tr>
 * </table>
 *
 * The rewriting happens in <code>prepareBpmn</code>, BEFORE wiring: everything after
 * it - the wiring validation, the listener injection, the workers - therefore sees
 * the identifiers the cluster will see, while the core keeps working with the plain
 * ones (the adapter translates at every boundary).
 * <p>
 * Why the deployed bytes carry the scoped identifiers while the registries stay plain is decision 2
 * in the repository's DECISIONS.md; that this rewrite is one of the model changes the adapter makes
 * is decision 5 in the repository's DECISIONS.md.
 */
@Slf4j
public final class Camunda8Scoping {

  private Camunda8Scoping() {
  }

  /**
   * The TENANT a workflow module is deployed to, respectively an operation of it runs
   * in: the workflow module id unless the adapter configured a name, and
   * <code>null</code> wherever the mode is not {@link NameClashAvoidance#BY_ADAPTER}
   * ({@code none} uses no tenant, and under {@code use-prefix} the prefix IS the
   * isolation - a tenant on top would defeat the purpose, since clusters are licensed
   * per tenant).
   * <p>
   * That a tenant is what {@code by-adapter} means here is CAMUNDA 8 knowledge, so it
   * lives in the adapter: the core answers the mode and nothing else.
   *
   * @param scoping The core's name-clash-avoidance support, or <code>null</code>
   *          (tests): the configured tenant is used as it is then
   * @param workflowModuleId The workflow module ID
   * @param adapterId The adapter ID
   * @param configuredTenantId The tenant name configured for the adapter, or
   *          <code>null</code>
   * @return The tenant ID, or <code>null</code> if the mode uses none
   */
  public static String tenantIdFor(
      final NameClashAvoidanceSupport scoping,
      final String workflowModuleId,
      final String adapterId,
      final String configuredTenantId) {

    final var configured = (configuredTenantId != null) && !configuredTenantId.isBlank()
        ? configuredTenantId
        : null;
    if (scoping == null) {
      return configured;
    }
    if (scoping.modeFor(workflowModuleId, null, adapterId) != NameClashAvoidance.BY_ADAPTER) {
      return null;
    }
    return configured != null
        ? configured
        : workflowModuleId;

  }

  /**
   * Rewrites the identifiers of the given model in place. A no-op unless the mode of
   * the workflow module is {@link NameClashAvoidance#USE_PREFIX}.
   *
   * @param model The model of one BPMN file
   * @param workflowModuleId The workflow module ID
   * @param adapterId The adapter ID
   * @param scoping The core's name-clash-avoidance support
   */
  public static void apply(
      final BpmnModelInstance model,
      final String workflowModuleId,
      final String adapterId,
      final NameClashAvoidanceSupport scoping) {

    if ((scoping == null) || (scoping.modeFor(workflowModuleId, null, adapterId) != NameClashAvoidance.USE_PREFIX)) {
      return;
    }

    // task definitions are scoped per PROCESS, so they are rewritten while the
    // process ids are still the plain ones
    model
        .getModelElementsByType(ZeebeTaskDefinition.class)
        .forEach(taskDefinition -> taskDefinition.setType(
            scoping.scopedTaskDefinition(
                workflowModuleId,
                owningProcessId(taskDefinition),
                taskDefinition.getType(),
                adapterId)));
    model
        .getModelElementsByType(ZeebeFormDefinition.class)
        .forEach(formDefinition -> {
          final var externalReference = formDefinition.getExternalReference();
          if ((externalReference == null) || externalReference.isBlank()) {
            return;
          }
          formDefinition.setExternalReference(
              scoping.scopedTaskDefinition(
                  workflowModuleId,
                  owningProcessId(formDefinition),
                  externalReference,
                  adapterId));
        });

    model
        .getModelElementsByType(Message.class)
        .forEach(message -> message.setName(
            scoping.scopedIdentifier(workflowModuleId, message.getName(), adapterId)));
    model
        .getModelElementsByType(Signal.class)
        .forEach(signal -> signal.setName(
            scoping.scopedIdentifier(workflowModuleId, signal.getName(), adapterId)));
    model
        .getModelElementsByType(Escalation.class)
        .forEach(escalation -> escalation.setEscalationCode(
            scoping.scopedIdentifier(workflowModuleId, escalation.getEscalationCode(), adapterId)));
    model
        .getModelElementsByType(Error.class)
        .forEach(error -> error.setErrorCode(
            scoping.scopedIdentifier(workflowModuleId, error.getErrorCode(), adapterId)));

    // call activities address another process BY ID - rewrite before the ids change
    model
        .getModelElementsByType(ZeebeCalledElement.class)
        .forEach(calledElement -> calledElement.setProcessId(
            scoping.scopedProcessId(workflowModuleId, calledElement.getProcessId(), adapterId)));

    // ... and the process ids last
    model
        .getModelElementsByType(Process.class)
        .forEach(process -> {
          final var scoped = scoping.scopedProcessId(workflowModuleId, process.getId(), adapterId);
          if (scoped.equals(process.getId())) {
            return;
          }
          log.debug(
              "Camunda8: BPMN process '{}' of workflow module '{}' is deployed as '{}' (name-clash "
                  + "avoidance 'use-prefix')",
              process.getId(),
              workflowModuleId,
              scoped);
          process.setId(scoped);
        });

  }

  /**
   * The id of the {@code bpmn:process} the given element belongs to - task
   * definitions are scoped per process, and at rewriting time the process ids are
   * still plain.
   */
  private static String owningProcessId(
      final io.camunda.zeebe.model.bpmn.instance.BpmnModelElementInstance element) {

    var current = element.getParentElement();
    while (current != null) {
      if (current instanceof Process process) {
        return process.getId();
      }
      current = current.getParentElement();
    }
    // an element outside any process (should not happen for task definitions) -
    // scoping by the module alone is the safe fallback
    if (element instanceof FlowElement flowElement) {
      log.warn(
          "Camunda8: could not determine the BPMN process of element '{}' - its task definition is "
              + "scoped by the workflow module only",
          flowElement.getId());
    }
    return null;

  }

}
