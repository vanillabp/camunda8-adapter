package io.vanillabp.camunda8.deployment;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.camunda.client.api.command.DeployResourceCommandStep1;
import io.camunda.client.api.command.DeployResourceCommandStep1.DeployResourceCommandStep2;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.Process;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.integration.adapter.spi.AdapterDeploymentService;
import io.vanillabp.integration.adapter.spi.BpmnParseException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Camunda 8 implementation of the {@link AdapterDeploymentService}. One instance is
 * created per configured adapter ID (not per adapter type) because the same BPMS type
 * may be configured multiple times (BPMS migration).
 * <p>
 * The BPMN model type is {@link BpmnModelInstance}, shipped with the Camunda 8 client via
 * {@code io.camunda:zeebe-bpmn-model}. The processing context is
 * {@link Camunda8ProcessingContext}, which collects all deployable resources of a workflow
 * module so they are deployed in a single {@code DeployResourceCommand}.
 * <p>
 * Task wiring ({@code wireBpmn}) and job workers ({@code startWorkflowProcessing}) are
 * later stories; for now those methods only log.
 */
@Slf4j
@RequiredArgsConstructor
public class Camunda8DeploymentService implements AdapterDeploymentService<BpmnModelInstance, Camunda8ProcessingContext> {

  /**
   * The adapter type of the Camunda 8 adapter. Constant across all instances; the
   * adapter ID (see {@link #getAdapterId()}) distinguishes instances.
   */
  public static final String ADAPTER_TYPE = io.vanillabp.camunda8.Camunda8Adapter.ADAPTER_TYPE;

  private final String adapterId;

  private final Camunda8ClientFactory clientFactory;

  @Override
  public String getAdapterId() {

    return adapterId;

  }

  @Override
  public String getAdapterType() {

    return ADAPTER_TYPE;

  }

  @Override
  public Class<BpmnModelInstance> getModelType() {

    return BpmnModelInstance.class;

  }

  @Override
  public Class<Camunda8ProcessingContext> getProcessContextType() {

    return Camunda8ProcessingContext.class;

  }

  @Override
  public List<Map.Entry<String, BpmnModelInstance>> readBpmn(
      final String workflowModuleId,
      final String filename,
      final InputStream bpmn,
      final boolean isVanillaBpBpmn) throws BpmnParseException {

    final BpmnModelInstance model;
    try {
      model = Bpmn.readModelFromStream(bpmn);
    } catch (final RuntimeException e) {
      throw new BpmnParseException(
          "Failed to parse BPMN file '%s' of workflow module '%s'!".formatted(filename, workflowModuleId), e);
    }

    // one entry per executable process; the value is always the whole model since
    // Camunda 8 deploys the entire file as one resource (a file may hold several
    // executable processes)
    final var executableProcesses = new ArrayList<Map.Entry<String, BpmnModelInstance>>();
    for (final var process : model.getModelElementsByType(Process.class)) {
      if (!process.isExecutable()) {
        continue;
      }
      executableProcesses.add(Map.entry(process.getId(), model));
    }
    return executableProcesses;

  }

  @Override
  public Camunda8ProcessingContext prepareBpmn(
      final String workflowModuleId,
      final Camunda8ProcessingContext existingContext,
      final String filename,
      final String bpmnProcessId,
      final BpmnModelInstance model) {

    // the core passes null for the first BPMN process of a workflow module
    final var context = existingContext != null
        ? existingContext
        : new Camunda8ProcessingContext(workflowModuleId);
    context.addResource(filename, model);
    return context;

  }

  @Override
  public void wireBpmn(
      final String workflowModuleId,
      final String filename,
      final String bpmnProcessId,
      final BpmnModelInstance model,
      final Camunda8ProcessingContext context) {

    // task wiring (job workers) is a later story
    log.debug("Wiring BPMN process '{}' of file '{}' (workflow module '{}', adapter '{}') - "
        + "task wiring is implemented in a later story",
        bpmnProcessId, filename, workflowModuleId, adapterId);

  }

  @Override
  public void deployResources(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext) throws IllegalStateException {

    if (bpmsProcessingContext == null || bpmsProcessingContext.isEmpty()) {
      log.info("No executable BPMN resources for workflow module '{}' and adapter '{}' - "
          + "nothing to deploy to Camunda 8", workflowModuleId, adapterId);
      return;
    }

    // one DeployResourceCommand per workflow module with all its models
    final var client = clientFactory.getClient();
    DeployResourceCommandStep2 command = null;
    for (final var resource : bpmsProcessingContext.getResources().entrySet()) {
      final DeployResourceCommandStep1 next = command != null ? command : client.newDeployResourceCommand();
      command = next.addProcessModel(resource.getValue(), resource.getKey());
    }

    // Camunda 8 has no C7-style tenant per workflow module: use the configured
    // multi-tenancy tenant if any, otherwise the default tenant. Module isolation
    // therefore relies on unique BPMN process IDs for now (see README).
    final var tenantId = clientFactory.getConfiguration().getTenantId();
    if (tenantId != null && !tenantId.isBlank()) {
      command = command.tenantId(tenantId);
    }

    try {
      final var deployment = command
          .send()
          .join();
      log.info("Deployed {} BPMN resource(s) of workflow module '{}' to Camunda 8 "
          + "(adapter '{}', deployment key {}, tenant '{}'): {}",
          bpmsProcessingContext.getResources().size(),
          workflowModuleId,
          adapterId,
          deployment.getKey(),
          tenantId != null && !tenantId.isBlank() ? tenantId : "<default>",
          bpmsProcessingContext.getResources().keySet());
    } catch (final RuntimeException e) {
      throw new IllegalStateException(
          "Failed to deploy BPMN resources of workflow module '%s' to Camunda 8 (adapter '%s')!"
              .formatted(workflowModuleId, adapterId), e);
    }

  }

  @Override
  public void startWorkflowProcessing(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext) {

    // job workers pulling service tasks are a later story; nothing to start yet
    log.info("Workflow processing started for workflow module '{}' (adapter '{}') - "
        + "job workers are implemented in a later story", workflowModuleId, adapterId);

  }

  @Override
  public void stopWorkflowProcessing(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext) {

    // the CamundaClient itself is closed by the Camunda8ClientFactory on application
    // shutdown (it is shared across all workflow modules of the adapter instance)
    log.info("Workflow processing stopped for workflow module '{}' (adapter '{}')",
        workflowModuleId, adapterId);

  }

}
