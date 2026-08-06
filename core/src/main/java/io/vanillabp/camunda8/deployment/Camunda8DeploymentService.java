package io.vanillabp.camunda8.deployment;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.camunda.client.api.command.DeployResourceCommandStep1;
import io.camunda.client.api.command.DeployResourceCommandStep1.DeployResourceCommandStep2;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.Process;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.wiring.Camunda8JobHandler;
import io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver;
import io.vanillabp.camunda8.wiring.Camunda8TaskWiring;
import io.vanillabp.integration.adapter.spi.AdapterDeploymentService;
import io.vanillabp.integration.adapter.spi.BpmnParseException;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskInvoker;
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
 * Task wiring ({@code wireBpmn}) validates the BPMN's job-worker tasks
 * (zeebe:taskDefinition) against the registered {@code @WorkflowTask} methods;
 * {@code startWorkflowProcessing} opens one polling job worker per task definition
 * (closed on {@code stopWorkflowProcessing}).
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

  /**
   * The core's task-processing entry point: wiring validation during
   * {@link #wireBpmn} and job dispatch at runtime.
   */
  private final WorkflowTaskInvoker workflowTaskInvoker;

  /**
   * Resolves the per-task job timeout from the adapter's configuration overlay
   * (task &gt; workflow &gt; workflow-module &gt; adapter, most specific wins).
   */
  private final Camunda8JobTimeoutResolver jobTimeoutResolver;

  /**
   * How long a {@code @TaskId} job stays dormant awaiting its asynchronous
   * completion (see {@link Camunda8JobHandler}).
   */
  private final Duration asyncTaskTimeout;

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

    // extract the job-worker tasks (zeebe:taskDefinition type = VanillaBP task
    // definition) and validate them against the registered @WorkflowTask methods;
    // throwing here honors the deployment-failure policy
    final var tasks = Camunda8TaskWiring.tasksOf(model, bpmnProcessId);
    // Camunda-managed user tasks (story 24): the V1-compatible lifecycle task
    // listeners are ADDED TO THE MODEL here (wireBpmn is the BPMN-modification
    // stage of the pipeline) - the modified model is what deployResources deploys
    final var userTasks = Camunda8TaskWiring.userTasksOf(model, bpmnProcessId, workflowModuleId, filename);
    final var specs = new java.util.ArrayList<io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec>();
    tasks
        .stream()
        .map(Camunda8TaskWiring.Camunda8TaskToWire::toSpec)
        .forEach(specs::add);
    userTasks
        .stream()
        .map(Camunda8TaskWiring.Camunda8UserTaskToWire::toSpec)
        .forEach(specs::add);
    workflowTaskInvoker.validateTaskWiring(workflowModuleId, bpmnProcessId, specs);
    // message correlation (story 23): inject the correlation-key expression
    // '=<aggregate-ID variable>' into message subscriptions lacking one - the V2
    // convention enabling ProcessService#correlateMessage without manual model
    // tweaks (existing expressions stay untouched, V1 models deploy unchanged)
    Camunda8TaskWiring.wireMessageSubscriptions(
        model,
        bpmnProcessId,
        () -> workflowTaskInvoker.resolveWorkflowAggregateIdName(workflowModuleId, bpmnProcessId));
    context.getTasksToWire().addAll(tasks);
    context.getUserTasksToWire().addAll(userTasks);

    log.info(
        "Camunda8[{}]: wired {} task(s) of BPMN process '{}' (file '{}', workflow module '{}')",
        adapterId,
        tasks.size(),
        bpmnProcessId,
        filename,
        workflowModuleId);

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
      // remember what was deployed: the viewer API serves definitions and BPMN XML
      // from these models instead of the eventually consistent query API (see
      // Camunda8DeployedProcesses)
      deployment
          .getProcesses()
          .forEach(process -> {
            final var model = bpmsProcessingContext
                .getResources()
                .get(process.getResourceName());
            if (model == null) {
              return;
            }
            clientFactory
                .getDeployedProcesses()
                .record(
                    new Camunda8DeployedProcesses.DeployedProcess(
                        workflowModuleId, process.getBpmnProcessId(), String
                            .valueOf(process.getProcessDefinitionKey()), process.getVersion(), model));
          });

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

    // after ALL processes of the module were wired: methods matching no task of
    // any wired process are a defect (per-module check, honors the policy)
    workflowTaskInvoker.validateNoUnwiredWorkflowTaskMethods(workflowModuleId);

  }

  @Override
  public void startWorkflowProcessing(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext) {

    // one polling worker per (adapter id, task definition): the job type routes
    // deliveries; tasks of DIFFERENT processes sharing a task definition are
    // served by one worker (job.getBpmnProcessId() routes to the right handlers).
    // The job timeout is resolved most-specific-wins - a task definition used by
    // several tasks with CONFLICTING configured timeouts fails guiding.
    final var timeoutsByDefinition = new LinkedHashMap<String, Duration>();
    final var client = clientFactory.getClient();
    bpmsProcessingContext
        .getTasksToWire()
        .forEach(task -> {
          if (task.taskDefinition() == null) {
            return; // already reported by the wiring validation
          }
          final var timeout = jobTimeoutResolver.jobTimeoutFor(
              workflowModuleId,
              task.bpmnProcessId(),
              task.taskDefinition());
          final var previous = timeoutsByDefinition.putIfAbsent(task.taskDefinition(), timeout);
          if ((previous != null) && !previous.equals(timeout)) {
            throw new IllegalStateException(
                """
                    The task definition '%s' of workflow module '%s' is used by several tasks with \
                    CONFLICTING job timeouts (%s vs. %s)! One polling worker serves a task \
                    definition - configure the same 'job-timeout' for all its tasks (property \
                    levels: vanillabp.workflow-modules.%s.workflows.<workflow>.tasks.%s.adapters.%s.job-timeout)."""
                    .formatted(
                        task.taskDefinition(),
                        workflowModuleId,
                        previous,
                        timeout,
                        workflowModuleId,
                        task.taskDefinition(),
                        adapterId));
          }
        });
    // user-task lifecycle listeners (story 24): one worker per distinct listener
    // job type; listener jobs are consumed like normal jobs
    final var userTasksByListenerJobType = new LinkedHashMap<String, java.util.List<String>>();
    bpmsProcessingContext
        .getUserTasksToWire()
        .forEach(userTask -> userTasksByListenerJobType
            .computeIfAbsent(userTask.listenerJobType(), key -> new java.util.LinkedList<>())
            .add(userTask.bpmnProcessId()));
    userTasksByListenerJobType.forEach((
        listenerJobType,
        bpmnProcessIds) -> {
      final var worker = client
          .newWorker()
          .jobType(listenerJobType)
          .handler(new io.vanillabp.camunda8.wiring.Camunda8UserTaskListenerHandler(
              adapterId, workflowModuleId, workflowTaskInvoker))
          .timeout(java.time.Duration.ofMinutes(1))
          .name("vanillabp-%s-%s".formatted(adapterId, listenerJobType))
          .open();
      bpmsProcessingContext.getOpenWorkers().add(worker);
      log.info(
          "Camunda8[{}]: opened user-task listener worker for '{}' of workflow module '{}'",
          adapterId,
          listenerJobType,
          workflowModuleId);
    });

    timeoutsByDefinition.forEach((
        taskDefinition,
        timeout) -> {
      final var worker = client
          .newWorker()
          .jobType(taskDefinition)
          .handler(new Camunda8JobHandler(adapterId, workflowModuleId, client, workflowTaskInvoker, asyncTaskTimeout))
          .timeout(timeout)
          .name("vanillabp-%s-%s".formatted(adapterId, taskDefinition))
          .open();
      bpmsProcessingContext.getOpenWorkers().add(worker);
      log.info(
          "Camunda8[{}]: opened job worker for task definition '{}' of workflow module '{}' "
              + "(job timeout {})",
          adapterId,
          taskDefinition,
          workflowModuleId,
          timeout);
    });

  }

  @Override
  public void stopWorkflowProcessing(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext) {

    // close this module's workers (reverse order); the CamundaClient itself is
    // closed by the Camunda8ClientFactory on application shutdown
    final var workers = bpmsProcessingContext.getOpenWorkers();
    for (var i = workers.size() - 1; i >= 0; --i) {
      workers.get(i).close();
    }
    workers.clear();
    log.info("Workflow processing stopped for workflow module '{}' (adapter '{}')",
        workflowModuleId, adapterId);

  }

}
