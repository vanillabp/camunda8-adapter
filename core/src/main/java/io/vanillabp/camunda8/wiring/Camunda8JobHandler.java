package io.vanillabp.camunda8.wiring;

import java.time.Duration;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.worker.JobClient;
import io.camunda.client.api.worker.JobHandler;
import io.vanillabp.integration.adapter.spi.workflowtask.TaskInvocationContext;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskInvoker;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskOutcome;
import lombok.extern.slf4j.Slf4j;

/**
 * The job handler of one polling worker (one worker per adapter ID and task
 * definition): builds the neutral invocation context from the {@link ActivatedJob}
 * and dispatches through the core's {@link WorkflowTaskInvoker}. Camunda 8 job
 * workers deliver AT LEAST ONCE - the handler (and its completion) are idempotent
 * by design:
 * <ul>
 * <li>the business method runs in a NEW local transaction which commits BEFORE the
 * job is completed (order: open TX - load aggregate - invoke - save - commit -
 * complete job). A crash between commit and complete causes a redelivery which
 * converges: the handler runs again on the already-updated aggregate and the
 * completion succeeds;</li>
 * <li>completing a job that was already completed by a parallel redelivery is
 * tolerated (WARN - the documented at-least-once residual);</li>
 * <li>{@code TaskException} maps to a BPMN error (error-boundary routing, aggregate
 * changes committed);</li>
 * <li>any other exception fails the job with decremented retries - the local
 * transaction was already rolled back by the core;</li>
 * <li>the completion CARRIES THE AGGREGATE STATE (story 28b): the values the
 * aggregate shares with the BPMS plus - always - the technical aggregate-ID
 * variable. Without them a gateway right after the service task would evaluate the
 * values of the last {@code ProcessService}-driven sync point, i.e. STALE data. The
 * values are read through the core's
 * {@link WorkflowTaskInvoker#syncedWorkflowAggregateValues} AFTER the local
 * transaction committed (in its own transaction) - a failing read never prevents
 * the completion, the job is then completed with the ID variable only. The same
 * holds for the BPMN_ERROR path: the error boundary's outgoing flow may branch on
 * the aggregate, too;</li>
 * <li>a {@code @TaskId} method returning without completing puts the job into
 * DORMANCY: the job's lock is extended once to the adapter's
 * <code>async-task-timeout</code> (days), so the handler is NOT re-invoked while
 * the workflow waits for the asynchronous completion
 * (<code>ProcessService#completeTask</code>). The worker's regular
 * job timeout stays short - crash recovery of non-async tasks is not delayed.</li>
 * </ul>
 */
@Slf4j
public class Camunda8JobHandler implements JobHandler {

  private final String adapterId;

  private final String workflowModuleId;

  /**
   * The full client - job-lock extension (dormancy) is not part of the
   * {@link JobClient} handed to handlers.
   */
  private final CamundaClient camundaClient;

  private final WorkflowTaskInvoker workflowTaskInvoker;

  private final Duration asyncTaskTimeout;

  /**
   * Translates the identifiers the cluster reports back into the plain ones the core
   * knows (story 35) - a no-op unless the workflow module uses prefixes. May be
   * <code>null</code> (tests).
   */
  private final io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport scoping;

  public Camunda8JobHandler(
      final String adapterId,
      final String workflowModuleId,
      final CamundaClient camundaClient,
      final WorkflowTaskInvoker workflowTaskInvoker,
      final Duration asyncTaskTimeout) {

    this(adapterId, workflowModuleId, camundaClient, workflowTaskInvoker, asyncTaskTimeout, null);

  }

  public Camunda8JobHandler(
      final String adapterId,
      final String workflowModuleId,
      final CamundaClient camundaClient,
      final WorkflowTaskInvoker workflowTaskInvoker,
      final Duration asyncTaskTimeout,
      final io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport scoping) {

    this.adapterId = adapterId;
    this.workflowModuleId = workflowModuleId;
    this.camundaClient = camundaClient;
    this.workflowTaskInvoker = workflowTaskInvoker;
    this.asyncTaskTimeout = asyncTaskTimeout;
    this.scoping = scoping;

  }

  @Override
  public void handle(
      final JobClient client,
      final ActivatedJob job) {

    // the cluster reports the identifiers IT knows - translate them back into the
    // plain ones the core's registries are keyed by (story 35)
    final var bpmnProcessId = scoping == null
        ? job.getBpmnProcessId()
        : scoping.plainProcessId(workflowModuleId, job.getBpmnProcessId(), adapterId);
    final var taskDefinition = scoping == null
        ? job.getType()
        : scoping.plainTaskDefinition(workflowModuleId, bpmnProcessId, job.getType(), adapterId);

    final WorkflowTaskOutcome outcome;
    final String aggregateIdName;
    final Object aggregateId;
    try {
      aggregateIdName = workflowTaskInvoker.resolveWorkflowAggregateIdName(
          workflowModuleId,
          bpmnProcessId);
      aggregateId = job.getVariablesAsMap().get(aggregateIdName);
      if (aggregateId == null) {
        throw new IllegalStateException(
            """
                Job '%s' (type '%s') of BPMN process '%s' carries no variable '%s' holding the \
                workflow aggregate's ID! Workflows processed by VanillaBP have to be started \
                through VanillaBP (the variable is written on start)."""
                .formatted(job.getKey(), taskDefinition, bpmnProcessId, aggregateIdName));
      }
      outcome = workflowTaskInvoker.invokeWorkflowTask(
          workflowModuleId,
          bpmnProcessId,
          new Camunda8TaskInvocationContext(taskDefinition, String.valueOf(aggregateId), job));
    } catch (final Exception e) {
      // the core rolled the local transaction back - fail the job so Camunda 8
      // applies its retry semantics (retries reach 0 -> incident)
      log.warn(
          "Camunda8[{}]: processing job '{}' (type '{}') of BPMN process '{}' failed - failing the job "
              + "with {} retries left",
          adapterId,
          job.getKey(),
          taskDefinition,
          bpmnProcessId,
          job.getRetries() - 1,
          e);
      client
          .newFailCommand(job.getKey())
          .retries(job.getRetries() - 1)
          .errorMessage(String.valueOf(e.getMessage()))
          .send()
          .join();
      return;
    }

    switch (outcome.kind()) {
      case COMPLETED -> completeTolerantly(
          client,
          job,
          variablesOf(bpmnProcessId, aggregateIdName, aggregateId));
      case BPMN_ERROR -> client
          .newThrowErrorCommand(job.getKey())
          // the model's error codes are prefixed too (story 35), so the code the
          // business method raised has to be translated on its way to the cluster
          .errorCode(scoping == null
              ? outcome.errorCode()
              : scoping.scopedIdentifier(workflowModuleId, outcome.errorCode(), adapterId))
          .errorMessage(String.valueOf(outcome.errorName()))
          // the error boundary's outgoing path may branch on the aggregate, too
          .variables(variablesOf(bpmnProcessId, aggregateIdName, aggregateId))
          .send()
          .join();
      case COMPLETION_PENDING -> {
        // dormancy: extend THIS job's lock once to the async-task timeout - the
        // job is not re-delivered (and the handler not re-invoked) while the
        // workflow waits for ProcessService#completeTask
        camundaClient
            .newUpdateTimeoutCommand(job.getKey())
            .timeout(asyncTaskTimeout)
            .send()
            .join();
        log.debug(
            "Camunda8[{}]: job '{}' (type '{}') stays open for asynchronous completion - lock "
                + "extended by {}",
            adapterId,
            job.getKey(),
            taskDefinition,
            asyncTaskTimeout);
      }
    }

  }

  /**
   * The variables the completion of a job carries: the values the workflow
   * aggregate shares with the cluster (story 28b - the {@code @WorkflowTask} method
   * just changed it and a gateway right after this task has to see the NEW values)
   * plus - always, no matter what the sync model says - the technical variable
   * holding the aggregate's ID.
   *
   * @param bpmnProcessId The BPMN process ID
   * @param aggregateIdName The name of the aggregate's ID property
   * @param aggregateId The aggregate's ID as it arrived in the job's variables
   * @return The variables (never <code>null</code>)
   */
  private java.util.Map<String, Object> variablesOf(
      final String bpmnProcessId,
      final String aggregateIdName,
      final Object aggregateId) {

    final var variables = new java.util.LinkedHashMap<String, Object>(
        // the core loads the aggregate in its OWN transaction (the task's one is
        // committed) and never throws - a failed read yields an empty map
        workflowTaskInvoker.syncedWorkflowAggregateValues(
            workflowModuleId,
            bpmnProcessId,
            String.valueOf(aggregateId),
            io.vanillabp.camunda8.processservice.Camunda8ProcessService.SYNC_MODE));
    variables.put(aggregateIdName, String.valueOf(aggregateId));
    return variables;

  }

  /**
   * Completes the job, tolerating that a parallel redelivery (at-least-once) has
   * already completed it - the documented residual of completing AFTER the local
   * transaction committed.
   */
  private void completeTolerantly(
      final JobClient client,
      final ActivatedJob job,
      final java.util.Map<String, Object> variables) {

    try {
      client
          .newCompleteCommand(job.getKey())
          .variables(variables)
          .send()
          .join();
    } catch (final Exception e) {
      if (io.vanillabp.camunda8.client.Camunda8Errors.jobAlreadyGone(e)) {
        log.warn(
            "Camunda8[{}]: job '{}' (type '{}') was already completed - a redelivery of the same "
                + "task converged (at-least-once semantics); the business method ran more than once",
            adapterId,
            job.getKey(),
            job.getType());
        return;
      }
      throw e;
    }

  }


  /**
   * The neutral invocation context built from an activated Camunda 8 job.
   */
  static class Camunda8TaskInvocationContext implements TaskInvocationContext {

    private final String taskDefinition;

    private final String workflowAggregateId;

    private final ActivatedJob job;

    Camunda8TaskInvocationContext(
        final String taskDefinition,
        final String workflowAggregateId,
        final ActivatedJob job) {

      this.taskDefinition = taskDefinition;
      this.workflowAggregateId = workflowAggregateId;
      this.job = job;

    }

    @Override
    public String getTaskDefinition() {

      return taskDefinition;

    }

    @Override
    public String getWorkflowAggregateId() {

      return workflowAggregateId;

    }

    @Override
    public String getTaskId() {

      // the job key identifies the open job - used by ProcessService#completeTask
      return String.valueOf(job.getKey());

    }

    @Override
    public Object getTaskParameter(
        final String name) {

      return job.getVariablesAsMap().get(name);

    }

    // runInCurrentTransaction stays false: job workers run on client threads
    // without a transaction - the core opens a NEW one which commits BEFORE the
    // job is completed (at-least-once ordering)

  }

}
