package io.vanillabp.camunda8.processservice;

import io.camunda.client.api.response.ProcessInstanceEvent;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.client.Camunda8Errors;
import io.vanillabp.integration.adapter.spi.MigratableProcessService;
import io.vanillabp.integration.adapter.spi.WorkflowAwareness;
import io.vanillabp.integration.spi.AggregatePersistenceAware;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Camunda 8 implementation of the {@link MigratableProcessService}. One instance is
 * created per configured adapter ID (not per adapter type).
 * <p>
 * Camunda 8 is a <b>remote</b>, eventually consistent BPMS: the engine cannot join the
 * application's local database transaction, therefore
 * {@link #needsTwoPhaseCommitForStartingWorkflows()} returns {@code true} - starting a
 * workflow is routed through the core {@code PhaseTwoOutbox}:
 * <ul>
 *   <li>{@link #startWorkflowPhaseOne} runs inside the caller's transaction. It must
 *       never perform an action that <i>advances</i> the BPMN process (e.g. creating the
 *       instance) - that would race the still-uncommitted local transaction and, on
 *       rollback, leave a ghost workflow instance behind. It <i>may</i> contact the
 *       cluster for a non-advancing check whose only purpose is to abort the local
 *       transaction early when the phase-two action is already known to be impossible.
 *       <b>For starting a workflow there is nothing to check against the cluster</b> - if
 *       the cluster is unavailable, the phase-two start simply waits in the outbox until
 *       it is reachable again - so this method only resolves the aggregate ID and
 *       verifies the adapter is configured. (Other operations do use the phase-one check:
 *       e.g. completing a service task verifies the task still exists by extending its job
 *       worker timeout - a non-advancing operation - ideally in a pre-commit transaction
 *       synchronization to minimize the window between the check and the phase-two action
 *       and thus the number of stale outbox entries. See the {@code
 *       vanillabp-bpms-characteristics} skill / later stories.)</li>
 *   <li>{@link #startWorkflowPhaseTwo} runs after the commit and creates the process
 *       instance via {@link #createProcessInstance(String, Object)}.</li>
 * </ul>
 *
 * @param <A> The workflow-aggregate type
 */
@Slf4j
@RequiredArgsConstructor
public class Camunda8ProcessService<A> implements MigratableProcessService<A> {

  private final String adapterId;

  private final Camunda8ClientFactory clientFactory;

  /**
   * The one-time job-lock extension applied by awareness probes and phase-one
   * checks (the same duration the job worker grants a dormant async task - see
   * story 21c's dormancy design).
   */
  private final java.time.Duration asyncTaskTimeout;

  /**
   * Runs phase-one existence checks right before the commit (platform-supplied) -
   * minimizes the window between check and phase two.
   */
  private final Camunda8PreCommitRegistrar preCommitRegistrar;

  @Override
  public String getAdapterId() {

    return adapterId;

  }

  @Override
  public boolean needsTwoPhaseCommitForStartingWorkflows() {

    return true;

  }

  @Override
  public WorkflowAwareness awarenessOfTask(
      final Object workflowAggregateId,
      final String taskId) {

    // the probe is UpdateJobTimeout - a NON-ADVANCING command which doubles as the
    // dormancy lock refresh (the job's lock is set to the async-task timeout, the
    // same value the worker granted when the task went dormant). Camunda 8 cannot
    // answer COMPLETED for jobs (a completed job is indistinguishable from a
    // never-existing one without the eventually-consistent search API), so a
    // successful "not found" maps to UNKNOWN_TO_BPMS.
    try {
      updateJobTimeout(taskId);
      return WorkflowAwareness.ACTIVE;
    } catch (final Exception e) {
      if (Camunda8Errors.jobAlreadyGone(e)) {
        return WorkflowAwareness.UNKNOWN_TO_BPMS;
      }
      log.warn(
          "Camunda8[{}]: could not determine awareness of task '{}' - reporting BPMS_UNAVAILABLE",
          adapterId,
          taskId,
          e);
      return WorkflowAwareness.BPMS_UNAVAILABLE;
    }

  }

  private void updateJobTimeout(
      final String taskId) {

    clientFactory
        .getClient()
        .newUpdateTimeoutCommand(Long.parseLong(taskId))
        .timeout(asyncTaskTimeout)
        .send()
        .join();

  }

  @Override
  public void completeTaskPhaseOne(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final A workflowAggregate,
      final String taskId) {

    registerPreCommitExistenceCheck(taskId, "completing");

  }

  @Override
  public void cancelTaskPhaseOne(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final A workflowAggregate,
      final String taskId,
      final String bpmnErrorCode) {

    registerPreCommitExistenceCheck(taskId, "canceling");

  }

  /**
   * The phase-one contract for remote BPMS: a NON-ADVANCING existence check whose
   * only purpose is to abort the local transaction early when the task is already
   * gone. Registered as a PRE-COMMIT synchronization (not run at method-call time)
   * so the window between check and phase-two dispatch - and therefore the number
   * of stale outbox entries - stays minimal (the V1 refinement). The check is the
   * same UpdateJobTimeout used by the awareness probe: it refreshes the dormant
   * job's lock as a side effect and never advances the process.
   */
  private void registerPreCommitExistenceCheck(
      final String taskId,
      final String operationDescription) {

    preCommitRegistrar.beforeCommit(() -> {
      try {
        updateJobTimeout(taskId);
      } catch (final Exception e) {
        if (Camunda8Errors.jobAlreadyGone(e)) {
          throw new IllegalStateException(
              ("The task '%s' is gone (completed or canceled meanwhile) - aborting the transaction "
                  + "%s it! If this task was completed by a concurrent redelivery, retrying the "
                  + "business operation will end in the documented no-op.")
                  .formatted(taskId, operationDescription), e);
        }
        throw e;
      }
    });

  }

  @Override
  public WorkflowAwareness awarenessOfUserTask(
      final Object workflowAggregateId,
      final String taskId) {

    // the probe is an EMPTY UpdateUserTask - an engine command (unlike the
    // query API it needs no secondary storage) which never advances the task;
    // it answers NOT_FOUND for gone tasks. Side effect: modeller-defined
    // 'updating' task listeners fire - documented in the README.
    try {
      updateUserTask(taskId);
      return WorkflowAwareness.ACTIVE;
    } catch (final Exception e) {
      if (Camunda8Errors.jobAlreadyGone(e)) {
        return WorkflowAwareness.UNKNOWN_TO_BPMS;
      }
      log.warn(
          "Camunda8[{}]: could not determine awareness of user task '{}' - reporting BPMS_UNAVAILABLE",
          adapterId,
          taskId,
          e);
      return WorkflowAwareness.BPMS_UNAVAILABLE;
    }

  }

  private void updateUserTask(
      final String taskId) {

    // an update carrying ONLY an 'action' (an audit metadatum) is the minimal
    // valid update - no task attribute changes, nothing advances
    clientFactory
        .getClient()
        .newUpdateUserTaskCommand(Long.parseLong(taskId))
        .action("io.vanillabp:probe")
        .send()
        .join();

  }

  @Override
  public void completeUserTaskPhaseOne(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final A workflowAggregate,
      final String taskId) {

    // pre-commit existence check (non-advancing empty update) - same shape as
    // service tasks, see registerPreCommitExistenceCheck
    preCommitRegistrar.beforeCommit(() -> {
      try {
        updateUserTask(taskId);
      } catch (final Exception e) {
        if (Camunda8Errors.jobAlreadyGone(e)) {
          throw new IllegalStateException(
              ("The user task '%s' is gone (completed or canceled meanwhile) - aborting the "
                  + "transaction completing it!")
                  .formatted(taskId), e);
        }
        throw e;
      }
    });

  }

  @Override
  public void completeUserTaskPhaseTwo(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId,
      final String taskId) {

    try {
      clientFactory
          .getClient()
          .newCompleteUserTaskCommand(Long.parseLong(taskId))
          .send()
          .join();
      log.info(
          "Camunda8[{}]: completed user task '{}' of BPMN process '{}' of workflow module '{}'",
          adapterId,
          taskId,
          bpmnProcessId,
          workflowModuleId);
    } catch (final Exception e) {
      if (!Camunda8Errors.jobAlreadyGone(e)) {
        throw e;
      }
      log.warn(
          "Camunda8[{}]: user task '{}' is gone - skipping the redelivered phase-two completion",
          adapterId,
          taskId);
    }

  }

  @Override
  public void cancelUserTaskPhaseOne(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final A workflowAggregate,
      final String taskId,
      final String bpmnErrorCode) {

    // fail EARLY inside the caller's transaction: see cancelUserTaskPhaseTwo
    throw newCancelUserTaskUnsupported(taskId, bpmnProcessId);

  }

  @Override
  public void cancelUserTaskPhaseTwo(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId,
      final String taskId,
      final String bpmnErrorCode) {

    throw newCancelUserTaskUnsupported(taskId, bpmnProcessId);

  }

  private UnsupportedOperationException newCancelUserTaskUnsupported(
      final String taskId,
      final String bpmnProcessId) {

    // Camunda 8.8 offers NO command to cancel a Camunda-managed user task by BPMN
    // error: ThrowError is job-based (a zeebe:userTask has no job), and the V1
    // workaround (completing the task with a marker variable evaluated by a
    // listener) is marked "currently not working" in the V1 adapter itself.
    // Task/execution listeners of Camunda 8.10 are expected to enable this - see
    // the prepared follow-up prompt.
    return new UnsupportedOperationException(
        ("Canceling user task '%s' of BPMN process '%s' by BPMN error is not supported on "
            + "Camunda 8.8! The engine offers no command for it (ThrowError is job-based; a "
            + "Camunda-managed user task has no job). Model the error path explicitly (e.g. a "
            + "boundary message/signal) or wait for the Camunda 8.10 listener support.")
            .formatted(taskId, bpmnProcessId));

  }

  @Override
  public void completeTaskPhaseTwo(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId,
      final String taskId) {

    try {
      clientFactory
          .getClient()
          .newCompleteCommand(Long.parseLong(taskId))
          .send()
          .join();
      log.info(
          "Camunda8[{}]: completed task '{}' of BPMN process '{}' of workflow module '{}'",
          adapterId,
          taskId,
          bpmnProcessId,
          workflowModuleId);
    } catch (final Exception e) {
      if (!Camunda8Errors.jobAlreadyGone(e)) {
        throw e;
      }
      // stale outbox entry: the job disappeared between the dispatch-time probe
      // and this command - the at-least-once residual, the entry is consumed
      log.warn(
          "Camunda8[{}]: task '{}' is gone - skipping the redelivered phase-two completion",
          adapterId,
          taskId);
    }

  }

  @Override
  public void cancelTaskPhaseTwo(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId,
      final String taskId,
      final String bpmnErrorCode) {

    try {
      clientFactory
          .getClient()
          .newThrowErrorCommand(Long.parseLong(taskId))
          .errorCode(bpmnErrorCode)
          .errorMessage("canceled via ProcessService#cancelTask")
          .send()
          .join();
      log.info(
          "Camunda8[{}]: canceled task '{}' (error code '{}') of BPMN process '{}' of workflow module '{}'",
          adapterId,
          taskId,
          bpmnErrorCode,
          bpmnProcessId,
          workflowModuleId);
    } catch (final Exception e) {
      if (!Camunda8Errors.jobAlreadyGone(e)) {
        throw e;
      }
      log.warn(
          "Camunda8[{}]: task '{}' is gone - skipping the redelivered phase-two cancellation",
          adapterId,
          taskId);
    }

  }

  @Override
  public WorkflowAwareness awarenessOfWorkflow(
      final Object workflowAggregateId) {

    throw new UnsupportedOperationException("awarenessOfWorkflow is implemented in a later story");

  }

  @Override
  public void startWorkflowPhaseOne(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final A workflowAggregate) {

    // the aggregate id was validated (non-null, non-blank) once in the core's
    // MigrationProcessService before phase one is invoked

    // For starting a workflow there is nothing to check against the cluster in phase
    // one, so this only verifies the adapter is configured. Phase one runs inside the
    // caller's DB transaction: it must never advance the process (that races the local
    // transaction), but it MAY contact the cluster for non-advancing checks that abort
    // the transaction early - not needed here, since an unavailable cluster just makes
    // the phase-two start wait in the outbox until it is reachable again.
    clientFactory.validateConfigured();

    log.debug("Validated phase one of starting Camunda 8 workflow '{}' of workflow module '{}' "
        + "(adapter '{}')", bpmnProcessId, workflowModuleId, adapterId);

  }

  @Override
  public void startWorkflowPhaseTwo(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId) {

    createProcessInstance(bpmnProcessId, aggregatePersistence.getAggregateIdName(), workflowAggregateId);

  }

  /**
   * Creates a Camunda 8 process instance of the latest version of the given BPMN process,
   * passing the workflow aggregate's ID as a single process variable. How the aggregate's
   * ID is stored in the BPMS is the adapter's decision: Camunda 8 stores the aggregate as
   * process variables, so the variable carrying the ID is named after the aggregate's ID
   * property (see {@link AggregatePersistenceAware#getAggregateIdName()}). This is the
   * actual work of
   * {@link #startWorkflowPhaseTwo(String, String, AggregatePersistenceAware, Object)}.
   * <p>
   * <b>Idempotency limitation:</b> a crash between a successful create and the removal of
   * the phase-two outbox entry can create the instance twice (at-least-once, duplicates
   * possible). Strict deduplication needs the core-side {@code WorkflowInstanceRegistry}
   * (separate story) - see the repository-root {@code README.md}. No Camunda-8-side
   * workaround is attempted here.
   *
   * @param bpmnProcessId The BPMN process ID of the workflow to start
   * @param aggregateIdName The name of the aggregate's ID property (used as the variable name)
   * @param workflowAggregateId The workflow aggregate's ID (sent as a string variable)
   * @return The created process-instance event
   */
  public ProcessInstanceEvent createProcessInstance(
      final String bpmnProcessId,
      final String aggregateIdName,
      final Object workflowAggregateId) {

    final var client = clientFactory.getClient();
    var command = client
        .newCreateInstanceCommand()
        .bpmnProcessId(bpmnProcessId)
        .latestVersion()
        .variable(
            aggregateIdName,
            workflowAggregateId == null ? null : workflowAggregateId.toString());

    final var tenantId = clientFactory.getConfiguration().getTenantId();
    if (tenantId != null && !tenantId.isBlank()) {
      command = command.tenantId(tenantId);
    }

    final var event = command
        .send()
        .join();
    log.info("Started Camunda 8 workflow '{}' (adapter '{}', process-instance key {}) for aggregate '{}'",
        bpmnProcessId, adapterId, event.getProcessInstanceKey(), workflowAggregateId);
    return event;

  }

}
