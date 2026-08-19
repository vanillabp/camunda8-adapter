package io.vanillabp.camunda8.processservice;

import io.camunda.client.api.response.ProcessInstanceEvent;
import io.vanillabp.camunda8.Camunda8ReleaseLine;
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
  private final java.time.Duration asyncTaskLockRenewal;

  /**
   * Runs phase-one existence checks right before the commit of the workflow aggregate's
   * transaction (platform-supplied, story 87) - minimizes the window between check and
   * phase two. The platform resolves the runner of the aggregate, so a unit of work the
   * APPLICATION brought is the one hooked into.
   */
  private final io.vanillabp.integration.adapter.spi.PreCommitRegistrar preCommitRegistrar;

  /**
   * The core's sync model (story 28): which aggregate attributes are shared with
   * the cluster. Camunda 8 is REMOTE, so its default is
   * {@link AggregateSyncMode#FULL} - a BPMN expression can only see what VanillaBP
   * pushed as a process variable. May be <code>null</code> (tests): only the
   * technical aggregate-ID variable is written then.
   */
  private final io.vanillabp.integration.adapter.spi.WorkflowAggregateSync aggregateSync;

  /**
   * How long a workflow of this cluster may stay invisible to the query API the
   * awareness probe searches (configured per adapter id, default
   * {@link #DEFAULT_WORKFLOW_VISIBILITY_TIMEOUT}). May be <code>null</code>
   * (tests): the default applies then.
   */
  private final java.time.Duration workflowVisibilityTimeout;

  /**
   * How long VanillaBP waits for a workflow this cluster holds to become findable.
   * Ten seconds is generous for a healthy exporter and still short enough to stay
   * inside the caller's transaction, which the waiting keeps open.
   */
  public static final java.time.Duration DEFAULT_WORKFLOW_VISIBILITY_TIMEOUT = java.time.Duration
      .ofSeconds(10);

  /**
   * How often the probe is repeated while waiting - deliberately not configurable:
   * the window is what an operator may have to raise, the sampling rate is not.
   */
  private static final java.time.Duration WORKFLOW_VISIBILITY_PROBE_INTERVAL = java.time.Duration
      .ofMillis(250);

  /**
   * Camunda 8 answers awareness probes from its query API, which an exporter feeds
   * asynchronously: a workflow started moments ago exists in the engine and is not
   * searchable yet. The core waits this window out where it knows this cluster
   * holds the workflow, so the everyday "start a workflow, then correlate the
   * message which lets it continue" works without the application retrying.
   */
  @Override
  public io.vanillabp.integration.adapter.spi.WorkflowVisibilityDelay workflowVisibilityDelay() {

    final var window = workflowVisibilityTimeout == null
        ? DEFAULT_WORKFLOW_VISIBILITY_TIMEOUT
        : workflowVisibilityTimeout;
    return window.isZero() || window.isNegative()
        ? io.vanillabp.integration.adapter.spi.WorkflowVisibilityDelay.none()
        : new io.vanillabp.integration.adapter.spi.WorkflowVisibilityDelay(
            window, WORKFLOW_VISIBILITY_PROBE_INTERVAL);

  }

  /**
   * The default of this adapter: everything is shared unless the application
   * excludes it ({@code @NoSyncWithBPMS}).
   */
  /**
   * How deeply the scope hierarchy is walked when a task-scoped push looks for the
   * scope a task runs in. Ten levels of nested subprocesses are a model nobody reads
   * any more, and the bound keeps a broken answer of the query API from looping.
   */
  private static final int MAX_SCOPE_DEPTH = 10;

  public static final io.vanillabp.integration.adapter.spi.AggregateSyncMode SYNC_MODE = io.vanillabp.integration.adapter.spi.AggregateSyncMode.FULL;

  /**
   * The core's name-clash-avoidance model (story 35): translates BPMN process ids,
   * message names and error codes into what the cluster knows, and decides the
   * tenant an operation runs in. May be <code>null</code> (tests): identifiers are
   * passed through and the configured tenant is used, as before.
   */
  private io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport scoping;

  /**
   * Sets the name-clash-avoidance support (constructor injection is not possible -
   * this class is built by Lombok's all-args constructor, which the platform
   * modules call).
   *
   * @param scoping The name-clash-avoidance support
   */
  public void setScoping(
      final io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport scoping) {

    this.scoping = scoping;

  }

  /**
   * The BPMN process id as the cluster knows it.
   */
  private String scopedProcessId(
      final String workflowModuleId,
      final String bpmnProcessId) {

    return scoping == null
        ? bpmnProcessId
        : scoping.scopedProcessId(workflowModuleId, bpmnProcessId, adapterId);

  }

  /**
   * A message name / error code as the cluster knows it.
   */
  private String scopedIdentifier(
      final String workflowModuleId,
      final String identifier) {

    return scoping == null
        ? identifier
        : scoping.scopedIdentifier(workflowModuleId, identifier, adapterId);

  }

  /**
   * The tenant an operation of the given workflow module runs in - see the
   * name-clash-avoidance mode (story 35).
   */
  private String tenantIdOf(
      final String workflowModuleId) {

    return io.vanillabp.camunda8.wiring.Camunda8Scoping.tenantIdFor(
        scoping, workflowModuleId, adapterId, clientFactory
            .getConfiguration()
            .getTenantId());

  }

  /**
   * The process variables written whenever this adapter talks to the cluster on
   * behalf of a workflow: the aggregate's shared attributes PLUS - always, no
   * matter what the sync model says - the technical variable carrying the
   * aggregate's ID (named after the aggregate's ID property). Camunda 8 has no
   * business key: that variable is how VanillaBP finds the workflow again.
   *
   * @param aggregatePersistence The aggregate's persistence
   * @param workflowAggregateId The aggregate's ID
   * @return The variables (never <code>null</code>)
   */
  private java.util.Map<String, Object> variablesOf(
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId) {

    final var variables = new java.util.LinkedHashMap<String, Object>();
    if (aggregatePersistence == null) {
      // no persistence at hand (e.g. a test driving the SPI directly): neither the
      // shared attributes nor the technical ID variable can be determined
      return variables;
    }
    if (aggregateSync != null) {
      final var aggregate = aggregatePersistence.loadById(workflowAggregateId);
      if (aggregate != null) {
        variables.putAll(aggregateSync.syncedValues(aggregate, SYNC_MODE));
      } else {
        log.warn(
            "Camunda8[{}]: the workflow aggregate '{}' could not be loaded - only the technical "
                + "aggregate-ID variable is written to the cluster",
            adapterId,
            workflowAggregateId);
      }
    }
    variables.put(
        aggregatePersistence.getAggregateIdName(),
        workflowAggregateId == null
            ? null
            : workflowAggregateId.toString());
    return variables;

  }

  @Override
  public String getAdapterId() {

    return adapterId;

  }

  @Override
  public boolean needsTwoPhaseCommitForStartingWorkflows() {

    return true;

  }

  @Override
  public boolean deliversTasksAtLeastOnce() {

    // job workers report the outcome AFTER the local transaction was committed, so a
    // crash in between makes the cluster hand the same job to a worker again (story
    // 51). The identity across such a redelivery is the JOB KEY, reported by every
    // invocation context.
    return true;

  }

  @Override
  public WorkflowAwareness awarenessOfTask(
      final Object workflowAggregateId,
      final String taskId) {

    // the probe is UpdateJobTimeout - a NON-ADVANCING command which doubles as a lock
    // renewal (the job's lock is set to the renewal window, the same value the worker
    // granted when the task was left open). Camunda 8 cannot
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
        .timeout(asyncTaskLockRenewal)
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

    registerPreCommitExistenceCheck(aggregateClassOf(aggregatePersistence), taskId, "completing");

  }

  @Override
  public void cancelTaskPhaseOne(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final A workflowAggregate,
      final String taskId,
      final String bpmnErrorCode) {

    registerPreCommitExistenceCheck(aggregateClassOf(aggregatePersistence), taskId, "canceling");

  }

  /**
   * The workflow aggregate whose transaction a phase-one check belongs into (story 87).
   * <p>
   * The core always hands the aggregate's persistence along; only tests call phase one
   * without one, and {@code Object.class} then resolves the platform's own runner - which
   * is what a test without any aggregate persistence has anyway.
   *
   * @param aggregatePersistence The persistence of the call at hand, may be
   *          <code>null</code> in tests
   * @return The aggregate class
   */
  private Class<?> aggregateClassOf(
      final AggregatePersistenceAware<A> aggregatePersistence) {

    return aggregatePersistence == null
        ? Object.class
        : aggregatePersistence.getAggregateClass();

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
      final Class<?> workflowAggregateClass,
      final String taskId,
      final String operationDescription) {

    preCommitRegistrar.beforeCommit(workflowAggregateClass, () -> {
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
    preCommitRegistrar.beforeCommit(aggregateClassOf(aggregatePersistence), () -> {
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
          .variables(variablesOf(aggregatePersistence, workflowAggregateId))
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

    // No Camunda 8 cluster up to 8.9 offers a command to cancel a Camunda-managed user
    // task by BPMN error: ThrowError is job-based (a zeebe:userTask has no job), and the
    // V1 workaround (completing the task with a marker variable evaluated by a listener)
    // is marked "currently not working" in the V1 adapter itself. The task/execution
    // listeners of 8.10 are what this needs, so it can only ever arrive on a line built
    // against 8.10 - see the prepared follow-up prompt. The message names the line
    // because that is what the reader has to change to get it.
    return new UnsupportedOperationException(
        ("Canceling user task '%s' of BPMN process '%s' by BPMN error is not supported by "
            + "the Camunda 8 cluster of release line %s! The engine offers no command for it "
            + "(ThrowError is job-based; a Camunda-managed user task has no job). Model the "
            + "error path explicitly, e.g. a boundary message or signal. The task listeners "
            + "this needs arrive with Camunda 8.10, so support for it can only ever come on a "
            + "line built against 8.10 or later.")
            .formatted(taskId, bpmnProcessId, Camunda8ReleaseLine.id()));

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
          // story 28: the aggregate changed before the task was completed - the
          // cluster only sees what VanillaBP pushes
          .variables(variablesOf(aggregatePersistence, workflowAggregateId))
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
          // the model's error codes are prefixed too (story 35)
          .errorCode(scopedIdentifier(workflowModuleId, bpmnErrorCode))
          .errorMessage("canceled via ProcessService#cancelTask")
          // story 28b: the error boundary's outgoing path may branch on the
          // aggregate, which the caller changed before canceling the task
          .variables(variablesOf(aggregatePersistence, workflowAggregateId))
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

  /**
   * Logged once per adapter: probing workflow awareness needs the query API
   * (secondary storage) - without it the adapter answers OPTIMISTICALLY.
   */
  private final java.util.concurrent.atomic.AtomicBoolean noSecondaryStorageWarned = new java.util.concurrent.atomic.AtomicBoolean();

  @Override
  public WorkflowAwareness awarenessOfWorkflow(
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId) {

    // Zeebe offers NO engine command answering "does an instance for this
    // aggregate exist" - only the eventually-consistent query API (requires
    // secondary storage, standard in any real Camunda 8 setup). The search
    // filters by the aggregate-ID process variable.
    //
    // It does NOT filter by state, although only an ACTIVE instance can be advanced:
    // an ended workflow is COMPLETED, not UNKNOWN_TO_BPMS. The difference is the whole
    // point of the two values - "unknown" permits falling back to the next adapter and
    // is what the viewer/history API reports as WorkflowNotFoundException, while
    // "completed" says this BPMS is the one which held the workflow. Filtering here
    // made every read of an ended workflow fail and turned an operation arriving too
    // late into a lookup failure.
    try {
      final var found = clientFactory
          .getClient()
          .newProcessInstanceSearchRequest()
          .filter(filter -> filter
              .variables(java.util.Map
                  .of(aggregateIdVariableName(aggregatePersistence),
                      Camunda8VariableFilters.aggregateIdSearchValue(workflowAggregateId))))
          .send()
          .join();
      if (found.items().isEmpty()) {
        return WorkflowAwareness.UNKNOWN_TO_BPMS;
      }
      return found
          .items()
          .stream()
          .anyMatch(instance -> instance.getState() == io.camunda.client.api.search.enums.ProcessInstanceState.ACTIVE)
              ? WorkflowAwareness.ACTIVE
              : WorkflowAwareness.COMPLETED;
    } catch (final Exception e) {
      if (isSecondaryStorageMissing(e)) {
        // OPTIMISTIC fallback: without the query API the instance's existence
        // cannot be probed. Correlation publishes are buffered by the engine
        // anyway (message TTL); in MULTI-BPMS migration setups this answer may
        // route an operation to the wrong BPMS - configure secondary storage
        // there (guiding WARN below).
        if (noSecondaryStorageWarned.compareAndSet(false, true)) {
          log.warn(
              "Camunda8[{}]: the cluster runs WITHOUT secondary storage - workflow awareness "
                  + "cannot be probed and is answered OPTIMISTICALLY (ACTIVE). Fine for "
                  + "single-BPMS setups; for BPMS migration scenarios configure the query API "
                  + "(camunda.database.type / secondary storage).",
              adapterId);
        }
        return WorkflowAwareness.ACTIVE;
      }
      log.warn(
          "Camunda8[{}]: could not determine awareness of the workflow of aggregate '{}' - "
              + "reporting BPMS_UNAVAILABLE",
          adapterId,
          workflowAggregateId,
          e);
      return WorkflowAwareness.BPMS_UNAVAILABLE;
    }

  }

  /**
   * The START re-dispatch mitigation probe (story 25) - STRICTER contract than
   * {@link #awarenessOfWorkflow(AggregatePersistenceAware, Object)}: the answer must NEVER be optimistic
   * (an optimistic ACTIVE would SKIP a recovered start = a lost workflow,
   * whereas a duplicate start is the accepted at-least-once residual).
   * Differences to the election probe:
   * <ul>
   * <li>no state filter - a workflow COMPLETED since the crashed start still
   * proves the start succeeded;</li>
   * <li>without secondary storage the answer is an honest
   * {@link WorkflowAwareness#UNKNOWN_TO_BPMS} (instead of the election's
   * optimistic ACTIVE): the start proceeds and this adapter's
   * {@link #startWorkflowPhaseTwo} at-least-once contract applies.</li>
   * </ul>
   */
  @Override
  public WorkflowAwareness awarenessOfWorkflowForRedispatch(
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId) {

    try {
      final var found = clientFactory
          .getClient()
          .newProcessInstanceSearchRequest()
          .filter(filter -> filter
              .variables(java.util.Map
                  .of(aggregateIdVariableName(aggregatePersistence),
                      Camunda8VariableFilters.aggregateIdSearchValue(workflowAggregateId))))
          .send()
          .join();
      return found.items().isEmpty()
          ? WorkflowAwareness.UNKNOWN_TO_BPMS
          : WorkflowAwareness.ACTIVE;
    } catch (final Exception e) {
      if (isSecondaryStorageMissing(e)) {
        log.debug(
            "Camunda8[{}]: no secondary storage - the re-dispatch mitigation cannot probe, the "
                + "start proceeds (duplicates within the documented at-least-once residual are "
                + "possible)",
            adapterId);
        return WorkflowAwareness.UNKNOWN_TO_BPMS;
      }
      log.warn(
          "Camunda8[{}]: could not probe the workflow of aggregate '{}' before re-dispatching "
              + "its start - reporting BPMS_UNAVAILABLE (the outbox entry is retried)",
          adapterId,
          workflowAggregateId,
          e);
      return WorkflowAwareness.BPMS_UNAVAILABLE;
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

  /**
   * The name of the process variable holding the workflow-aggregate ID: Camunda 8
   * has no business key, so every lookup of a workflow filters by that variable.
   * <p>
   * It is ALWAYS derived from the aggregate persistence of the call at hand, never
   * remembered between calls. One process service serves every workflow module and
   * aggregate of its adapter id, so a remembered name would be the one of whichever
   * aggregate was handled last. That was this adapter's bug until story 54: the
   * awareness probe ran before any call carrying a persistence and searched for the
   * placeholder name, which found nothing on a cluster with secondary storage, so
   * every operation locating its workflow by the probe failed.
   *
   * @param aggregatePersistence The persistence of the aggregate of this call
   * @return The variable name
   */
  private String aggregateIdVariableName(
      final AggregatePersistenceAware<A> aggregatePersistence) {

    final var name = aggregatePersistence == null
        ? null
        : aggregatePersistence.getAggregateIdName();
    if (name == null) {
      throw new IllegalStateException(
          """
              Camunda 8 cannot look up the workflow of an aggregate without knowing the name of \
              its ID attribute! The aggregate persistence has to answer getAggregateIdName() - \
              it names the process variable VanillaBP writes the aggregate's ID into.""");
    }
    return name;

  }

  @Override
  public boolean isPhaseTwoFailureRepeatable(
      final Throwable failure) {

    // story 73: the outbox repeats what a second attempt may fix - a cluster which
    // is busy, unreachable or lost a conflict. A command the cluster REJECTS looks
    // the same on every attempt, and so does a task key which is not a number. The
    // list of those cases lives in Camunda8Errors, next to the job-gone rule
    return !Camunda8Errors.permanentFailure(failure);

  }

  /**
   * Phase one of a message correlation asks the MODEL, not the cluster (story 73).
   * <p>
   * A subscription search exists since the client version this adapter builds
   * against, and it would be the wrong check: the cluster BUFFERS a message for its
   * time-to-live, so correlating before the subscription exists is legitimate and a
   * search would reject exactly that case - besides reading the eventually consistent
   * secondary storage, whose window the caller would wait out inside their
   * transaction.
   * <p>
   * What can be checked without asking anybody is whether the deployed models of this
   * workflow module declare the message at all. A name no model knows is a typo or a
   * renamed message, and phase two would publish it into the void: the cluster accepts
   * the publication, the TTL passes, nothing ever correlates. So the mistake is
   * reported where the application made the call.
   * <p>
   * The check stays silent where this application version deployed no process of the
   * workflow module (a workflow still running on a definition of a previous version -
   * see {@link Camunda8DeployedProcesses}), because then the declared names are
   * unknown rather than absent.
   */
  @Override
  public void correlateMessagePhaseOne(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final A workflowAggregate,
      final String messageName,
      final String correlationId) {

    clientFactory.validateConfigured();
    validateMessageIsDeclared(workflowModuleId, messageName);

  }

  /**
   * Reports a message name the deployed models of the workflow module do not declare.
   *
   * @param workflowModuleId The workflow module of the correlation
   * @param messageName The message name the application passed
   */
  private void validateMessageIsDeclared(
      final String workflowModuleId,
      final String messageName) {

    final var deployed = clientFactory
        .getDeployedProcesses()
        .ofWorkflowModule(workflowModuleId);
    if (deployed.isEmpty()) {
      return;
    }
    // the models carry the SCOPED names (story 35 renames messages while deploying),
    // so the name of the call is scoped the same way the publication scopes it
    final var scopedMessageName = scopedIdentifier(workflowModuleId, messageName);
    final var declared = deployed
        .stream()
        .flatMap(process -> declaredMessageNames(process.model()).stream())
        .collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new));
    if (declared.contains(scopedMessageName)) {
      return;
    }
    throw new IllegalArgumentException(
        """
            No BPMN model of workflow module '%s' declares a message '%s'! Camunda 8 would accept \
            the publication and buffer the message until its time-to-live passed, so nothing would \
            ever correlate and nothing would fail. The messages declared by the models this \
            application deployed are: %s. Correct the name passed to correlateMessage, or declare \
            the message at the event which waits for it."""
            .formatted(
                workflowModuleId,
                scopedMessageName.equals(messageName)
                    ? messageName
                    : "%s (scoped: '%s')".formatted(messageName, scopedMessageName),
                declared.isEmpty()
                    ? "none"
                    : declared));

  }

  /**
   * The message names a deployed model declares: message catch events (intermediate,
   * boundary, event-subprocess start) and receive tasks - the same elements
   * {@code Camunda8TaskWiring#wireMessageSubscriptions} wires a correlation key into.
   * Message START events are included as well: a start by message goes through
   * {@code startWorkflowByMessage}, but declaring the name is what matters here.
   *
   * @param model The model as deployed
   * @return The declared message names
   */
  private static java.util.Set<String> declaredMessageNames(
      final io.camunda.zeebe.model.bpmn.BpmnModelInstance model) {

    return model
        .getModelElementsByType(model
            .getModel()
            .getType(io.camunda.zeebe.model.bpmn.instance.Message.class))
        .stream()
        .map(io.camunda.zeebe.model.bpmn.instance.Message.class::cast)
        .map(io.camunda.zeebe.model.bpmn.instance.Message::getName)
        .filter(java.util.Objects::nonNull)
        .filter(name -> !name.isBlank())
        .collect(java.util.stream.Collectors.toSet());

  }

  @Override
  public void correlateMessagePhaseTwo(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId,
      final String messageName,
      final String correlationId) {

    // correlationKey: the correlation id if given, the aggregate ID otherwise
    // (V1 semantics; the wired zeebe:subscription evaluates '=<idName>' - a
    // correlation id requires a model-side subscription on the matching variable).
    // messageId: the idempotency key WHERE ONE EXISTS (with a correlation id) -
    // the engine then deduplicates redeliveries within the message TTL; without
    // one, an at-least-once redelivery may double-correlate (documented).
    // PAYLOAD DOCTRINE: no message CONTENT travels - what does travel is the
    // aggregate state shared with the BPMS (story 28), because the cluster can
    // only evaluate BPMN expressions against variables it was given.
    final var correlationKey = correlationId != null
        ? correlationId
        : String.valueOf(workflowAggregateId);
    var command = clientFactory
        .getClient()
        .newPublishMessageCommand()
        .messageName(scopedIdentifier(workflowModuleId, messageName))
        .correlationKey(correlationKey)
        .variables(variablesOf(aggregatePersistence, workflowAggregateId));
    final var correlationTenantId = tenantIdOf(workflowModuleId);
    if (correlationTenantId != null) {
      command = command.tenantId(correlationTenantId);
    }
    if (correlationId != null) {
      command = command.messageId(
          "%s|%s|%s|%s|%s".formatted(
              workflowModuleId, bpmnProcessId, workflowAggregateId, messageName, correlationId));
    }
    try {
      command
          .send()
          .join();
      log.info(
          "Camunda8[{}]: published message '{}' (correlation key '{}') for BPMN process '{}' of "
              + "workflow module '{}'",
          adapterId,
          messageName,
          correlationKey,
          bpmnProcessId,
          workflowModuleId);
    } catch (final Exception e) {
      if (!isMessageAlreadyPublished(e)) {
        throw e;
      }
      // the engine deduplicated by messageId - a redelivered at-least-once
      // dispatch, the entry is consumed
      log.warn(
          "Camunda8[{}]: message '{}' (id-deduplicated) was already published - skipping the "
              + "redelivered phase-two correlation",
          adapterId,
          messageName);
    }

  }

  private static boolean isMessageAlreadyPublished(
      final Throwable throwable) {

    var current = throwable;
    while (current != null) {
      final var message = current.getMessage();
      if ((message != null) && (message.contains("ALREADY_EXISTS") || message.contains("already been published"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;

  }

  @Override
  public void startWorkflowByMessagePhaseOne(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final A workflowAggregate,
      final String messageName) {

    clientFactory.validateConfigured();

  }

  /**
   * A remote BPMS must not act before the caller's transaction committed: phase one
   * does nothing, the broadcast happens in phase two through the outbox.
   */
  @Override
  public void sendSignalPhaseOne(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String signalName) {

  }

  @Override
  public void sendSignalPhaseTwo(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String signalName) {

    // no variables travel with a signal, and there is nothing to deduplicate by:
    // unlike a message, a broadcast carries no correlation key the cluster could
    // recognize a redelivery from (documented at-least-once residual)
    var command = clientFactory
        .getClient()
        .newBroadcastSignalCommand()
        .signalName(scopedIdentifier(workflowModuleId, signalName));
    final var signalTenantId = tenantIdOf(workflowModuleId);
    if (signalTenantId != null) {
      command = command.tenantId(signalTenantId);
    }
    command
        .send()
        .join();
    log.info(
        "Camunda8[{}]: broadcast signal '{}' of workflow module '{}'",
        adapterId,
        signalName,
        workflowModuleId);

  }

  @Override
  public void aggregateChangedPhaseOne(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final A workflowAggregate,
      final String taskId) {

    // a remote BPMS: writing here would show the cluster values of a transaction
    // which may still roll back - the push happens in phase two

  }

  @Override
  public void aggregateChangedPhaseTwo(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId,
      final String taskId) {

    final var variables = variablesOf(aggregatePersistence, workflowAggregateId);

    if (taskId == null) {
      final var processInstanceKey = processInstanceKeyOf(aggregatePersistence, workflowAggregateId);
      if (processInstanceKey == null) {
        // at-least-once residual: the workflow ended between the dispatch-time
        // election and now - there is nothing left to write to
        log.warn(
            "Camunda8[{}]: no active workflow found for aggregate '{}' - skipping the push of the "
                + "changed aggregate",
            adapterId,
            workflowAggregateId);
        return;
      }
      clientFactory
          .getClient()
          .newSetVariablesCommand(processInstanceKey)
          .variables(variables)
          // the workflow's own scope, which is what a gateway behind the current
          // element and every other branch reads
          .local(false)
          .send()
          .join();
      log.info(
          "Camunda8[{}]: pushed the changed aggregate '{}' into process instance '{}'",
          adapterId,
          workflowAggregateId,
          processInstanceKey);
      return;
    }

    final var elementInstanceKey = flowScopeKeyOf(taskId);
    if (elementInstanceKey == null) {
      log.warn(
          "Camunda8[{}]: the scope of task '{}' of aggregate '{}' was not found within {} - skipping "
              + "the push of the changed aggregate. Either the task was completed meanwhile, or the "
              + "query API did not catch up with it: raise "
              + "'vanillabp.adapters.{}.workflow-visibility-timeout' if this cluster's exporter "
              + "regularly needs longer. The workflow's own scope is deliberately NOT written "
              + "instead - it is read by every branch, and the task asked for its own scope",
          adapterId,
          taskId,
          workflowAggregateId,
          workflowVisibilityDelay().window(),
          adapterId);
      return;
    }
    clientFactory
        .getClient()
        .newSetVariablesCommand(elementInstanceKey)
        .variables(variables)
        // the scope the task RUNS IN - a workflow-wide write would be a lost update
        // between the iterations of a multi-instance subprocess
        .local(true)
        .send()
        .join();
    log.info(
        "Camunda8[{}]: pushed the changed aggregate '{}' into element instance '{}' (task '{}')",
        adapterId,
        workflowAggregateId,
        elementInstanceKey,
        taskId);

  }

  /**
   * The key of the ACTIVE process instance carrying the aggregate's ID variable -
   * Camunda 8 has no business key, so the eventually-consistent query API answers
   * (like {@link #awarenessOfWorkflow(AggregatePersistenceAware, Object)}).
   *
   * @param workflowAggregateId The aggregate's ID
   * @return The process instance key or <code>null</code> if none is active
   */
  private Long processInstanceKeyOf(
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId) {

    try {
      final var found = clientFactory
          .getClient()
          .newProcessInstanceSearchRequest()
          .filter(filter -> filter
              .state(io.camunda.client.api.search.enums.ProcessInstanceState.ACTIVE)
              .variables(java.util.Map
                  .of(aggregateIdVariableName(aggregatePersistence),
                      Camunda8VariableFilters.aggregateIdSearchValue(workflowAggregateId))))
          .send()
          .join();
      return found.items().isEmpty()
          ? null
          : found.items().getFirst().getProcessInstanceKey();
    } catch (final Exception e) {
      throw queryApiRequired(e, "the workflow of aggregate '%s'".formatted(workflowAggregateId));
    }

  }

  /**
   * The element instance of the scope the task RUNS IN: the process instance, an
   * embedded subprocess, or the one iteration of a multi-instance embedded subprocess
   * it belongs to.
   * <p>
   * Not the task's own element instance: in Camunda 8 every element instance is a
   * variable scope of its own, and one belonging to a task disappears with the task -
   * values written there would be read by nothing. The scope AROUND the task is what
   * the rest of that scope evaluates.
   * <p>
   * Camunda 8 reports no parent for an element instance, so the scope is found by
   * walking DOWN from the process instance (the query API filters element instances
   * by their scope) until the task's element instance shows up. A multi-instance BODY
   * on the way is skipped: it is the technical wrapper around the instances, not a
   * scope of the model.
   * <p>
   * The query API is fed by an exporter, so the task this push belongs to may not be
   * reported yet - which is why the search is repeated for as long as
   * {@link #workflowVisibilityDelay()} allows. A scope which stays unknown yields
   * <code>null</code>: the process instance is NOT used as a substitute, because
   * writing there is exactly the lost update between the iterations of a
   * multi-instance subprocess this scoping exists to prevent.
   *
   * @param taskId The task ID reported to the application (the job key)
   * @return The element instance key to write at, or <code>null</code> if the scope
   *         did not become known within the window
   */
  private Long flowScopeKeyOf(
      final String taskId) {

    final var delay = workflowVisibilityDelay();
    final var deadline = System.currentTimeMillis() + (delay.isWaiting()
        ? delay.window().toMillis()
        : 0);
    while (true) {
      final var scopeKey = searchFlowScopeKeyOf(taskId);
      if (scopeKey != null) {
        return scopeKey;
      }
      if (System.currentTimeMillis() >= deadline) {
        return null;
      }
      try {
        Thread.sleep(delay.interval().toMillis());
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }

  }

  /**
   * One attempt of {@link #flowScopeKeyOf(String)}.
   *
   * @param taskId The task ID reported to the application (the job key)
   * @return The element instance key to write at, or <code>null</code> if the query
   *         API knows neither the task nor its scope (yet)
   */
  private Long searchFlowScopeKeyOf(
      final String taskId) {

    final var job = jobOf(taskId);
    if (job == null) {
      return null;
    }
    final var scopes = scopePathOf(job.getProcessInstanceKey(), job.getElementInstanceKey());
    // innermost first: the scope holding the task, then its own scopes
    for (final var scope : scopes) {
      if (scope.type() != io.camunda.client.api.search.enums.ElementInstanceType.MULTI_INSTANCE_BODY) {
        return scope.key();
      }
    }
    return null;

  }

  /**
   * A scope on the way from the process instance down to an element instance.
   *
   * @param key The element instance key of the scope
   * @param type What kind of element it is
   */
  private record Scope(Long key, io.camunda.client.api.search.enums.ElementInstanceType type) {
  }

  /**
   * The scopes containing the given element instance, innermost first. Walks down
   * from the process instance, because Camunda 8 reports children of a scope but
   * never the parent of one.
   *
   * @param processInstanceKey The workflow's process instance
   * @param elementInstanceKey The element instance to find
   * @return The containing scopes, innermost first (empty if the element instance was
   *         not found below the process instance)
   */
  private java.util.List<Scope> scopePathOf(
      final Long processInstanceKey,
      final Long elementInstanceKey) {

    final var path = new java.util.LinkedList<Scope>();
    if ((processInstanceKey == null) || (elementInstanceKey == null)) {
      return path;
    }
    if (findScopePath(
        new Scope(processInstanceKey, io.camunda.client.api.search.enums.ElementInstanceType.PROCESS),
        elementInstanceKey,
        path,
        0)) {
      return path;
    }
    return path;

  }

  /**
   * Depth-first walk down the scope hierarchy, collecting the scopes containing the
   * wanted element instance.
   *
   * @param scope The scope to look below
   * @param elementInstanceKey The element instance to find
   * @param path Filled with the containing scopes, innermost first
   * @param depth The current nesting depth (bounded - a BPMN model is not a graph)
   * @return Whether the element instance was found below this scope
   */
  private boolean findScopePath(
      final Scope scope,
      final Long elementInstanceKey,
      final java.util.LinkedList<Scope> path,
      final int depth) {

    if (depth > MAX_SCOPE_DEPTH) {
      return false;
    }
    final var children = clientFactory
        .getClient()
        .newElementInstanceSearchRequest()
        .filter(filter -> filter.elementInstanceScopeKey(scope.key()))
        .send()
        .join()
        .items();
    for (final var child : children) {
      if (elementInstanceKey.equals(child.getElementInstanceKey())) {
        path.add(scope);
        return true;
      }
    }
    for (final var child : children) {
      if (findScopePath(
          new Scope(child.getElementInstanceKey(), child.getType()),
          elementInstanceKey,
          path,
          depth + 1)) {
        path.add(scope);
        return true;
      }
    }
    return false;

  }

  /**
   * The job behind a task ID - a VanillaBP task ID IS the job's key, and the job
   * knows which element instance and which process instance it belongs to.
   *
   * @param taskId The task ID reported to the application
   * @return The job or <code>null</code> if the query API does not know it
   */
  private io.camunda.client.api.search.response.Job jobOf(
      final String taskId) {

    try {
      final var found = clientFactory
          .getClient()
          .newJobSearchRequest()
          .filter(filter -> filter.jobKey(Long.parseLong(taskId)))
          .send()
          .join();
      return found.items().isEmpty()
          ? null
          : found.items().getFirst();
    } catch (final Exception e) {
      throw queryApiRequired(e, "the task '%s'".formatted(taskId));
    }

  }

  /**
   * The guiding failure of a push which cannot find WHERE to write. Camunda 8 has
   * neither a business key nor a command addressing a workflow by one of its
   * variables, so the query API is the only way from an aggregate ID to the keys
   * {@code SetVariables} needs - a cluster without secondary storage cannot serve
   * this feature at all.
   *
   * @param cause What the search failed with
   * @param subject What was searched for
   * @return The exception to throw
   */
  private RuntimeException queryApiRequired(
      final Exception cause,
      final String subject) {

    if (isSecondaryStorageMissing(cause)) {
      return new UnsupportedOperationException(
          ("Camunda8[%s]: cannot push a changed workflow-aggregate - %s cannot be located because "
              + "the cluster runs WITHOUT secondary storage. Camunda 8 addresses variables by "
              + "process-instance and element-instance keys only, and the query API is what "
              + "translates the aggregate's ID into them: configure secondary storage "
              + "(camunda.database.type) or push the aggregate by completing a task instead.")
              .formatted(adapterId, subject), cause);
    }
    if (cause instanceof RuntimeException runtimeException) {
      return runtimeException;
    }
    return new RuntimeException(cause);

  }

  @Override
  public void startWorkflowByMessagePhaseTwo(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId,
      final String messageName) {

    // message START events ignore the correlation key; the aggregate-ID variable
    // is the ONLY variable published (the same technical field a regular start
    // sets - not message content). messageId = the start's idempotency key so the
    // engine deduplicates redelivered dispatches within the message TTL.
    try {
      var startCommand = clientFactory
          .getClient()
          .newPublishMessageCommand()
          .messageName(scopedIdentifier(workflowModuleId, messageName))
          .correlationKey("")
          .messageId("%s|%s|%s".formatted(workflowModuleId, bpmnProcessId, workflowAggregateId))
          .variables(variablesOf(aggregatePersistence, workflowAggregateId));
      final var startTenantId = tenantIdOf(workflowModuleId);
      if (startTenantId != null) {
        startCommand = startCommand.tenantId(startTenantId);
      }
      startCommand
          .send()
          .join();
      log.info(
          "Camunda8[{}]: published start message '{}' for BPMN process '{}' of workflow module "
              + "'{}' (aggregate '{}')",
          adapterId,
          messageName,
          bpmnProcessId,
          workflowModuleId,
          workflowAggregateId);
    } catch (final Exception e) {
      if (!isMessageAlreadyPublished(e)) {
        throw e;
      }
      log.info(
          "Camunda8[{}]: start message '{}' for aggregate '{}' was already published - skipping "
              + "the redelivered phase-two start",
          adapterId,
          messageName,
          workflowAggregateId);
    }

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

    createProcessInstance(
        scopedProcessId(workflowModuleId, bpmnProcessId),
        variablesOf(aggregatePersistence, workflowAggregateId),
        workflowAggregateId,
        tenantIdOf(workflowModuleId));

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
      final java.util.Map<String, Object> variables,
      final Object workflowAggregateId) {

    return createProcessInstance(bpmnProcessId, variables, workflowAggregateId, tenantIdOf(null));

  }

  /**
   * Creates the instance in the given tenant - which the name-clash-avoidance mode
   * decides (story 35): the workflow module id under {@code by-adapter}, none under
   * {@code use-prefix}/{@code none}.
   *
   * @param bpmnProcessId The BPMN process ID AS THE CLUSTER KNOWS IT
   * @param variables The process variables
   * @param workflowAggregateId The workflow aggregate's ID (for logging)
   * @param tenantId The tenant or <code>null</code>
   * @return The created process-instance event
   */
  public ProcessInstanceEvent createProcessInstance(
      final String bpmnProcessId,
      final java.util.Map<String, Object> variables,
      final Object workflowAggregateId,
      final String tenantId) {

    final var client = clientFactory.getClient();
    var command = client
        .newCreateInstanceCommand()
        .bpmnProcessId(bpmnProcessId)
        .latestVersion()
        .variables(variables);

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


  /**
   * The viewer/history API (story 26) - see {@link Camunda8WorkflowViewer} for the
   * two data sources (what this application version deployed vs. the cluster's
   * query API) and the consistency caveats.
   */
  private volatile Camunda8WorkflowViewer viewer;

  private Camunda8WorkflowViewer viewer() {

    // built on first use: this class is constructed by Lombok's all-args
    // constructor, so a field initializer could not reference the final fields
    if (viewer == null) {
      viewer = new Camunda8WorkflowViewer(adapterId, clientFactory, this::scopedProcessId, this::tenantIdOf);
    }
    return viewer;

  }

  @Override
  public java.util.List<io.vanillabp.spi.process.ProcessDefinition> getProcessDefinitions(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId,
      final String historyContext) {

    return viewer().getProcessDefinitions(
        workflowModuleId, bpmnProcessId, aggregateIdVariableName(aggregatePersistence), workflowAggregateId,
        historyContext);

  }

  @Override
  public java.io.InputStream getBpmnXml(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String processDefinitionId) {

    return viewer().getBpmnXml(processDefinitionId);

  }

  @Override
  public io.vanillabp.spi.process.WorkflowHistory getWorkflowHistory(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final Object workflowAggregateId,
      final String historyContext) {

    return viewer().getWorkflowHistory(
        workflowModuleId, bpmnProcessId, aggregateIdVariableName(aggregatePersistence), workflowAggregateId,
        historyContext);

  }

}
