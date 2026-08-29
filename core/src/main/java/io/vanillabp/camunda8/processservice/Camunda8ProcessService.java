package io.vanillabp.camunda8.processservice;

import java.util.Map;

import io.camunda.client.api.response.ProcessInstanceEvent;
import io.vanillabp.camunda8.Camunda8ReleaseLine;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.client.Camunda8Errors;
import io.vanillabp.integration.adapter.spi.MigratableProcessService;
import io.vanillabp.integration.adapter.spi.PhaseOneRequest;
import io.vanillabp.integration.adapter.spi.PhaseOperationHandler;
import io.vanillabp.integration.adapter.spi.PhaseTwoRequest;
import io.vanillabp.integration.adapter.spi.WorkflowAwareness;
import io.vanillabp.integration.spi.AggregatePersistenceAware;
import io.vanillabp.integration.spi.PhaseOperation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Camunda 8 implementation of the {@link MigratableProcessService}. One instance is
 * created per configured adapter ID (not per adapter type).
 * <p>
 * Camunda 8 is a <b>remote</b>, eventually consistent BPMS: the engine cannot join the
 * application's local database transaction, so starting a workflow is routed through the
 * core {@code PhaseTwoOutbox} like every other operation which reaches the cluster:
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
 *       instance via {@link #createProcessInstance(String, java.util.Map, Object)}.</li>
 * </ul>
 *
 * @param <A> The workflow-aggregate type
 */
@Slf4j
@RequiredArgsConstructor
// see decision 4 in the repository's DECISIONS.md
@SuppressWarnings("LombokSetterMayBeUsed")
public class Camunda8ProcessService<A> implements MigratableProcessService<A> {

  private final String adapterId;

  private final Camunda8ClientFactory clientFactory;

  /**
   * The one-time job-lock extension applied by awareness probes and phase-one
   * checks (the same duration the job worker grants a dormant async task).
   */
  private final java.time.Duration asyncTaskLockRenewal;

  /**
   * Runs phase-one existence checks right before the commit of the workflow aggregate's
   * transaction (platform-supplied) - minimizes the window between check and
   * phase two. The platform resolves the runner of the aggregate, so a unit of work the
   * APPLICATION brought is the one hooked into.
   */
  private final io.vanillabp.integration.adapter.spi.PreCommitRegistrar preCommitRegistrar;

  /**
   * The core's sync model: which aggregate attributes are shared with
   * the cluster. Camunda 8 is REMOTE, so its default is
   * {@link io.vanillabp.integration.adapter.spi.AggregateSyncMode#FULL} - a BPMN
   * expression can only see what VanillaBP
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
   * How deeply the scope hierarchy is walked when a task-scoped push looks for the
   * scope a task runs in. Ten levels of nested subprocesses are a model nobody reads
   * any more, and the bound keeps a broken answer of the query API from looping.
   */
  private static final int MAX_SCOPE_DEPTH = 10;

  /**
   * The default of this adapter: everything is shared unless the application
   * excludes it ({@code @NoSyncWithBPMS}).
   */
  public static final io.vanillabp.integration.adapter.spi.AggregateSyncMode SYNC_MODE = io.vanillabp.integration.adapter.spi.AggregateSyncMode.FULL;

  /**
   * The core's name-clash-avoidance model: translates BPMN process ids,
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
   * Resolves how long the cluster keeps a message this adapter publishes, per adapter,
   * workflow module, workflow and message. May be <code>null</code> (tests, and a platform
   * written before this): the client's own default applies then and VanillaBP sets nothing
   * on the command.
   */
  private io.vanillabp.camunda8.wiring.Camunda8MessageTimeToLiveResolver messageTimeToLiveResolver;

  /**
   * Injected by the platform module after construction, like the scoping next to it - an
   * optional collaborator rather than a constructor argument, so a test building this
   * service by hand does not have to know about it.
   *
   * @param messageTimeToLiveResolver The resolver, or <code>null</code>
   */
  public void setMessageTimeToLiveResolver(
      final io.vanillabp.camunda8.wiring.Camunda8MessageTimeToLiveResolver messageTimeToLiveResolver) {

    this.messageTimeToLiveResolver = messageTimeToLiveResolver;

  }

  /**
   * The window the cluster keeps the given message in, or <code>null</code> where nothing
   * configures one.
   *
   * @param workflowModuleId The workflow module
   * @param bpmnProcessId The BPMN process
   * @param messageName The message name as the application wrote it
   * @return The time-to-live or <code>null</code>
   */
  private java.time.Duration messageTimeToLiveFor(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String messageName) {

    return messageTimeToLiveResolver == null
        ? null
        : messageTimeToLiveResolver.messageTimeToLiveFor(workflowModuleId, bpmnProcessId, messageName);

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
   * name-clash-avoidance mode.
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
   * <p>
   * Package-private so {@code Camunda8SharedValuesTest} can hold the second half of that
   * promise without a cluster: an aggregate annotated {@code @NoSyncWithBPMS} shares
   * nothing, and Camunda 8 has no business key, so losing the ID variable would start a
   * workflow nobody can find again.
   *
   * @param aggregatePersistence The aggregate's persistence
   * @param workflowAggregateId The aggregate's ID
   * @return The variables (never <code>null</code>)
   */
  java.util.Map<String, Object> variablesOf(
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

  /**
   * What this adapter does for each operation, in both phases.
   * <p>
   * Phase one is what a REMOTE cluster can be asked without advancing anything: a job
   * timeout renewal, an empty user-task update, the model's message names - and it runs
   * as a pre-commit hook, so the window to the phase-two dispatch stays small. Phase two
   * sends the command and tolerates what the at-least-once dispatch of the outbox
   * implies, up to the message deduplication the cluster runs on its own.
   */
  @Override
  public Map<PhaseOperation, PhaseOperationHandler<A>> phaseOperations() {

    return Map
        .ofEntries(
            Map
                .entry(
                    PhaseOperation.START_WORKFLOW,
                    PhaseOperationHandler.of(this::preflightStart, this::startWorkflow)),
            Map
                .entry(
                    PhaseOperation.START_WORKFLOW_BY_MESSAGE,
                    PhaseOperationHandler.of(this::preflightStartByMessage, this::startWorkflowByMessage)),
            Map
                .entry(
                    PhaseOperation.COMPLETE_TASK,
                    PhaseOperationHandler.of(this::preflightCompleteTask, this::completeTask)),
            Map
                .entry(
                    PhaseOperation.CANCEL_TASK,
                    PhaseOperationHandler.of(this::preflightCancelTask, this::cancelTask)),
            Map
                .entry(
                    PhaseOperation.COMPLETE_USER_TASK,
                    PhaseOperationHandler.of(this::preflightCompleteUserTask, this::completeUserTask)),
            Map
                .entry(
                    PhaseOperation.CANCEL_USER_TASK,
                    PhaseOperationHandler.of(this::preflightCancelUserTask, this::cancelUserTask)),
            Map
                .entry(
                    PhaseOperation.CORRELATE_MESSAGE,
                    PhaseOperationHandler.of(this::preflightCorrelateMessage, this::correlateMessage)),
            Map
                .entry(
                    PhaseOperation.SEND_SIGNAL,
                    PhaseOperationHandler.of(this::preflightSendSignal, this::sendSignal)),
            Map
                .entry(
                    PhaseOperation.AGGREGATE_CHANGED,
                    PhaseOperationHandler.of(this::preflightAggregateChanged, this::pushChangedAggregate)));

  }

  @Override
  public boolean deliversTasksAtLeastOnce() {

    // job workers report the outcome AFTER the local transaction was committed, so a
    // crash in between makes the cluster hand the same job to a worker again. The
    // identity across such a redelivery is the JOB KEY, reported by every
    // invocation context.
    return true;

  }

  @Override
  public Long openTaskCount(
      final String workflowModuleId,
      final String bpmnProcessId) {

    // asked once per BPMN process at startup, so one search is affordable where the
    // cluster can serve it at all. Both kinds VanillaBP delivers are jobs: a service
    // task IS a job, and a Camunda-managed user task reaches the application through
    // its listener job
    try {
      // the TOTAL rather than the page which came back, and one item fetched because
      // only the number is wanted
      final var found = clientFactory
          .getClient()
          .newJobSearchRequest()
          .filter(filter -> filter.processDefinitionId(scopedProcessId(workflowModuleId, bpmnProcessId)))
          .page(page -> page.limit(1))
          .send()
          .join();
      return found.page().totalItems();
    } catch (final Exception e) {
      if (isSecondaryStorageMissing(e)) {
        // a cluster without secondary storage cannot be asked what it is holding, and
        // a startup diagnosis is no reason to say so twice - the core stays silent
        return null;
      }
      log
          .debug(
              "Camunda8[{}]: the cluster did not answer how many tasks of '{}' are open",
              adapterId,
              bpmnProcessId,
              e);
      return null;
    }

  }

  @Override
  public WorkflowAwareness awarenessOfTask(
      final io.vanillabp.integration.adapter.spi.WorkflowScope scope,
      final Object workflowAggregateId,
      final String taskId) {

    // the probe is UpdateJobTimeout - a NON-ADVANCING command which doubles as a lock
    // renewal (the job's lock is set to the renewal window, the same value the worker
    // granted when the task was left open). Camunda 8 cannot
    // answer COMPLETED for jobs (a completed job is indistinguishable from a
    // never-existing one without the eventually-consistent search API), so a
    // successful "not found" maps to UNKNOWN_TO_BPMS.
    //
    // A job key is unique per CLUSTER, so where another adapter id addresses
    // the same one the probe would answer for its job and extend its lock on the way
    // (see decision 3 in the repository's DECISIONS.md).
    // Which scope the key belongs to is asked FIRST there, and nowhere else - the
    // question costs a query-API round trip.
    if (!belongsToThisAdapter(scope, taskId, false)) {
      return WorkflowAwareness.UNKNOWN_TO_BPMS;
    }
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
        .newUpdateTimeoutCommand(taskKeyOf(taskId))
        .timeout(asyncTaskLockRenewal)
        .send()
        .join();

  }

  private void preflightCompleteTask(
      final PhaseOneRequest<A> request) {

    registerPreCommitExistenceCheck(aggregateClassOf(request.aggregatePersistence()), request.taskId(), "completing");

  }

  private void preflightCancelTask(
      final PhaseOneRequest<A> request) {

    registerPreCommitExistenceCheck(aggregateClassOf(request.aggregatePersistence()), request.taskId(), "canceling");

  }

  /**
   * The workflow aggregate whose transaction a phase-one check belongs into.
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
      final io.vanillabp.integration.adapter.spi.WorkflowScope scope,
      final Object workflowAggregateId,
      final String taskId) {

    // the probe is an EMPTY UpdateUserTask - an engine command (unlike the
    // query API it needs no secondary storage) which never advances the task;
    // it answers NOT_FOUND for gone tasks. Side effect: modeller-defined
    // 'updating' task listeners fire - documented in the README.
    //
    // As for service tasks, a user-task key is unique per cluster, and on a
    // shared one the scope is asked before the task is claimed
    if (!belongsToThisAdapter(scope, taskId, true)) {
      return WorkflowAwareness.UNKNOWN_TO_BPMS;
    }
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
        .newUpdateUserTaskCommand(taskKeyOf(taskId))
        .action("io.vanillabp:probe")
        .send()
        .join();

  }

  private void preflightCompleteUserTask(
      final PhaseOneRequest<A> request) {

    // pre-commit existence check (non-advancing empty update) - same shape as
    // service tasks, see registerPreCommitExistenceCheck
    preCommitRegistrar.beforeCommit(aggregateClassOf(request.aggregatePersistence()), () -> {
      try {
        updateUserTask(request.taskId());
      } catch (final Exception e) {
        if (Camunda8Errors.jobAlreadyGone(e)) {
          throw new IllegalStateException(
              ("The user task '%s' is gone (completed or canceled meanwhile) - aborting the "
                  + "transaction completing it!")
                  .formatted(request.taskId()), e);
        }
        throw e;
      }
    });

  }

  private void completeUserTask(
      final PhaseTwoRequest<A> request) {

    try {
      clientFactory
          .getClient()
          .newCompleteUserTaskCommand(taskKeyOf(request.taskId()))
          .variables(variablesOf(request.aggregatePersistence(), request.workflowAggregateId()))
          .send()
          .join();
      log.info(
          "Camunda8[{}]: completed user task '{}' of BPMN process '{}' of workflow module '{}'",
          adapterId,
          request.taskId(),
          request.bpmnProcessId(),
          request.workflowModuleId());
    } catch (final Exception e) {
      if (!Camunda8Errors.jobAlreadyGone(e)) {
        throw e;
      }
      log.warn(
          "Camunda8[{}]: user task '{}' is gone - skipping the redelivered phase-two completion",
          adapterId,
          request.taskId());
    }

  }

  private void preflightCancelUserTask(
      final PhaseOneRequest<A> request) {

    // fail EARLY inside the caller's transaction: see cancelUserTaskPhaseTwo
    throw newCancelUserTaskUnsupported(request.taskId(), request.bpmnProcessId());

  }

  private void cancelUserTask(
      final PhaseTwoRequest<A> request) {

    throw newCancelUserTaskUnsupported(request.taskId(), request.bpmnProcessId());

  }

  private UnsupportedOperationException newCancelUserTaskUnsupported(
      final String taskId,
      final String bpmnProcessId) {

    // No Camunda 8 cluster up to 8.9 offers a command to cancel a Camunda-managed user
    // task by BPMN error: ThrowError is job-based (a zeebe:userTask has no job), and the
    // V1 workaround (completing the task with a marker variable evaluated by a listener)
    // is marked "currently not working" in the V1 adapter itself. The task/execution
    // listeners of 8.10 are what this needs, so it can only ever arrive on a line built
    // against 8.10. The message names the line
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

  private void completeTask(
      final PhaseTwoRequest<A> request) {

    try {
      clientFactory
          .getClient()
          .newCompleteCommand(taskKeyOf(request.taskId()))
          // The aggregate changed before the task was completed - the
          // cluster only sees what VanillaBP pushes
          .variables(variablesOf(request.aggregatePersistence(), request.workflowAggregateId()))
          .send()
          .join();
      log.info(
          "Camunda8[{}]: completed task '{}' of BPMN process '{}' of workflow module '{}'",
          adapterId,
          request.taskId(),
          request.bpmnProcessId(),
          request.workflowModuleId());
    } catch (final Exception e) {
      if (!Camunda8Errors.jobAlreadyGone(e)) {
        throw e;
      }
      // stale outbox entry: the job disappeared between the dispatch-time probe
      // and this command - the at-least-once residual, the entry is consumed
      log.warn(
          "Camunda8[{}]: task '{}' is gone - skipping the redelivered phase-two completion",
          adapterId,
          request.taskId());
    }

  }

  private void cancelTask(
      final PhaseTwoRequest<A> request) {

    try {
      clientFactory
          .getClient()
          .newThrowErrorCommand(taskKeyOf(request.taskId()))
          // the model's error codes are prefixed too
          .errorCode(scopedIdentifier(request.workflowModuleId(), request.bpmnErrorCode()))
          .errorMessage("canceled via ProcessService#cancelTask")
          // The error boundary's outgoing path may branch on the
          // aggregate, which the caller changed before canceling the task
          .variables(variablesOf(request.aggregatePersistence(), request.workflowAggregateId()))
          .send()
          .join();
      log.info(
          "Camunda8[{}]: canceled task '{}' (error code '{}') of BPMN process '{}' of workflow module '{}'",
          adapterId,
          request.taskId(),
          request.bpmnErrorCode(),
          request.bpmnProcessId(),
          request.workflowModuleId());
    } catch (final Exception e) {
      if (!Camunda8Errors.jobAlreadyGone(e)) {
        throw e;
      }
      log.warn(
          "Camunda8[{}]: task '{}' is gone - skipping the redelivered phase-two cancellation",
          adapterId,
          request.taskId());
    }

  }

  /**
   * Logged once per adapter: probing workflow awareness needs the query API
   * (secondary storage) - without it the adapter answers OPTIMISTICALLY.
   */
  private final java.util.concurrent.atomic.AtomicBoolean noSecondaryStorageWarned = new java.util.concurrent.atomic.AtomicBoolean();

  /**
   * What the cluster answered when it was asked whether it can be searched at all -
   * <code>null</code> until somebody asked. The answer cannot change while the
   * application runs: secondary storage is part of how the cluster was started.
   */
  private volatile Boolean queryApiAvailable;

  /**
   * Whether this cluster can be ASKED which workflows it holds.
   * <p>
   * Finding a workflow means searching the query API, which a cluster without secondary
   * storage does not have - {@link #awarenessOfWorkflow} then answers optimistically,
   * which is right while this is the only BPMS and a guess as soon as it is not. Saying
   * so here is what lets the core refuse that combination while it boots.
   * <p>
   * The core asks after this adapter deployed, so one search settles it: an empty result
   * is an answer like any other, and only the "no secondary storage" failure means the
   * cluster cannot be asked. A cluster which is unreachable in that moment is not
   * declared incapable - it is asked again the next time.
   *
   * @return Whether the query API answers
   */
  @Override
  public boolean canLocateWorkflows() {

    if (queryApiAvailable != null) {
      return queryApiAvailable;
    }
    try {
      // the same shape the real probe uses - a search FILTERED BY A VARIABLE is what
      // needs the secondary storage, while a bare search is answered by the broker
      // itself. The variable name matches nothing on purpose: an empty result is an
      // answer, and only the missing query API is not
      clientFactory
          .getClient()
          .newProcessInstanceSearchRequest()
          .filter(filter -> filter
              .variables(java.util.Map
                  .of("vanillabp-query-api-probe", Camunda8VariableFilters.aggregateIdSearchValue("none"))))
          .page(page -> page.limit(1))
          .send()
          .join();
      queryApiAvailable = Boolean.TRUE;
    } catch (final Exception e) {
      if (!isSecondaryStorageMissing(e)) {
        // the cluster could not be reached, which says nothing about its capabilities
        log.debug(
            "Camunda8[{}]: could not find out whether the query API answers - assuming it does",
            adapterId,
            e);
        return true;
      }
      queryApiAvailable = Boolean.FALSE;
    }
    return queryApiAvailable;

  }

  @Override
  public WorkflowAwareness awarenessOfWorkflow(
      final io.vanillabp.integration.adapter.spi.WorkflowScope scope,
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
      // On a cluster shared with another adapter id the variable alone finds
      // the other deployment's instance too - only what THIS adapter deployed counts
      final var mine = found
          .items()
          .stream()
          .filter(instance -> isInScope(scope, instance.getTenantId(), instance.getProcessDefinitionId()))
          .toList();
      if (mine.isEmpty()) {
        return WorkflowAwareness.UNKNOWN_TO_BPMS;
      }
      return mine
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
   * The START re-dispatch mitigation probe - STRICTER contract than
   * {@link #awarenessOfWorkflow}: the answer must NEVER be optimistic
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
      final io.vanillabp.integration.adapter.spi.WorkflowScope scope,
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
      return found
          .items()
          .stream()
          // as in awarenessOfWorkflow: an instance of the other adapter id on
          // this cluster does not prove that THIS one started the workflow
          .noneMatch(instance -> isInScope(scope, instance.getTenantId(), instance.getProcessDefinitionId()))
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
   * Whether the given process instance, job or user task belongs to the scope the probe
   * was asked about.
   * <p>
   * The scope is the workflow module and the BPMN processes of the CALL, translated into
   * what the cluster knows them by: the tenant of that module and the SCOPED process
   * definition ids. Two facts make the comparison necessary, and neither alone would be
   * enough. Two <code>camunda8</code> adapter ids may address ONE cluster, which is the
   * supported setup migrating a workflow module from tenants to prefixed identifiers, and
   * there every key is global. And one adapter id serves several workflow modules, whose
   * aggregate ids are unique per aggregate type rather than across an application, so
   * "one of mine" is not the same question as "the one you asked about".
   *
   * @param scope What the probe was asked about
   * @param tenantId The tenant the cluster reports, possibly {@code <default>}
   * @param processDefinitionId The process definition id the cluster reports, which is
   *          the SCOPED one wherever a prefix is used
   * @return Whether it belongs to the scope of the call
   */
  private boolean isInScope(
      final io.vanillabp.integration.adapter.spi.WorkflowScope scope,
      final String tenantId,
      final String processDefinitionId) {

    return scopeKeysOf(scope).contains(scopeKey(tenantId, processDefinitionId));

  }

  /**
   * @param scope What the probe was asked about
   * @return The (tenant, scoped process definition id) pairs that scope stands for
   */
  private java.util.Set<String> scopeKeysOf(
      final io.vanillabp.integration.adapter.spi.WorkflowScope scope) {

    final var tenantId = tenantIdOf(scope.workflowModuleId());
    return scope
        .bpmnProcessIds()
        .stream()
        .map(bpmnProcessId -> scopeKey(
            tenantId,
            scopedProcessId(scope.workflowModuleId(), bpmnProcessId)))
        .collect(java.util.stream.Collectors.toSet());

  }

  /**
   * Whether the task behind the given key belongs to the scope the probe was asked about
   * - asked only where another <code>camunda8</code> adapter id
   * addresses the same cluster, because a key is unique per cluster and the two ids would
   * otherwise answer for each other's tasks.
   * <p>
   * The answer needs the query API, which is why an application configuring two ids on
   * one cluster without secondary storage does not boot (see
   * {@code Camunda8DeploymentService}). A task the query API does not know is left to the
   * probe itself: it is either gone or not exported yet, and both are answered by the
   * command which follows.
   * <p>
   * <b>Why not always.</b> Where one adapter id owns the cluster, the read would buy very
   * little for a query-API round trip on every task election: the key of another workflow
   * module of the same application still addresses the task the operation then acts on,
   * because completing or cancelling goes by that key, and a key of ANOTHER BPMS is not a
   * Camunda 8 key at all. The workflow probes, whose answer routes a message or a pushed
   * aggregate, compare the scope always - there it is free.
   *
   * @param scope What the probe was asked about
   * @param taskId The task id, which is the job respectively user-task key
   * @param userTask Whether it is a user task
   * @return Whether the probe may claim the task
   */
  private boolean belongsToThisAdapter(
      final io.vanillabp.integration.adapter.spi.WorkflowScope scope,
      final String taskId,
      final boolean userTask) {

    if (!clientFactory.sharesItsCluster()) {
      return true;
    }
    try {
      if (userTask) {
        final var task = clientFactory
            .getClient()
            .newUserTaskGetRequest(taskKeyOf(taskId))
            .send()
            .join();
        return isInScope(scope, task.getTenantId(), task.getBpmnProcessId());
      }
      final var job = jobOf(taskId);
      return (job == null) || isInScope(scope, job.getTenantId(), job.getProcessDefinitionId());
    } catch (final Exception e) {
      if (Camunda8Errors.jobAlreadyGone(e)) {
        // not exported yet or gone - the probe's own command answers that
        return true;
      }
      log.debug(
          "Camunda8[{}]: could not read the scope of task '{}' - probing it as if it were this "
              + "adapter's",
          adapterId,
          taskId,
          e);
      return true;
    }

  }

  /**
   * The comparable form of one scope. The cluster reports {@code <default>} for an
   * untenanted instance while the adapter has no tenant configured at all, so both are
   * folded into the same value.
   */
  private static String scopeKey(
      final String tenantId,
      final String processDefinitionId) {

    final var tenant = (tenantId == null) || tenantId.isBlank() || DEFAULT_TENANT.equals(tenantId)
        ? DEFAULT_TENANT
        : tenantId;
    return tenant
        + "|"
        + processDefinitionId;

  }

  /**
   * What Camunda 8 calls the tenant of everything which has none.
   */
  private static final String DEFAULT_TENANT = "<default>";

  /**
   * The name of the process variable holding the workflow-aggregate ID: Camunda 8
   * has no business key, so every lookup of a workflow filters by that variable.
   * <p>
   * It is ALWAYS derived from the aggregate persistence of the call at hand, never
   * remembered between calls. One process service serves every workflow module and
   * aggregate of its adapter id, so a remembered name would be the one of whichever
   * aggregate was handled last, which was a defect of this adapter once: the
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

    // The outbox repeats what a second attempt may fix - a cluster which
    // is busy, unreachable or lost a conflict. A command the cluster REJECTS looks
    // the same on every attempt, and so does a task key which is not a number. The
    // list of those cases lives in Camunda8Errors, next to the job-gone rule
    return !Camunda8Errors.permanentFailure(failure);

  }

  /**
   * Phase one of a message correlation asks the MODEL, not the cluster.
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
   * see {@code Camunda8DeployedProcesses}), because then the declared names are
   * unknown rather than absent.
   */
  private void preflightCorrelateMessage(
      final PhaseOneRequest<A> request) {

    clientFactory.validateConfigured();
    validateMessageIsDeclared(request.workflowModuleId(), request.messageName());

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
    // the models carry the SCOPED names - messages are renamed while deploying - so
    // the name of the call is scoped the same way the publication scopes it
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

  private void correlateMessage(
      final PhaseTwoRequest<A> request) {

    // correlationKey: the correlation id if given, the aggregate ID otherwise
    // (V1 semantics; the wired zeebe:subscription evaluates '=<idName>' - a
    // correlation id requires a model-side subscription on the matching variable).
    // messageId WHERE A CORRELATION ID EXISTS: the engine then deduplicates a second
    // publication of the same message for as long as the message time-to-live lasts,
    // which is a net of its own and a shorter one than VanillaBP's outbox - that one
    // ends with the dispatch, while this one runs for the TTL (the client's default
    // hour unless 'message-time-to-live' says otherwise). A repetition inside the TTL
    // is therefore swallowed by the ENGINE, whatever VanillaBP does, so a repeating
    // scope has to vary the correlation id. Without a correlation id there is no
    // messageId either, and an at-least-once redelivery may double-correlate
    // (documented).
    // PAYLOAD DOCTRINE: no message CONTENT travels - what does travel is the
    // aggregate state shared with the BPMS, because the cluster can
    // only evaluate BPMN expressions against variables it was given.
    final var correlationKey = request.correlationId() != null
        ? request.correlationId()
        : String.valueOf(request.workflowAggregateId());
    var command = clientFactory
        .getClient()
        .newPublishMessageCommand()
        .messageName(scopedIdentifier(request.workflowModuleId(), request.messageName()))
        .correlationKey(correlationKey)
        .variables(variablesOf(request.aggregatePersistence(), request.workflowAggregateId()));
    final var correlationTenantId = tenantIdOf(request.workflowModuleId());
    if (correlationTenantId != null) {
      command = command.tenantId(correlationTenantId);
    }
    if (request.correlationId() != null) {
      // The ACTIVATION belongs in here for the same reason it belongs in VanillaBP's own
      // idempotency key: three elements of a multi-instance call activity agree in every
      // other part, because a called process is a secondary workflow of the SAME
      // aggregate. Without it they are three operations for the outbox and ONE message
      // for the cluster, and the two which lose are lost silently. Absent where the
      // correlation was planned outside any activation, which keeps the id a REST
      // endpoint produces exactly what it was
      command = command
          .messageId(
              messageIdOf(request.workflowModuleId(), request.bpmnProcessId(), request.workflowAggregateId(),
                  request.messageName(), request.correlationId(),
                  request.activationId()));
    }
    final var timeToLive = messageTimeToLiveFor(request.workflowModuleId(), request.bpmnProcessId(),
        request.messageName());
    if (timeToLive != null) {
      // per message, because the number buffers AND deduplicates and those two want it
      // to go in opposite directions. Nothing configured means nothing set: the client's
      // own default then applies, as it always did
      command = command.timeToLive(timeToLive);
    }
    try {
      command
          .send()
          .join();
      log.info(
          "Camunda8[{}]: published message '{}' (correlation key '{}') for BPMN process '{}' of "
              + "workflow module '{}'",
          adapterId,
          request.messageName(),
          correlationKey,
          request.bpmnProcessId(),
          request.workflowModuleId());
    } catch (final Exception e) {
      if (!isMessageAlreadyPublished(e)) {
        throw e;
      }
      // the engine deduplicated by messageId. Which of the two it was cannot be told
      // from here, and the entry counts as consumed either way - repeating the publish
      // would be refused again
      log.warn(
          """
              Camunda8[{}]: the cluster refused message '{}' (correlation key '{}') for BPMN process \
              '{}' of workflow module '{}' because a message of the same id was published before, \
              within the message time-to-live. Either this dispatch is a repetition of one which \
              reached the cluster already - then nothing is lost - or it is a second, legitimate \
              correlation of the same message name and correlation id for this aggregate, and the \
              workflow will never see it. The entry counts as done in both cases. This net is the \
              cluster's own and lasts for the message time-to-live \
              ('vanillabp.adapters.<id>.message-time-to-live', resolvable down to the single \
              message); a scope which repeats within it has to vary the correlation id, unless the \
              repetitions are separate activations of a BPMN element, which the message id already \
              tells apart.""",
          adapterId,
          request.messageName(),
          correlationKey,
          request.bpmnProcessId(),
          request.workflowModuleId());
    }

  }

  /**
   * The id this adapter hands the cluster for a correlated message, which is what the
   * cluster deduplicates by for as long as the message lives.
   *
   * <h2>Why it looks like VanillaBP's own key and is not the same thing</h2>
   *
   * Both are derived from the same values, and they guard different windows: VanillaBP's
   * key deduplicates the entries which have not been dispatched yet, this one deduplicates
   * publications inside the message time-to-live. An operation can pass the first net and
   * be dropped by the second, which is why the adapter says so when the cluster refuses a
   * publication.
   *
   * <h2>Why the activation is part of it</h2>
   *
   * A called process is a secondary workflow of the SAME aggregate, so the three elements
   * of a multi-instance call activity agree in module, process, aggregate, message name
   * and - where it comes from business data - correlation id. Without the activation they
   * are three operations for the outbox and ONE message for the cluster.
   *
   * @param workflowModuleId The workflow module
   * @param bpmnProcessId The BPMN process
   * @param workflowAggregateId The workflow aggregate's id
   * @param messageName The message name as the application wrote it
   * @param correlationId The correlation id (never <code>null</code> here - without one
   *          nothing is deduplicated and no id is sent)
   * @param activationId The activation the correlation was planned in, or
   *          <code>null</code> where it was planned outside any
   * @return The message id
   */
  static String messageIdOf(
      final String workflowModuleId,
      final String bpmnProcessId,
      final Object workflowAggregateId,
      final String messageName,
      final String correlationId,
      final String activationId) {

    final var withoutActivation = "%s|%s|%s|%s|%s"
        .formatted(workflowModuleId, bpmnProcessId, workflowAggregateId, messageName, correlationId);
    return activationId == null
        ? withoutActivation
        : "%s|%s".formatted(withoutActivation, activationId);

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

  private void preflightStartByMessage(
      final PhaseOneRequest<A> request) {

    clientFactory.validateConfigured();

  }

  /**
   * A remote BPMS must not act before the caller's transaction committed: phase one
   * does nothing, the broadcast happens in phase two through the outbox.
   */
  private void preflightSendSignal(
      final PhaseOneRequest<A> request) {

  }

  private void sendSignal(
      final PhaseTwoRequest<A> request) {

    // no variables travel with a signal, and there is nothing to deduplicate by:
    // unlike a message, a broadcast carries no correlation key the cluster could
    // recognize a redelivery from (documented at-least-once residual)
    var command = clientFactory
        .getClient()
        .newBroadcastSignalCommand()
        .signalName(scopedIdentifier(request.workflowModuleId(), request.signalName()));
    final var signalTenantId = tenantIdOf(request.workflowModuleId());
    if (signalTenantId != null) {
      command = command.tenantId(signalTenantId);
    }
    command
        .send()
        .join();
    log.info(
        "Camunda8[{}]: broadcast signal '{}' of workflow module '{}'",
        adapterId,
        request.signalName(),
        request.workflowModuleId());

  }

  private void preflightAggregateChanged(
      final PhaseOneRequest<A> request) {

    // a remote BPMS: writing here would show the cluster values of a transaction
    // which may still roll back - the push happens in phase two

  }

  private void pushChangedAggregate(
      final PhaseTwoRequest<A> request) {

    final var variables = variablesOf(request.aggregatePersistence(), request.workflowAggregateId());

    if (request.taskId() == null) {
      final var processInstanceKey = processInstanceKeyOf(
          io.vanillabp.integration.adapter.spi.WorkflowScope.of(request.workflowModuleId(), request.bpmnProcessId()),
          request.aggregatePersistence(),
          request.workflowAggregateId());
      if (processInstanceKey == null) {
        // at-least-once residual: the workflow ended between the dispatch-time
        // election and now - there is nothing left to write to
        log.warn(
            "Camunda8[{}]: no active workflow found for aggregate '{}' - skipping the push of the "
                + "changed aggregate",
            adapterId,
            request.workflowAggregateId());
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
          request.workflowAggregateId(),
          processInstanceKey);
      return;
    }

    final var elementInstanceKey = flowScopeKeyOf(request.taskId());
    if (elementInstanceKey == null) {
      log.warn(
          "Camunda8[{}]: the scope of task '{}' of aggregate '{}' was not found within {} - skipping "
              + "the push of the changed aggregate. Either the task was completed meanwhile, or the "
              + "query API did not catch up with it: raise "
              + "'vanillabp.adapters.{}.workflow-visibility-timeout' if this cluster's exporter "
              + "regularly needs longer. The workflow's own scope is deliberately NOT written "
              + "instead - it is read by every branch, and the task asked for its own scope",
          adapterId,
          request.taskId(),
          request.workflowAggregateId(),
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
        request.workflowAggregateId(),
        elementInstanceKey,
        request.taskId());

  }

  /**
   * The key of the ACTIVE process instance carrying the aggregate's ID variable -
   * Camunda 8 has no business key, so the eventually-consistent query API answers
   * (like {@link #awarenessOfWorkflow}).
   *
   * @param workflowAggregateId The aggregate's ID
   * @return The process instance key or <code>null</code> if none is active
   */
  private Long processInstanceKeyOf(
      final io.vanillabp.integration.adapter.spi.WorkflowScope scope,
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
      // Writing into the instance of ANOTHER adapter id of this cluster would
      // put the values of one migration half into the other one
      return found
          .items()
          .stream()
          .filter(instance -> isInScope(scope, instance.getTenantId(), instance.getProcessDefinitionId()))
          .findFirst()
          .map(instance -> instance.getProcessInstanceKey())
          .orElse(null);
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
   * {@link #workflowVisibilityDelay()} allows. Repeating is allowed HERE because this
   * runs in phase two, on the outbox dispatcher's thread: no application transaction is
   * open, so the waiting costs the entry an attempt rather than a database connection
   * (decision 27 of the platform's DECISIONS.md, which draws that line for the core's
   * election as well). A scope which stays unknown yields
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
          .filter(filter -> filter.jobKey(taskKeyOf(taskId)))
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

  private void startWorkflowByMessage(
      final PhaseTwoRequest<A> request) {

    // message START events ignore the correlation key; the aggregate-ID variable
    // is the ONLY variable published (the same technical field a regular start
    // sets - not message content). messageId is derived from the same values as the
    // start's idempotency key, so the engine deduplicates a redelivered dispatch within
    // the message TTL - a net of the cluster's, alongside the outbox' own.
    try {
      var startCommand = clientFactory
          .getClient()
          .newPublishMessageCommand()
          .messageName(scopedIdentifier(request.workflowModuleId(), request.messageName()))
          .correlationKey("")
          .messageId(
              "%s|%s|%s".formatted(request.workflowModuleId(), request.bpmnProcessId(), request.workflowAggregateId()))
          .variables(variablesOf(request.aggregatePersistence(), request.workflowAggregateId()));
      final var startTenantId = tenantIdOf(request.workflowModuleId());
      if (startTenantId != null) {
        startCommand = startCommand.tenantId(startTenantId);
      }
      // The ADAPTER level only, deliberately: this message starts a workflow, so its
      // deduplication is wanted for as long as possible and the subscription of a message
      // START event exists as long as the process is deployed - the buffering half of the
      // number does not apply here at all. A per-message override meant for a repeating
      // catch event must not shorten the protection against a double-started workflow
      final var startTimeToLive = clientFactory.getConfiguration().getMessageTimeToLive();
      if (startTimeToLive != null) {
        startCommand = startCommand.timeToLive(startTimeToLive);
      }
      startCommand
          .send()
          .join();
      log.info(
          "Camunda8[{}]: published start message '{}' for BPMN process '{}' of workflow module "
              + "'{}' (aggregate '{}')",
          adapterId,
          request.messageName(),
          request.bpmnProcessId(),
          request.workflowModuleId(),
          request.workflowAggregateId());
    } catch (final Exception e) {
      if (!isMessageAlreadyPublished(e)) {
        throw e;
      }
      // a start is different from a correlation: a workflow is started at most once per
      // aggregate anyway, so a refused start message says the start happened and this
      // stays an INFO rather than a warning about something possibly lost
      log.info(
          "Camunda8[{}]: the cluster refused the start message '{}' for aggregate '{}' because a "
              + "message of the same id was published before, within the message time-to-live - the "
              + "workflow was started already and the entry counts as done",
          adapterId,
          request.messageName(),
          request.workflowAggregateId());
    }

  }

  private void preflightStart(
      final PhaseOneRequest<A> request) {

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
        + "(adapter '{}')", request.bpmnProcessId(), request.workflowModuleId(), adapterId);

  }

  private void startWorkflow(
      final PhaseTwoRequest<A> request) {

    createProcessInstance(
        scopedProcessId(request.workflowModuleId(), request.bpmnProcessId()),
        variablesOf(request.aggregatePersistence(), request.workflowAggregateId()),
        request.workflowAggregateId(),
        tenantIdOf(request.workflowModuleId()));

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
   * possible). That residual is accepted rather than pending: a workflow is located by
   * asking, not by a persistent registry (decision 25 of the platform's DECISIONS.md),
   * and what narrows the window is the core probing
   * {@code awarenessOfWorkflowForRedispatch} before it dispatches a start again. What is
   * left of it is documented in this repository's README under "Idempotency limitation".
   * No Camunda-8-side workaround is attempted here.
   *
   * @param bpmnProcessId The BPMN process ID of the workflow to start
   * @param variables The variables the instance is created with
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
   * decides: the workflow module id under {@code by-adapter}, none under
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
   * The viewer/history API - see {@link Camunda8WorkflowViewer} for the
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

  /**
   * A VanillaBP task id turned into the key Camunda 8 commands expect - the job's key
   * for a service task, the user task's key for a user task. Always a decimal number,
   * which is why the failure is worth a message of its own.
   *
   * <h2>Why this exists</h2>
   *
   * VanillaBP 1 could hand out the same key in HEXADECIMAL
   * (<code>task-id-as-hex-string</code>, off by default), and an application which
   * switched it on stored those ids in its own data - a user task it is holding, a
   * service task waiting for its <code>completeTask</code>. Those ids outlive the
   * upgrade. Version 2 has no such setting and parses decimally everywhere, so what the
   * developer got was a bare NumberFormatException naming neither the setting which
   * produced the number nor the fact that the operation is not retried: a task key which
   * is not a number is a PERMANENT phase-two failure, so the outbox entry is blocked
   * after a single attempt.
   * <p>
   * The classification stays exactly as it was - the NumberFormatException travels as the
   * cause, which is what {@code Camunda8Errors.permanentFailure} walks the chain for.
   * Version 2 deliberately does NOT accept hexadecimal ids again: one representation of a
   * task id is simpler than two, and an application which has them in its data has a
   * migration of its own, which the message points at.
   *
   * <p>
   * Package-private so the message can be asserted without a client.
   *
   * @param taskId The task id as the application knows it
   * @return The key
   */
  static long taskKeyOf(
      final String taskId) {

    try {
      return Long.parseLong(taskId);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException(
          ("The task id '%s' is not a Camunda 8 task key! A task key is the decimal key of a job "
              + "respectively of a user task, and this operation is NOT retried, because the cluster "
              + "would refuse it the same way every time.%s")
              .formatted(taskId, looksHexadecimal(taskId)
                  ? " It does read like a HEXADECIMAL number, which is how VanillaBP 1 handed "
                      + "task ids out where 'task-id-as-hex-string' was switched on. Version 2 has no such "
                      + "setting and there is no configuration which makes it read them: the ids your "
                      + "application stored have to be converted to decimal."
                  : ""), e);
    }

  }

  /**
   * Whether a task id reads like one of version 1's hexadecimal ids: not a decimal
   * number, but a valid hexadecimal one. A hint rather than a claim, which is how the
   * message states it.
   *
   * @param taskId The task id which failed to parse
   * @return <code>true</code> where hexadecimal would have worked
   */
  private static boolean looksHexadecimal(
      final String taskId) {

    if (taskId == null) {
      return false;
    }
    try {
      Long.parseLong(taskId, 16);
      return true;
    } catch (final NumberFormatException e) {
      return false;
    }

  }

}
