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

  /**
   * The core's sync model (story 28): which aggregate attributes are shared with
   * the cluster. Camunda 8 is REMOTE, so its default is
   * {@link AggregateSyncMode#FULL} - a BPMN expression can only see what VanillaBP
   * pushed as a process variable. May be <code>null</code> (tests): only the
   * technical aggregate-ID variable is written then.
   */
  private final io.vanillabp.integration.adapter.spi.WorkflowAggregateSync aggregateSync;

  /**
   * The default of this adapter: everything is shared unless the application
   * excludes it ({@code @NoSyncWithBPMS}).
   */
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
      final Object workflowAggregateId) {

    // Zeebe offers NO engine command answering "does an instance for this
    // aggregate exist" - only the eventually-consistent query API (requires
    // secondary storage, standard in any real Camunda 8 setup). The search
    // filters by the aggregate-ID process variable.
    try {
      final var found = clientFactory
          .getClient()
          .newProcessInstanceSearchRequest()
          .filter(filter -> filter
              .state(io.camunda.client.api.search.enums.ProcessInstanceState.ACTIVE)
              .variables(java.util.Map
                  .of(aggregateIdVariableName(), String.valueOf(workflowAggregateId))))
          .send()
          .join();
      return found.items().isEmpty()
          ? WorkflowAwareness.UNKNOWN_TO_BPMS
          : WorkflowAwareness.ACTIVE;
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
   * {@link #awarenessOfWorkflow(Object)}: the answer must NEVER be optimistic
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
      final Object workflowAggregateId) {

    try {
      final var found = clientFactory
          .getClient()
          .newProcessInstanceSearchRequest()
          .filter(filter -> filter
              .variables(java.util.Map
                  .of(aggregateIdVariableName(), String.valueOf(workflowAggregateId))))
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
   * The name of the process variable holding the workflow-aggregate ID. Kept as a
   * field set by the deployment wiring would be nicer, but the process service is
   * decoupled from deployment - the name is resolved per call via the aggregate
   * persistence passed into the SPI methods where available; awareness probes
   * fall back to the value set by {@link #rememberAggregateIdName(String)}.
   */
  private volatile String aggregateIdVariableName = "id";

  private String aggregateIdVariableName() {

    return aggregateIdVariableName;

  }

  /**
   * Remembers the aggregate-ID variable name for awareness probes (called from
   * the SPI methods which receive the persistence).
   *
   * @param name The variable name
   */
  private void rememberAggregateIdName(
      final String name) {

    if (name != null) {
      this.aggregateIdVariableName = name;
    }

  }

  @Override
  public void correlateMessagePhaseOne(
      final String workflowModuleId,
      final String bpmnProcessId,
      final AggregatePersistenceAware<A> aggregatePersistence,
      final A workflowAggregate,
      final String messageName,
      final String correlationId) {

    // no cheap NON-ADVANCING existence check exists for a waiting message
    // subscription (the query API is eventually consistent) - like workflow
    // starts, phase one only validates the configuration; an unreachable cluster
    // just makes the phase-two publish wait in the outbox
    rememberAggregateIdName(aggregatePersistence.getAggregateIdName());
    clientFactory.validateConfigured();

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

    rememberAggregateIdName(aggregatePersistence.getAggregateIdName());
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

    rememberAggregateIdName(aggregatePersistence.getAggregateIdName());
    return viewer().getProcessDefinitions(
        workflowModuleId, bpmnProcessId, aggregateIdVariableName(), workflowAggregateId, historyContext);

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

    rememberAggregateIdName(aggregatePersistence.getAggregateIdName());
    return viewer().getWorkflowHistory(
        workflowModuleId, bpmnProcessId, aggregateIdVariableName(), workflowAggregateId, historyContext);

  }

}
