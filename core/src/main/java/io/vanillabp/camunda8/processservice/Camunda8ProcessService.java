package io.vanillabp.camunda8.processservice;

import io.camunda.client.api.response.ProcessInstanceEvent;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
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
 *   <li>{@link #startWorkflowPhaseOne} runs inside the caller's transaction and only
 *       <i>validates</i> (resolve the aggregate ID, verify the client is configured); it
 *       must not contact the cluster, otherwise a rolled-back transaction would leave a
 *       ghost workflow instance behind.</li>
 *   <li>{@link #startWorkflowPhaseTwo} runs after the commit and creates the process
 *       instance via {@link #createProcessInstance(String, Object)}.</li>
 * </ul>
 *
 * @param <A> The workflow-aggregate type
 */
@Slf4j
@RequiredArgsConstructor
public class Camunda8ProcessService<A> implements MigratableProcessService<A> {

  /**
   * Name of the single process variable carrying the workflow aggregate's ID. No other
   * process variables are set (aggregate attribute sync is the {@code @SyncWithBPMS}
   * story).
   */
  public static final String AGGREGATE_ID_VARIABLE = "aggregateId";

  private final String adapterId;

  private final Camunda8ClientFactory clientFactory;

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

    throw new UnsupportedOperationException("awarenessOfTask is implemented in a later story");

  }

  @Override
  public WorkflowAwareness awarenessOfWorkflow(
      final Object workflowAggregateId) {

    throw new UnsupportedOperationException("awarenessOfWorkflow is implemented in a later story");

  }

  @Override
  public void startWorkflowPhaseOne(
      final AggregatePersistenceAware<A> aggregatePersistence,
      final A workflowAggregate) {

    // resolve the aggregate ID - fail fast so the caller's transaction rolls back
    // before a phase-two outbox entry is scheduled
    final var aggregateId = aggregatePersistence.getAggregateId(workflowAggregate);
    if (aggregateId == null) {
      throw new IllegalStateException(
          "Cannot start a Camunda 8 workflow (adapter '%s'): the workflow aggregate's ID is null "
              + "after persisting. A workflow aggregate must have a non-null ID.".formatted(adapterId));
    }

    // verify the adapter instance is configured, but DO NOT contact the cluster:
    // phase one runs inside the caller's DB transaction
    clientFactory.validateConfigured();

    log.debug("Validated phase one of starting a Camunda 8 workflow for aggregate '{}' (adapter '{}')",
        aggregateId, adapterId);

  }

  @Override
  public void startWorkflowPhaseTwo(
      final Object workflowAggregateId) {

    // PLATFORM-INTEGRATION GAP: creating a Camunda 8 process instance requires the BPMN
    // process ID (newCreateInstanceCommand().bpmnProcessId(...)), but the adapter SPI
    // method MigratableProcessService#startWorkflowPhaseTwo(Object) does not provide it.
    // The core MigrationProcessService knows workflowModuleId and bpmnProcessId (they are
    // its fields) but drops them when delegating to the adapter. The real creation logic
    // is ready in #createProcessInstance(String, Object) and is exercised against a real
    // Camunda 8 cluster in the deployment integration test; it only needs the process ID
    // to be threaded through the SPI.
    throw new IllegalStateException(
        ("Camunda 8 cannot start the workflow of aggregate '%s' (adapter '%s'): the adapter SPI "
            + "method MigratableProcessService.startWorkflowPhaseTwo(Object) does not supply the BPMN "
            + "process ID needed to create a process instance. This is a platform-integration gap - "
            + "MigrationProcessService must pass its workflowModuleId and bpmnProcessId to the adapter. "
            + "The Camunda 8 creation logic itself is implemented in "
            + "Camunda8ProcessService.createProcessInstance(String, Object).")
            .formatted(workflowAggregateId, adapterId));

  }

  /**
   * Creates a Camunda 8 process instance of the latest version of the given BPMN process,
   * passing the workflow aggregate's ID as the single {@value #AGGREGATE_ID_VARIABLE}
   * process variable. This is the actual work of {@link #startWorkflowPhaseTwo(Object)}
   * once the BPMN process ID can be supplied (see the note there).
   * <p>
   * <b>Idempotency limitation:</b> a crash between a successful create and the removal of
   * the phase-two outbox entry can create the instance twice (at-least-once, duplicates
   * possible). Strict deduplication needs the core-side {@code WorkflowInstanceRegistry}
   * (separate story) - see the repository-root {@code README.md}. No Camunda-8-side
   * workaround is attempted here.
   *
   * @param bpmnProcessId The BPMN process ID of the workflow to start
   * @param workflowAggregateId The workflow aggregate's ID (sent as a string variable)
   * @return The created process-instance event
   */
  public ProcessInstanceEvent createProcessInstance(
      final String bpmnProcessId,
      final Object workflowAggregateId) {

    final var client = clientFactory.getClient();
    var command = client
        .newCreateInstanceCommand()
        .bpmnProcessId(bpmnProcessId)
        .latestVersion()
        .variable(
            AGGREGATE_ID_VARIABLE,
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
