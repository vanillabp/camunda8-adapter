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
