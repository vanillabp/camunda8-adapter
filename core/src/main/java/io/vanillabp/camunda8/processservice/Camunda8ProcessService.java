package io.vanillabp.camunda8.processservice;

import io.vanillabp.integration.adapter.spi.MigratableProcessService;
import io.vanillabp.integration.adapter.spi.WorkflowAwareness;
import io.vanillabp.integration.spi.AggregatePersistenceAware;
import lombok.RequiredArgsConstructor;

/**
 * Camunda 8 implementation of the {@link MigratableProcessService}. One instance is
 * created per configured adapter ID (not per adapter type).
 * <p>
 * Camunda 8 is a <b>remote</b>, eventually consistent BPMS: the engine cannot join the
 * application's local database transaction, therefore
 * {@link #needsTwoPhaseCommitForStartingWorkflows()} returns {@code true} - starting a
 * workflow is routed through the core {@code PhaseTwoOutbox} (phase one only validates,
 * the actual {@code CreateProcessInstance} happens in phase two).
 * <p>
 * <b>Skeleton stage:</b> {@link #getAdapterId()} and
 * {@link #needsTwoPhaseCommitForStartingWorkflows()} are implemented; every other
 * method throws {@link UnsupportedOperationException} - implemented in later stories.
 * No {@code CamundaClient} is created here (client construction is a later story), so
 * applications boot without a reachable cluster.
 *
 * @param <A> The workflow-aggregate type
 */
@RequiredArgsConstructor
public class Camunda8ProcessService<A> implements MigratableProcessService<A> {

  private final String adapterId;

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

    throw new UnsupportedOperationException("startWorkflowPhaseOne is implemented in a later story");

  }

  @Override
  public void startWorkflowPhaseTwo(
      final Object workflowAggregateId) {

    throw new UnsupportedOperationException("startWorkflowPhaseTwo is implemented in a later story");

  }

}
