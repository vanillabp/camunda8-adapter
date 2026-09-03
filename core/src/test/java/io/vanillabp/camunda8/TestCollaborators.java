package io.vanillabp.camunda8;

import static org.mockito.Mockito.mock;

import io.vanillabp.integration.adapter.spi.AdapterCollaborators;
import io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport;
import io.vanillabp.integration.adapter.spi.PreCommitRegistrar;
import io.vanillabp.integration.adapter.spi.WorkflowAggregateSync;
import io.vanillabp.integration.adapter.spi.workflowend.WorkflowEndedInvoker;
import io.vanillabp.integration.adapter.spi.workflowstart.BpmsInitiatedStartInvoker;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskInvoker;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskWiring;

/**
 * What the platform hands the adapter, for tests which need the adapter and not the
 * registration. The core standing in for both halves of the task SPI is given per test;
 * the rest are mocks nobody calls unless the test says so.
 */
public final class TestCollaborators {

  private TestCollaborators() {
    // static helper
  }

  /**
   * @param <T> A double playing both halves of the task SPI
   * @param core The double
   * @return A complete set built around it
   */
  public static <T extends WorkflowTaskWiring & WorkflowTaskInvoker> AdapterCollaborators of(
      final T core) {

    return of(core, mock(NameClashAvoidanceSupport.class));

  }

  /**
   * @param <T> A double playing both halves of the task SPI
   * @param core The double
   * @param scoping What the test wants the name-clash avoidance to answer
   * @return A complete set built around them
   */
  public static <T extends WorkflowTaskWiring & WorkflowTaskInvoker> AdapterCollaborators of(
      final T core,
      final NameClashAvoidanceSupport scoping) {

    return AdapterCollaborators
        .forAdapter("c8")
        .workflowTaskWiring(core)
        .workflowTaskInvoker(core)
        .scoping(scoping)
        .workflowAggregateSync(mock(WorkflowAggregateSync.class))
        .preCommitRegistrar(mock(PreCommitRegistrar.class))
        .workflowEndedInvoker(mock(WorkflowEndedInvoker.class))
        .bpmsInitiatedStartInvoker(mock(BpmsInitiatedStartInvoker.class))
        .build();

  }

}
