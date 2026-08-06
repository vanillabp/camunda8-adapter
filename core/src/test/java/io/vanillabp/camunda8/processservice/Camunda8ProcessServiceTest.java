package io.vanillabp.camunda8.processservice;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.integration.spi.AggregatePersistenceAware;

/**
 * Unit tests of {@link Camunda8ProcessService} that do not require a cluster: phase one
 * validates only (never contacts Camunda 8). Phase two (which creates the process
 * instance on the cluster) is covered end-to-end by {@code Camunda8DeploymentAndStartIT}.
 */
public class Camunda8ProcessServiceTest {

  /** A minimal aggregate whose ID is configurable (including {@code null}). */
  private record Aggregate(Object id) {
  }

  private static AggregatePersistenceAware<Aggregate> persistence(
      final Object aggregateId) {

    return new AggregatePersistenceAware<>() {
      @Override
      public Class<Aggregate> getAggregateClass() {
        return Aggregate.class;
      }

      @Override
      public Aggregate save(
          final Aggregate aggregate) {
        return aggregate;
      }

      @Override
      public Object getAggregateId(
          final Aggregate aggregate) {
        return aggregateId;
      }
    };

  }

  private static Camunda8ProcessService<Aggregate> configuredService() {

    final var configuration = new Camunda8AdapterConfiguration();
    // a bogus address that is never contacted in phase one
    configuration.setRestAddress("http://localhost:1");
    return new Camunda8ProcessService<>(
        "c8", new Camunda8ClientFactory("c8", configuration), java.time.Duration.ofDays(14), Runnable::run, null);

  }

  @Test
  @DisplayName("phase one validates a configured adapter without contacting the cluster")
  public void phaseOneValidatesWithoutContactingCluster() {

    final var service = configuredService();

    assertDoesNotThrow(() -> service.startWorkflowPhaseOne(
        "module", "Process", persistence("agg-1"), new Aggregate("agg-1")));

  }

  @Test
  @DisplayName("phase one fails naming the missing property if the adapter is not configured")
  public void phaseOneFailsIfNotConfigured() {

    final var service = new Camunda8ProcessService<Aggregate>(
        "c8", new Camunda8ClientFactory("c8", new Camunda8AdapterConfiguration()), java.time.Duration
            .ofDays(14), Runnable::run, null);

    final var exception = assertThrows(
        IllegalStateException.class,
        () -> service.startWorkflowPhaseOne("module", "Process", persistence("agg-1"), new Aggregate("agg-1")));
    assertTrue(exception.getMessage().contains("vanillabp.adapters.c8.rest-address"));

  }

  @Test
  @DisplayName("the re-dispatch probe never answers ACTIVE on a failing query - a recovered start must not be skipped")
  public void redispatchProbeIsNeverOptimisticOnFailure() {

    // an unreachable cluster must yield BPMS_UNAVAILABLE (the outbox entry stays
    // pending and is retried) - answering ACTIVE would SKIP a recovered start and
    // thereby lose the workflow, which is why this probe is stricter than the
    // election's awarenessOfWorkflow (that one may answer optimistically when the
    // query API is absent)
    final var awareness = configuredService().awarenessOfWorkflowForRedispatch("agg-1");

    assertTrue(
        awareness == io.vanillabp.integration.adapter.spi.WorkflowAwareness.BPMS_UNAVAILABLE,
        "expected BPMS_UNAVAILABLE but got "
            + awareness);

  }

}
