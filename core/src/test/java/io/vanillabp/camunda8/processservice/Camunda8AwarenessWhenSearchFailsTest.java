package io.vanillabp.camunda8.processservice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.CamundaFuture;
import io.camunda.client.api.ProblemDetail;
import io.camunda.client.api.command.ProblemException;
import io.camunda.client.api.search.request.ProcessInstanceSearchRequest;
import io.camunda.client.api.search.response.ProcessInstance;
import io.camunda.client.api.search.response.SearchResponse;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.integration.adapter.spi.WorkflowAwareness;
import io.vanillabp.integration.adapter.spi.WorkflowScope;
import io.vanillabp.integration.spi.AggregatePersistenceAware;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What the election probe answers when its search fails, and what decides it.
 * <p>
 * Two failures which look alike from the outside get opposite answers, and the
 * difference is not read from either of them: a cluster which refuses to be searched
 * cannot be probed at all, so the probe answers optimistically, while a cluster which
 * answers searches and failed this one is in trouble and the probe reports
 * BPMS_UNAVAILABLE. Which of the two this cluster is was settled before, by a probe of
 * its own.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8AwarenessWhenSearchFailsTest {

  private static final WorkflowScope SCOPE = WorkflowScope.of("test-module", "TestProcess");

  private record Aggregate(String id) {
  }

  private static AggregatePersistenceAware<Aggregate> persistence() {

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
        return aggregate.id();
      }

    };

  }

  /**
   * How a cluster refuses a query-API request - the answer of every query endpoint of a
   * cluster which cannot be searched at all.
   */
  private static ProblemException refusal() {

    final var details = new ProblemDetail();
    details.setStatus(403);
    details.setTitle("FORBIDDEN");
    return new ProblemException(403, "Forbidden", details);

  }

  /**
   * A process service whose cluster is asked once whether it can be searched - the way
   * the adapter asks while it starts processing a workflow module - and fails every
   * search after that.
   *
   * @param probeAnswer What the probe runs into, or <code>null</code> where the cluster
   *          answers it
   * @param searchFailure What every search after the probe throws
   * @return The service under test, its capability settled
   */
  private static Camunda8ProcessService<Aggregate> serviceOf(
      final RuntimeException probeAnswer,
      final RuntimeException searchFailure) {

    final var client = mock(CamundaClient.class);
    final var search = mock(ProcessInstanceSearchRequest.class, RETURNS_SELF);
    when(client.newProcessInstanceSearchRequest()).thenReturn(search);
    @SuppressWarnings("unchecked")
    final SearchResponse<ProcessInstance> nothing = mock(SearchResponse.class);
    when(nothing.items()).thenReturn(List.of());
    @SuppressWarnings("unchecked")
    final CamundaFuture<SearchResponse<ProcessInstance>> answer = mock(CamundaFuture.class);
    when(answer.join()).thenReturn(nothing);
    final var probed = new AtomicBoolean();
    when(search.send()).thenAnswer(invocation -> {
      if (probed.compareAndSet(false, true)) {
        if (probeAnswer != null) {
          throw probeAnswer;
        }
        return answer;
      }
      throw searchFailure;
    });

    final var configuration = new Camunda8AdapterConfiguration();
    // an address nothing ever contacts - every request of this test meets the mock below
    configuration.setRestAddress("http://localhost:1");
    final var clientFactory = new Camunda8ClientFactory("c8", configuration) {

      @Override
      public CamundaClient getClient() {
        return client;
      }

    };
    final var service = new Camunda8ProcessService<Aggregate>(
        "c8", clientFactory, Duration.ofDays(14), (
            aggregateClass,
            check) -> check.run(), null, Duration.ZERO);
    // what startWorkflowProcessing does, and the only search which is allowed to
    // succeed here: from now on the capability is settled and every later failure is
    // read against it
    clientFactory.getQueryApi().answers();
    return service;

  }

  @Test
  @DisplayName("A cluster which can be searched turns a failing probe into BPMS_UNAVAILABLE")
  public void aSearchableClusterFailingASearchIsUnavailable() {

    // the failure even carries the words a cluster without secondary storage uses, and
    // they change nothing: this cluster answered the probe, so a search failing now is
    // an outage
    final var service = serviceOf(
        null,
        new IllegalStateException("This endpoint requires a secondary storage, but none is set"));

    assertTrue(service.canLocateWorkflows());
    assertEquals(
        WorkflowAwareness.BPMS_UNAVAILABLE,
        service.awarenessOfWorkflow(SCOPE, persistence(), "agg-1"));

  }

  @Test
  @DisplayName("A cluster which refuses to be searched is answered optimistically")
  public void anUnsearchableClusterIsAnsweredOptimistically() {

    final var service = serviceOf(refusal(), refusal());

    assertFalse(
        service.canLocateWorkflows(),
        "which is what lets the core refuse this adapter next to a second one");
    assertEquals(
        WorkflowAwareness.ACTIVE,
        service.awarenessOfWorkflow(SCOPE, persistence(), "agg-1"),
        "right while this is the only BPMS, and a guess as soon as it is not");

  }

  @Test
  @DisplayName("The re-dispatch probe of a start stays honest where the cluster cannot be searched")
  public void theRedispatchProbeStaysHonest() {

    final var service = serviceOf(refusal(), refusal());

    assertEquals(
        WorkflowAwareness.UNKNOWN_TO_BPMS,
        service.awarenessOfWorkflowForRedispatch(SCOPE, persistence(), "agg-1"),
        "an optimistic answer would skip a recovered start, which loses the workflow");

  }

  @Test
  @DisplayName("The re-dispatch probe reports an outage of a searchable cluster as such")
  public void theRedispatchProbeReportsAnOutage() {

    final var service = serviceOf(null, new IllegalStateException("connection reset"));

    assertEquals(
        WorkflowAwareness.BPMS_UNAVAILABLE,
        service.awarenessOfWorkflowForRedispatch(SCOPE, persistence(), "agg-1"),
        "so the outbox entry is retried instead of starting the workflow a second time");

  }

}
