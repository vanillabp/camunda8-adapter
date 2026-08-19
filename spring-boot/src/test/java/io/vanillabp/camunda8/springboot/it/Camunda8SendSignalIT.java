package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * End-to-end test of broadcasting a BPMN signal (story 42) against a real Camunda 8:
 * two workflows wait at an intermediate signal catch event, one broadcast continues
 * both of them. The broadcast happens in phase two, after the local transaction was
 * committed - a rolled-back transaction broadcasts nothing, because its outbox entry
 * is gone with it.
 * <p>
 * The cluster is deployed with prefixed identifiers (the module's
 * name-clash-avoidance mode), so the signal reaches its subscription only if the
 * adapter scopes the plain name the application passed.
 * <p>
 * Runs WITHOUT secondary storage, so the query API is unavailable and the adapter's
 * awareness probe answers optimistically - what this test exercises is that fallback.
 * The query path is covered by {@code Camunda8SecondaryStorageIT}, which brings its own
 * Elasticsearch (story 52).
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
// the order is part of the test: a broadcast reaches EVERY workflow of the module which
// waits at that moment, and the broadcast test repeats its signal until both of its
// instances continued. Whatever is still in flight from that loop would continue the
// instance of the rollback test as well, which is what made it fail in the GitHub build
// ('expected <null> but was <recordSignal>'). The rollback test therefore runs FIRST, on a
// cluster nobody signalled yet.
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
// closed when the class is done: every IT here has a context of its own (its own
// container), Spring would keep them all until the JVM exits, and a context outliving
// its cluster keeps its job workers polling an address nobody answers - which is what
// made the later classes of this module run into their timeouts
@DirtiesContext
public class Camunda8SendSignalIT {

  @Container
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.standaloneBroker();

  @DynamicPropertySource
  static void camunda8Properties(
      final DynamicPropertyRegistry registry) {

    registry
        .add(
            "vanillabp.adapters.c8.rest-address",
            () -> "http://"
                + CAMUNDA.getHost()
                + ":"
                + CAMUNDA.getMappedPort(8080));
    registry
        .add(
            "vanillabp.adapters.c8.grpc-address",
            () -> "http://"
                + CAMUNDA.getHost()
                + ":"
                + CAMUNDA.getMappedPort(26500));

  }

  @Autowired
  private SignalDockerWorkflowService workflowService;

  @Autowired
  private SignalDockerAggregateRepository repository;

  @Autowired
  private TransactionTemplate transactionTemplate;

  private Long start() {

    final var aggregateId = transactionTemplate
        .execute(status -> workflowService.startWorkflow().getId());
    assertNotNull(aggregateId);
    return aggregateId;

  }

  @Test
  @Order(2)
  @DisplayName("one broadcast continues every workflow waiting for the signal")
  public void broadcastContinuesEveryWaitingWorkflow() throws Exception {

    final var first = start();
    final var second = start();

    // A SIGNAL IS NOT BUFFERED: it reaches whoever waits at that very moment. The
    // instances here are created after the commit (phase two) and need a moment to
    // reach the catch event, so the test broadcasts REPEATEDLY until both continued
    // instead of guessing a sleep - which is also what an application would do if it
    // cared, and harmless because a signal has no deduplication anyway.
    // generous since this module also runs a two-container test (story 44): under that
    // load the cluster needs longer to reach the catch event, and a signal is not
    // buffered - the broadcast has to keep meeting a workflow which already waits
    final var deadline = System.currentTimeMillis() + 150_000;
    while (!bothContinued(first, second)) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("the workflows waiting for the signal never continued");
      }
      // the application passes the PLAIN signal name; the deployed model carries the
      // prefixed one
      transactionTemplate.executeWithoutResult(status -> workflowService.broadcast("OrderReceived"));
      Thread.sleep(2000);
    }

  }

  private boolean bothContinued(
      final Long first,
      final Long second) {

    return java.util.List
        .of(first, second)
        .stream()
        .allMatch(aggregateId -> "recordSignal".equals(
            repository
                .findById(aggregateId)
                .map(SignalDockerAggregate::getProcessedBy)
                .orElse(null)));

  }

  @Test
  @Order(1)
  @DisplayName("a broadcast in a rolled-back transaction never reaches the cluster")
  public void rollbackBroadcastsNothing() throws Exception {

    final var aggregateId = start();
    Thread.sleep(2000);

    try {
      transactionTemplate.executeWithoutResult(status -> {
        workflowService.broadcast("OrderReceived");
        throw new RuntimeException("test rollback");
      });
    } catch (final RuntimeException e) {
      assertEquals("test rollback", e.getMessage());
    }

    // the outbox entry carrying the broadcast rode the rolled-back transaction
    Thread.sleep(3000);
    assertNull(
        repository
            .findById(aggregateId)
            .map(SignalDockerAggregate::getProcessedBy)
            .orElse(null));

  }

}
