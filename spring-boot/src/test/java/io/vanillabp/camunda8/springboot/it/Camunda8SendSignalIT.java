package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.util.function.Supplier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

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
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
public class Camunda8SendSignalIT {

  @Container
  static final GenericContainer<?> CAMUNDA = new GenericContainer<>(
      DockerImageName.parse("camunda/zeebe:8.8.31"))
      .withExposedPorts(8080, 26500, 9600)
      .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "none")
      .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
      .waitingFor(Wait
          .forHttp("/actuator/health/readiness")
          .forPort(9600)
          .forStatusCode(200)
          .withStartupTimeout(Duration.ofMinutes(3)));

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

  private void awaitUntil(
      final Supplier<Boolean> condition,
      final String description) throws InterruptedException {

    final var deadline = System.currentTimeMillis() + 60_000;
    while (!Boolean.TRUE.equals(condition.get())) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("timed out waiting for: "
            + description);
      }
      Thread.sleep(200);
    }

  }

  private Long start() {

    final var aggregateId = transactionTemplate
        .execute(status -> workflowService.startWorkflow().getId());
    assertNotNull(aggregateId);
    return aggregateId;

  }

  @Test
  @DisplayName("one broadcast continues every workflow waiting for the signal")
  public void broadcastContinuesEveryWaitingWorkflow() throws Exception {

    final var first = start();
    final var second = start();

    // the instances are created after the commit (phase two) and then reach the
    // catch event - give the cluster a moment before broadcasting
    Thread.sleep(2000);

    // the application passes the PLAIN signal name; the deployed model carries the
    // prefixed one
    transactionTemplate.executeWithoutResult(status -> workflowService.broadcast("OrderReceived"));

    for (final var aggregateId : java.util.List.of(first, second)) {
      awaitUntil(
          () -> "recordSignal".equals(
              repository
                  .findById(aggregateId)
                  .map(SignalDockerAggregate::getProcessedBy)
                  .orElse(null)),
          "the task behind the signal catch event of aggregate '%s' to run".formatted(aggregateId));
    }

  }

  @Test
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
