package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.function.Supplier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * End-to-end test of a workflow the CLUSTER starts on its own (story 41) against a
 * real Camunda 8: a timer start event fires, the start execution listener VanillaBP
 * injected into the model activates a job, the core builds the workflow aggregate and
 * the job completion writes its ID into the instance - which is how the service task
 * behind the start event finds the aggregate again.
 * <p>
 * The class is skipped when Docker is unavailable.
 * <p>
 * Runs WITHOUT secondary storage, so the query API is unavailable and the adapter's
 * awareness probe answers optimistically - what this test exercises is that fallback.
 * The query path is covered by {@code Camunda8SecondaryStorageIT}, which brings its own
 * Elasticsearch (story 52).
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
// closed when the class is done: every IT here has a context of its own (its own
// container), Spring would keep them all until the JVM exits, and a context outliving
// its cluster keeps its job workers polling an address nobody answers - which is what
// made the later classes of this module run into their timeouts
@DirtiesContext
public class Camunda8BpmsInitiatedStartIT {

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
  private TimerStartDockerAggregateRepository repository;

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

  @Test
  @DisplayName("a timer start event creates the workflow aggregate and the following task finds it")
  public void timerStartCreatesTheAggregate() throws Exception {

    // the timer fires a second after the deployment; the start execution listener
    // gates the instance until VanillaBP built the aggregate
    awaitUntil(() -> !repository.findAll().isEmpty(), "the timer to fire and the aggregate to be created");

    final var aggregates = repository.findAll();
    assertEquals(1, aggregates.size(), "one workflow, one aggregate");
    final var aggregate = aggregates.getFirst();

    // the cluster's own identity of the start is the aggregate's ID: the process
    // instance key, which survives a retried listener job
    assertNotNull(aggregate.getId());
    assertTrue(
        aggregate.getId().matches("\\d+"),
        "the aggregate's ID is the process instance key: "
            + aggregate.getId());

    // the service task behind the start event ran against exactly that aggregate,
    // which proves the aggregate-ID variable was written by the listener completion
    awaitUntil(
        () -> "recordTimerStart".equals(
            repository
                .findById(aggregate.getId())
                .map(TimerStartDockerAggregate::getProcessedBy)
                .orElse(null)),
        "the task following the timer start event to be processed");

    // story 43: the cluster reports the end of the workflow, and the application's
    // method ran against the aggregate
    awaitUntil(
        () -> "COMPLETED".equals(
            repository
                .findById(aggregate.getId())
                .map(TimerStartDockerAggregate::getEndedAs)
                .orElse(null)),
        "the end of the workflow to be reported");

  }

}
