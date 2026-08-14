package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Story 51 against a real Camunda 8 cluster: a REDELIVERY of a task VanillaBP already
 * processed must not run the <code>&#64;WorkflowTask</code> method again.
 * <p>
 * Forcing a redelivery of a task whose result the cluster already learned is impossible -
 * the cluster would have to lose a completion. What can be forced is the case of an
 * asynchronous task: a <code>&#64;TaskId</code> method leaves the job open, the adapter
 * extends its lock to <code>async-task-timeout</code>, and once that passes the cluster
 * hands the very same job out again. This test therefore runs with an
 * <code>async-task-timeout</code> of two seconds and waits several of those windows out:
 * the handler must have run exactly once, every further delivery being answered from the
 * record VanillaBP wrote in the handler's own transaction.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = {
        "spring.config.name=camunda8-it",
        // the dormant job's lock expires after two seconds, so the cluster redelivers it
        "vanillabp.adapters.c8.async-task-timeout=PT2S"
    })
public class Camunda8InboundIdempotencyIT {

  @Container
  static final GenericContainer<?> CAMUNDA = new GenericContainer<>(
      DockerImageName.parse("camunda/zeebe:8.8.31"))
      .withExposedPorts(8080, 26500, 9600)
      .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "none")
      .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
      .waitingFor(org.testcontainers.containers.wait.strategy.Wait
          .forHttp("/actuator/health/readiness")
          .forPort(9600)
          .forStatusCode(200)
          .withStartupTimeout(Duration.ofMinutes(3)));

  @DynamicPropertySource
  static void camunda8Properties(
      final DynamicPropertyRegistry registry) {

    registry.add("vanillabp.adapters.c8.rest-address",
        () -> "http://"
            + CAMUNDA.getHost()
            + ":"
            + CAMUNDA.getMappedPort(8080));
    registry.add("vanillabp.adapters.c8.grpc-address",
        () -> "http://"
            + CAMUNDA.getHost()
            + ":"
            + CAMUNDA.getMappedPort(26500));

  }

  @Autowired
  private TaskDockerAggregateRepository repository;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private DataSource dataSource;

  @Autowired
  private io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry clientFactoryRegistry;

  private void awaitUntil(
      final java.util.function.Supplier<Boolean> condition,
      final long timeoutMillis,
      final String description) throws InterruptedException {

    final var deadline = System.currentTimeMillis() + timeoutMillis;
    while (!Boolean.TRUE.equals(condition.get())) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("timed out waiting for: "
            + description);
      }
      Thread.sleep(200);
    }

  }

  private int invocations(
      final Long aggregateId) {

    final var counter = TaskDockerWorkflowService.INVOCATIONS
        .get("asyncTask:"
            + aggregateId);
    return counter != null
        ? counter.get()
        : 0;

  }

  @Test
  @DisplayName("A job the cluster hands out again does not run the handler a second time")
  public void redeliveredJobsSkipTheHandler() throws Exception {

    // the core logs every skipped redelivery - the proof that the cluster really
    // handed the job out again (an unnoticed redelivery and none at all would look
    // the same on the aggregate)
    final var skippedDeliveries = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    skippedDeliveries.start();
    final var coreLogger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
        .getLogger(io.vanillabp.integration.adapter.migration.processservice.MigrationProcessService.class);
    coreLogger.addAppender(skippedDeliveries);

    try {
      final var aggregateId = transactionTemplate.execute(status -> repository
          .save(new TaskDockerAggregate())
          .getId());
      // the counters are static and the aggregate IDs of the test classes overlap
      TaskDockerWorkflowService.INVOCATIONS
          .remove("asyncTask:"
              + aggregateId);

      // AsyncProcess parks at a @TaskId task: the job stays open and its lock expires
      // after the configured two seconds
      clientFactoryRegistry
          .getFactory("c8")
          .getClient()
          .newCreateInstanceCommand()
          .bpmnProcessId("test-app__AsyncProcess")
          .latestVersion()
          .variable("id", String.valueOf(aggregateId))
          .send()
          .join();

      awaitUntil(
          () -> invocations(aggregateId) == 1,
          60000,
          "the asynchronous task to be invoked once");

      // the delivery was recorded in the handler's transaction, with the outcome the
      // adapter reports again on every redelivery. The record is looked up by task, not
      // by aggregate alone: every integration test class of this repository shares the
      // in-memory database, and their aggregate IDs start at 1 just like ours
      assertTrue(
          new JdbcTemplate(dataSource)
              .queryForList(
                  "SELECT OUTCOME FROM VANILLABP_TASK_DELIVERY WHERE AGGREGATE_ID = ? AND TASK_DEFINITION = ?",
                  String.class,
                  String.valueOf(aggregateId),
                  "asyncTask")
              .contains("COMPLETION_PENDING"),
          "the delivery of the asynchronous task was recorded as pending completion");

      // wait out several lock windows - each expiry hands the job out again
      awaitUntil(
          () -> messagesOf(skippedDeliveries)
              .stream()
              .anyMatch(message -> message.contains("Skipping the repeated delivery")),
          30000,
          "the cluster to redeliver the dormant job");

      assertEquals(
          1,
          invocations(aggregateId),
          "the redelivered job must not run the handler again");
      assertEquals(
          "async-open",
          repository.findById(aggregateId).orElseThrow().getResults(),
          "the handler's result was appended once, although the job arrived more than once");
      assertTrue(
          repository.findById(aggregateId).orElseThrow().getTaskId() != null,
          "the job key was committed as the task id");

    } finally {
      coreLogger.detachAppender(skippedDeliveries);
      skippedDeliveries.stop();
    }

  }

  private static List<String> messagesOf(
      final ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender) {

    return List
        .copyOf(appender.list)
        .stream()
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .toList();

  }

}
