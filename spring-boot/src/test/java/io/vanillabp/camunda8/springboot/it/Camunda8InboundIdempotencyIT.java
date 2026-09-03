package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.integration.adapter.migration.processservice.DeliveryRecords;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * A REDELIVERY of a task VanillaBP already processed, against a real Camunda 8
 * cluster: it must not run the <code>&#64;WorkflowTask</code> method again.
 * <p>
 * Forcing a redelivery of a task whose result the cluster already learned is impossible -
 * the cluster would have to lose a completion. What can be forced is the case of an
 * asynchronous task: a <code>&#64;TaskId</code> method leaves the job open, the adapter
 * renews its lock for <code>async-task-lock-renewal</code>, and once that window passes
 * the cluster hands the very same job out again. This test therefore runs with a renewal
 * window of two seconds and waits several of those windows out: the handler must have run
 * exactly once, every further delivery being answered from the record VanillaBP wrote in
 * the handler's own transaction.
 * <p>
 * The second half of the arrangement is the retention of the delivery records, set
 * to ten seconds here, well below the runtime of this test and five renewal
 * windows wide. That is the relation which matters - the window has to sit
 * clearly below the retention, because the record is what answers the redelivery which
 * renews the lock. A configuration violating it does not even boot.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = {
        "spring.config.name=camunda8-it",
        // the open job's lock expires after two seconds, so the cluster redelivers it
        "vanillabp.adapters.c8.async-task-lock-renewal=PT2S",
        // five renewal windows: the record has to answer every one of them
        "vanillabp.delivery.retention=PT10S",
        // deliberately far away from the number above: the delivery log has to read its
        // own retention, and against a real cluster this is where reading the outbox one
        // would show
        "vanillabp.outbox.retention=P7D"
    })
// closed when the class is done: every IT here has a context of its own (its own
// container), Spring would keep them all until the JVM exits, and a context outliving
// its cluster keeps its job workers polling an address nobody answers - which is what
// made the later classes of this module run into their timeouts
@DirtiesContext
public class Camunda8InboundIdempotencyIT {

  @Container
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.standaloneBroker();

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
  private Camunda8ClientFactoryRegistry clientFactoryRegistry;

  private void awaitUntil(
      final Supplier<Boolean> condition,
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
    // a repeated delivery is answered from the core's delivery records, which is where
    // the message about it comes from
    final var coreLogger = (ch.qos.logback.classic.Logger) LoggerFactory
        .getLogger(DeliveryRecords.class);
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
      // awaited, not asserted right away: the counter above is incremented INSIDE the
      // handler, while the record is written after it returned and becomes visible with
      // the commit - reading it immediately is a race the test lost on a CI runner
      awaitUntil(
          () -> new JdbcTemplate(dataSource)
              .queryForList(
                  "SELECT OUTCOME FROM VANILLABP_TASK_DELIVERY WHERE AGGREGATE_ID = ? AND TASK_DEFINITION = ?",
                  String.class,
                  String.valueOf(aggregateId),
                  "asyncTask")
              .contains("COMPLETION_PENDING"),
          30000,
          "the delivery of the asynchronous task to be recorded as pending completion");

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
