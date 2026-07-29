package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.camunda.client.api.worker.JobWorker;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.processservice.Camunda8ProcessService;
import io.vanillabp.spi.process.ProcessService;

/**
 * End-to-end integration test of the Camunda 8 adapter against a real Camunda 8 cluster
 * (Testcontainers, {@code camunda/zeebe:8.8.31}, standalone broker without Elasticsearch,
 * unprotected API). It drives the <b>full two-phase workflow start through
 * {@code ProcessService#startWorkflow}</b> inside a JPA transaction with the gruelbox
 * phase-two outbox:
 * <ul>
 *   <li>the BPMN {@code TestProcess} is deployed to the cluster on application startup,</li>
 *   <li>starting a workflow inside a committed transaction creates the process instance
 *       only <b>after the commit</b> (phase two) carrying the {@code aggregateId}
 *       variable - proven by a raw Camunda 8 job worker activating the service task and
 *       observing the variable, and</li>
 *   <li>a rolled-back transaction never creates an instance (no job is ever activated and
 *       the outbox entry is gone).</li>
 * </ul>
 * The class is skipped when Docker is unavailable
 * ({@code @Testcontainers(disabledWithoutDocker = true)}).
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
public class Camunda8DeploymentAndStartIT {

  private static final String JOB_TYPE = "test-job";

  private static final String COUNT_OUTBOX_ENTRIES = "select count(*) from TXNO_OUTBOX";

  @Container
  static final GenericContainer<?> CAMUNDA = new GenericContainer<>(
      DockerImageName.parse("camunda/zeebe:8.8.31"))
      .withExposedPorts(8080, 26500, 9600)
      // run the broker standalone (no Elasticsearch) with an unprotected API so no
      // authentication/identity provider is needed for this test
      .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "none")
      .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
      // the readiness probe turns UP only once the partition leader can accept
      // deployments, avoiding a transient 503 on the first deploy at startup
      .waitingFor(Wait
          .forHttp("/actuator/health/readiness")
          .forPort(9600)
          .forStatusCode(200)
          .withStartupTimeout(Duration.ofMinutes(3)));

  /** Aggregate IDs seen by the job worker on the deployed process's service task. */
  private static final List<String> ACTIVATED_AGGREGATE_IDS = new CopyOnWriteArrayList<>();

  private static JobWorker jobWorker;

  @DynamicPropertySource
  static void camunda8Properties(
      final DynamicPropertyRegistry registry) {

    registry.add("camunda8-adapter.c8.rest-address",
        () -> "http://"
            + CAMUNDA.getHost()
            + ":"
            + CAMUNDA.getMappedPort(8080));
    registry.add("camunda8-adapter.c8.grpc-address",
        () -> "http://"
            + CAMUNDA.getHost()
            + ":"
            + CAMUNDA.getMappedPort(26500));

  }

  @Autowired
  private ProcessService<DockerAggregate> processService;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private Camunda8ClientFactoryRegistry clientFactoryRegistry;

  @BeforeEach
  void openWorker() {

    ACTIVATED_AGGREGATE_IDS.clear();
    if (jobWorker == null) {
      // a raw Camunda 8 job worker completing the service task and recording the
      // aggregateId variable (adapter task wiring is a later story)
      jobWorker = clientFactoryRegistry
          .getFactory("c8")
          .getClient()
          .newWorker()
          .jobType(JOB_TYPE)
          .handler((
              jobClient,
              job) -> {
            ACTIVATED_AGGREGATE_IDS.add(
                String.valueOf(job.getVariablesAsMap().get(Camunda8ProcessService.AGGREGATE_ID_VARIABLE)));
            jobClient
                .newCompleteCommand(job)
                .send()
                .join();
          })
          .name("it-worker")
          .fetchVariables(Camunda8ProcessService.AGGREGATE_ID_VARIABLE)
          .open();
    }

  }

  @AfterAll
  static void closeWorker() {

    if (jobWorker != null) {
      jobWorker.close();
      jobWorker = null;
    }

  }

  private long countOutboxEntries() {

    final var count = jdbcTemplate.queryForObject(COUNT_OUTBOX_ENTRIES, Long.class);
    return count == null ? 0 : count;

  }

  @Test
  @DisplayName("startWorkflow in a committed transaction creates the instance only after commit with the aggregateId variable")
  public void instanceAppearsOnlyAfterCommit() throws Exception {

    final var attached = transactionTemplate.execute(status -> {
      final var aggregate = new DockerAggregate();
      aggregate.setContent("commit-test");
      final var saved = processService.startWorkflow(aggregate);
      // phase two runs only after commit - within the transaction no instance exists yet,
      // so the worker cannot have seen anything
      assertTrue(ACTIVATED_AGGREGATE_IDS.isEmpty(),
          "no process instance may exist before the transaction commits");
      return saved;
    });

    final var expectedAggregateId = String.valueOf(attached.getId());

    // after the commit the phase-two outbox dispatches the create; the worker eventually
    // activates the service task of the started instance carrying the aggregateId
    final var found = awaitAggregateId(expectedAggregateId, 20000);
    assertTrue(found,
        "expected a process instance with aggregateId '"
            + expectedAggregateId
            + "' after commit, but the worker saw "
            + ACTIVATED_AGGREGATE_IDS);

  }

  @Test
  @DisplayName("startWorkflow in a rolled-back transaction never creates an instance")
  public void noInstanceAfterRollback() throws Exception {

    final var entriesBefore = countOutboxEntries();

    final var exception = assertThrowsExactly(
        RuntimeException.class,
        () -> transactionTemplate.execute(status -> {
          final var aggregate = new DockerAggregate();
          aggregate.setContent("rollback-test");
          processService.startWorkflow(aggregate);
          throw new RuntimeException("test rollback");
        }));
    assertEquals("test rollback", exception.getMessage());

    // the outbox entry was enlisted in the rolled-back transaction, so it is gone
    assertEquals(entriesBefore, countOutboxEntries());

    // wait well beyond the outbox poll interval: no instance may ever be created, so the
    // worker must never activate a job
    // 3000ms is coupled to 'vanillabp.outbox.poll-interval: PT0.5S' in
    // camunda8-it.yaml: it spans several poll cycles, so a phase-two dispatch WOULD
    // have happened if the rollback had left an outbox entry behind - do not shrink
    // one without the other
    Thread.sleep(3000);
    assertTrue(ACTIVATED_AGGREGATE_IDS.isEmpty(),
        "no process instance may be created on rollback, but the worker saw "
            + ACTIVATED_AGGREGATE_IDS);

  }

  private boolean awaitAggregateId(
      final String aggregateId,
      final long timeoutMillis) throws InterruptedException {

    final var deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      if (ACTIVATED_AGGREGATE_IDS.contains(aggregateId)) {
        return true;
      }
      Thread.sleep(200);
    }
    return ACTIVATED_AGGREGATE_IDS.contains(aggregateId);

  }

}
