package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.observability.Camunda8Metrics;
import io.vanillabp.camunda8.observability.MicrometerCamunda8Metrics;
import io.vanillabp.integration.health.VanillaBpHealthIndicator;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Execution slots against a real Camunda 8 cluster: a handler which
 * blocks must not delay the job of another worker of the same adapter.
 * <p>
 * The defect this guards against, turned into a test. The client's own default is
 * ONE execution thread, and that one thread runs every handler invocation AND the poll
 * scheduling of every worker of the client, so a blocking handler stopped the adapter
 * from asking the cluster for work at all. The spike measured 8013 ms of delay for an
 * unrelated job with one thread and 13 ms with four; this test asserts the property
 * rather than the number, because a cluster in a build has its own latencies.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
// closed when the class is done: every IT here has a context of its own (its own
// container), Spring would keep them all until the JVM exits, and a context outliving
// its cluster keeps its job workers polling an address nobody answers
@DirtiesContext
public class Camunda8WorkerThreadsIT {

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
  private WorkerThreadsDockerWorkflowService blockingWorkflowService;

  @Autowired
  private QuickDockerWorkflowService quickWorkflowService;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private Camunda8ClientFactoryRegistry clientFactoryRegistry;

  @Autowired
  private MicrometerCamunda8Metrics metrics;

  @Autowired
  private VanillaBpHealthIndicator healthIndicator;

  @BeforeEach
  public void resetObservations() {

    WorkerThreadsDockerWorkflowService.reset();

  }

  @AfterEach
  public void resetObservationsForWhoeverComesNext() {

    WorkerThreadsDockerWorkflowService.reset();

  }

  @Test
  @DisplayName("the adapter runs its workers on the four platform threads it chose, not on the client's one")
  public void theAdapterChoosesItsExecutionModel() {

    final var factory = clientFactoryRegistry.getFactory("c8");

    Assertions.assertEquals(4, factory.getExecutionModel().slots(),
        "nothing is configured here, so the adapter's default applies");
    Assertions.assertEquals(4, factory.getExecutor().getBound(),
        "and it reaches the executor the adapter hands the client, which would otherwise use one thread");

  }

  @Test
  @DisplayName("a blocking handler does not delay the job of another worker of the same adapter")
  public void aBlockingHandlerDoesNotDelayAnotherWorker() throws Exception {

    final var blocked = transactionTemplate
        .execute(status -> blockingWorkflowService.startBlocking().getId());
    assertNotNull(blocked);

    assertTrue(
        WorkerThreadsDockerWorkflowService.BLOCKING_ENTERED
            .await(30, TimeUnit.SECONDS),
        "the blocking handler was delivered");

    final var startedAt = System.currentTimeMillis();
    final var quick = transactionTemplate
        .execute(status -> quickWorkflowService.startWorkflow().getId());
    assertNotNull(quick);

    assertTrue(
        WorkerThreadsDockerWorkflowService.QUICK_SERVED
            .await(WorkerThreadsDockerWorkflowService.BLOCK_MILLIS, TimeUnit.MILLISECONDS),
        "the other worker's job was served while a handler was blocking, not after it");

    final var waited = WorkerThreadsDockerWorkflowService.QUICK_SERVED_AT.get() - startedAt;
    assertTrue(
        WorkerThreadsDockerWorkflowService.QUICK_SERVED_WHILE_BLOCKED.get(),
        "the blocking handler was still inside its slot, so the two really ran at the same time");
    assertTrue(waited < WorkerThreadsDockerWorkflowService.BLOCK_MILLIS,
        "the job waited "
            + waited
            + " ms, which is the runtime of the blocking handler rather than the cluster's latency");

  }

  @Test
  @DisplayName("the cluster reports itself to the metrics and to the health endpoint")
  public void theClusterIsMeasuredAndReported() throws Exception {

    // the meters of this adapter, bound by hand: this application has no Actuator, which
    // is what applies MeterBinder beans in a real one
    final var registry = new SimpleMeterRegistry();
    metrics.bindTo(registry);

    // the slot gauges are there as soon as the client exists - the adapter owns the
    // executor in this mode too, so all three of them say something
    Assertions.assertEquals(
        4.0,
        registry
            .get(Camunda8Metrics.EXECUTION_SLOTS_CONFIGURED)
            .tag(Camunda8Metrics.TAG_ADAPTER, "c8")
            .gauge()
            .value());
    assertNotNull(
        registry
            .find(Camunda8Metrics.EXECUTION_SLOTS_IN_USE)
            .tag(Camunda8Metrics.TAG_ADAPTER, "c8")
            .gauge(),
        "the adapter owns the executor in this mode too, so it can say how many slots are busy");
    assertNotNull(
        registry
            .find(Camunda8Metrics.JOBS_WAITING)
            .tag(Camunda8Metrics.TAG_ADAPTER, "c8")
            .gauge(),
        "and how many jobs wait for one");

    // a job which really travelled through the cluster: the client counts it before
    // VanillaBP sees anything of it
    final var quick = transactionTemplate
        .execute(status -> quickWorkflowService.startWorkflow().getId());
    assertNotNull(quick);
    assertTrue(
        WorkerThreadsDockerWorkflowService.QUICK_SERVED
            .await(30, TimeUnit.SECONDS),
        "the quick handler was delivered");

    // the tag carries the job type the WORKER subscribes to, which is the scoped one
    // wherever name-clash avoidance prefixes it (here 'test-app__QuickProcess__quickTask')
    final var activated = registry
        .find(Camunda8Metrics.JOBS_ACTIVATED)
        .counters()
        .stream()
        .filter(counter -> counter
            .getId()
            .getTag(Camunda8Metrics.TAG_JOB_TYPE)
            .endsWith("quickTask"))
        .mapToDouble(Counter::count)
        .sum();
    assertTrue(
        activated >= 1.0,
        "the client's own activation counter reaches the application's registry");

    // and the cluster which just served that job answers the health question
    final var health = healthIndicator.health();
    Assertions.assertEquals(
        Status.UP,
        health.getStatus());
    @SuppressWarnings("unchecked")
    final var detail = (Map<String, Object>) health
        .getDetails()
        .get("c8");
    assertNotNull(detail.get("gatewayVersion"), "a cluster which answered says which version it is");
    assertNotNull(detail.get("address"));

  }

}
