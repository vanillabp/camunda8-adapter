package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vanillabp.camunda8.observability.Camunda8Metrics;
import io.vanillabp.camunda8.observability.MicrometerCamunda8Metrics;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The designed back pressure against a real cluster: a worker of an adapter whose every
 * execution slot is busy does not ask the cluster for work, so a job nobody could run is
 * left where it is. It is the client's own activation counter which answers, because it
 * counts a job before VanillaBP sees anything of it.
 * <p>
 * The setup is the one the starvation was measured with, asked the other way round: one
 * blocking handler, a second worker, and the question whether a job is fetched which
 * nobody can run. One execution slot here, so the blocking handler occupies all of them.
 * <p>
 * Two settings make the measurement say what it should. The block lasts long enough to
 * outlive an activation request which was already parked at the cluster when the slot
 * filled - such a request is answered whenever a job appears, and no gate on this side
 * reaches it. And <code>request-timeout</code> is two seconds rather than the default ten,
 * so that parked request is over before the workflow of the second worker is started. What
 * is measured is therefore the rule itself: a worker which has to ask again waits for a
 * slot.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
@DirtiesContext
public class Camunda8PollWhenASlotIsFreeIT {

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
    registry.add("vanillabp.adapters.c8.worker-threads", () -> "1");
    registry.add("vanillabp.adapters.c8.request-timeout", () -> "PT2S");

  }

  @Autowired
  private WorkerThreadsDockerWorkflowService blockingWorkflowService;

  @Autowired
  private QuickDockerWorkflowService quickWorkflowService;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private MicrometerCamunda8Metrics metrics;

  /**
   * How long the only execution slot stays busy here: long enough for a parked activation
   * request to run out, for the second workflow to be started afterwards and for the
   * measurement to be taken, and still clearly below the twenty seconds this workflow
   * module locks a job for.
   */
  private static final long BLOCK_MILLIS = 12_000;

  @BeforeEach
  public void resetObservations() {

    WorkerThreadsDockerWorkflowService.reset();
    WorkerThreadsDockerWorkflowService.blockFor(BLOCK_MILLIS);

  }

  @AfterEach
  public void resetObservationsForWhoeverComesNext() {

    WorkerThreadsDockerWorkflowService.reset();

  }

  @Test
  @DisplayName("a worker which has to ask again does not, while the only execution slot is busy")
  public void noJobIsFetchedWhileTheOnlySlotIsBusy() throws Exception {

    // this application has no Actuator, which is what applies MeterBinder beans in a real
    // one, so the meters of this adapter are bound by hand
    final var registry = new SimpleMeterRegistry();
    metrics.bindTo(registry);

    final var blocked = transactionTemplate
        .execute(status -> blockingWorkflowService.startBlocking().getId());
    assertNotNull(blocked);
    assertTrue(
        WorkerThreadsDockerWorkflowService.BLOCKING_ENTERED.await(30, TimeUnit.SECONDS),
        "the blocking handler was delivered");

    // the activation request the quick worker had parked at the cluster when the slot
    // filled runs out within request-timeout, and everything it asks afterwards waits
    // for a slot
    Thread.sleep(3_000);

    final var quick = transactionTemplate
        .execute(status -> quickWorkflowService.startWorkflow().getId());
    assertNotNull(quick);

    // long enough for the start to have reached the cluster - the outbox dispatches
    // within half a second here - and short enough to stay inside the block
    Thread.sleep(4_000);

    assertTrue(
        activated(registry, "blockingTask") >= 1.0,
        "the measurement works: the job the blocking handler is inside was activated");
    assertEquals(
        0.0,
        activated(registry, "quickTask"),
        "but no job is fetched for a worker of an adapter whose every slot is busy");

    assertTrue(
        WorkerThreadsDockerWorkflowService.QUICK_SERVED.await(30, TimeUnit.SECONDS),
        "and the job is served once the blocking handler gave its slot back");
    assertTrue(
        activated(registry, "quickTask") >= 1.0,
        "which is when it was activated");

  }

  /**
   * What the client counted for the worker of one task definition. The tag carries the job
   * type the WORKER subscribes to, which is the scoped one wherever name-clash avoidance
   * prefixes it.
   *
   * @param registry Where the adapter's meters were bound
   * @param taskDefinition The task definition the worker serves
   * @return How many jobs the client activated for it
   */
  private static double activated(
      final MeterRegistry registry,
      final String taskDefinition) {

    return registry
        .find(Camunda8Metrics.JOBS_ACTIVATED)
        .counters()
        .stream()
        .filter(counter -> counter
            .getId()
            .getTag(Camunda8Metrics.TAG_JOB_TYPE)
            .endsWith(taskDefinition))
        .mapToDouble(Counter::count)
        .sum();

  }

}
