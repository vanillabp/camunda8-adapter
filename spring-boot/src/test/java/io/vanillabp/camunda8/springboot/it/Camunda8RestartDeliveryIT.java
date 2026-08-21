package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The acceptance test of story 102: a workflow started right after a restart gets its
 * first job in milliseconds rather than in a job timeout.
 * <p>
 * Two application contexts run in one JVM against one cluster, which is what a restart
 * with changed configuration looks like. The first one works one workflow, so its workers
 * have an activation request parked at the cluster, and is then closed. The second one
 * starts a workflow, and the time until its handler is reached is the number this test
 * exists for.
 * <p>
 * <b>What it guards.</b> An activation request which is parked at the cluster when its
 * client is closed stays parked, and a job created afterwards is activated into it and
 * answered by nobody. Measured against {@code camunda/camunda:8.9.16} with a plain client
 * and no VanillaBP: 20,2 to 21,0 seconds at a {@code job-timeout} of {@code PT20S} where
 * the second application starts seven seconds after the first one was closed, and 15 to
 * 25 milliseconds where it starts twelve seconds afterwards, which is beyond the client's
 * {@code request-timeout} of ten seconds. The adapter closes that window by waiting for
 * its workers to report themselves closed before the client goes down, which is what this
 * test measures end to end.
 * <p>
 * The applications are booted by the test rather than by {@code @SpringBootTest}, for the
 * same reason as in {@link Camunda8ShutdownDrainIT}: shutting one down is part of the
 * scenario, and Spring's test context is a cache which expects to own that lifecycle.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
public class Camunda8RestartDeliveryIT {

  /**
   * The lock of the job under test. A job swallowed by a parked activation request comes
   * back exactly this late, so it is also the number the assertion is measured against.
   */
  private static final Duration JOB_TIMEOUT = Duration.ofSeconds(20);

  /**
   * How long the second application waits before it starts. It has to stay below the
   * client's {@code request-timeout} of ten seconds, because that is how long an
   * activation request of the closed application can outlive it. The blueprint which
   * found this took 7,4 seconds.
   */
  private static final Duration GAP = Duration.ofSeconds(5);

  /**
   * What the first job may take before the test calls it a delivery which waited for the
   * lock. Far above the milliseconds a healthy delivery needs and far below the lock.
   */
  private static final Duration DELIVERED_IN_SECONDS = Duration.ofSeconds(8);

  @Container
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.standaloneBroker();

  private ConfigurableApplicationContext application;

  private static String restAddress() {

    return "http://"
        + CAMUNDA.getHost()
        + ":"
        + CAMUNDA.getMappedPort(8080);

  }

  private static String grpcAddress() {

    return "http://"
        + CAMUNDA.getHost()
        + ":"
        + CAMUNDA.getMappedPort(26500);

  }

  private ConfigurableApplicationContext boot() {

    return new SpringApplicationBuilder(DockerTestApplication.class)
        .run(
            "--spring.config.name=camunda8-it",
            "--spring.main.web-application-type=none",
            "--vanillabp.adapters.c8.rest-address="
                + restAddress(),
            "--vanillabp.adapters.c8.grpc-address="
                + grpcAddress(),
            "--vanillabp.workflow-modules.test-app.workflows.RestartProcess.adapters.c8.job-timeout="
                + JOB_TIMEOUT);

  }

  @AfterEach
  public void closeWhatIsLeft() {

    if ((application != null) && application.isActive()) {
      application.close();
    }
    RestartDockerWorkflowService.reset();

  }

  private <T> T bean(
      final Class<T> type) {

    return application.getBean(type);

  }

  private void startWorkflow() {

    bean(TransactionTemplate.class)
        .executeWithoutResult(status -> bean(RestartDockerWorkflowService.class).startWorkflow());

  }

  /**
   * Puts the measurement where it can be read after a build: into the test report of the
   * runner respectively an IDE, and into a file of its own, because the console output of
   * a test which passes is suppressed.
   *
   * @param reporter The JUnit reporter
   * @param measurement What was measured
   */
  private static void report(
      final TestReporter reporter,
      final String measurement) {

    reporter.publishEntry("story-102-restart-delivery", measurement);
    try {
      Files.writeString(
          Path.of("target", "story-102-restart-delivery.txt"),
          measurement + System.lineSeparator());
    } catch (final IOException e) {
      throw new UncheckedIOException("Cannot write down what was measured", e);
    }

  }

  @Test
  @DisplayName("A workflow started right after a restart gets its first job in milliseconds")
  public void aWorkflowStartedAfterARestartIsServedRightAway(
      final CapturedOutput output,
      final TestReporter reporter) throws Exception {

    // the first application, which works one workflow so its workers have an activation
    // request parked at the cluster by the time it is closed
    RestartDockerWorkflowService.reset();
    application = boot();
    startWorkflow();
    assertTrue(
        RestartDockerWorkflowService.SERVED.await(1, TimeUnit.MINUTES),
        "the first application served its own workflow");

    final var shutdownStartedAt = System.nanoTime();
    application.close();
    final var shutdownMillis = (System.nanoTime() - shutdownStartedAt) / 1_000_000;

    TimeUnit.MILLISECONDS.sleep(GAP.toMillis());

    // and the second one, whose worker is open before the workflow exists
    RestartDockerWorkflowService.reset();
    application = boot();
    final var startedAt = System.nanoTime();
    startWorkflow();
    final var served = RestartDockerWorkflowService.SERVED.await(
        JOB_TIMEOUT.multipliedBy(2).toSeconds(),
        TimeUnit.SECONDS);
    final var deliveredAfterMillis = (RestartDockerWorkflowService.SERVED_AT.get() - startedAt) / 1_000_000;

    // written down rather than printed: the output of a test which passes is suppressed,
    // and this number is what the story asked for
    report(
        reporter,
        "the first job of a workflow started %s after a restart was delivered after %d ms (job timeout %s, the shutdown of the first application took %d ms)"
            .formatted(GAP, deliveredAfterMillis, JOB_TIMEOUT, shutdownMillis));

    assertTrue(served, "the job reached the handler at all");
    assertTrue(
        deliveredAfterMillis < DELIVERED_IN_SECONDS.toMillis(),
        "the first job of the restarted application was delivered in seconds rather than in a job timeout (was "
            + deliveredAfterMillis
            + " ms, the lock is "
            + JOB_TIMEOUT
            + ")");

    // and the shutdown said what it did: the workers were closed and the cluster released
    // them, which is the reason the number above is what it is
    final var logged = output.getOut() + output.getErr();
    final var drainedAt = logged.indexOf("no activation request of theirs left at the cluster");
    final var clientClosedAt = logged.indexOf("Closing Camunda 8 client");
    assertTrue(drainedAt >= 0, "the drain reported a module which is quiet: "
        + logged);
    assertTrue(clientClosedAt >= 0, "and the client was closed: "
        + logged);
    assertTrue(
        drainedAt < clientClosedAt,
        "the workers of the module were closed and released BEFORE its client went down");
    assertFalse(
        logged.contains("still holding an activation request at the cluster"),
        "and nothing was left parked when the client was closed: "
            + logged);
    assertFalse(
        logged.contains("did not stop workflow processing"),
        "the ordinary Spring Boot shutdown reaches the adapter, so the backstop of the client factory stays "
            + "silent: "
            + logged);

  }

}
