package io.vanillabp.camunda8.quarkus.it;

import static io.vanillabp.integration.test.utils.TestCoverageUtils.testCoverageJavaAgent;
import static io.vanillabp.integration.test.utils.TestJvmArgs.quarkusProdModeTestDefaults;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.quarkus.test.QuarkusProdModeTest;
import io.restassured.RestAssured;
import io.vanillabp.integration.test.utils.FreePortUtil;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The Quarkus half: the workers of a workflow module are closed before the
 * client on this platform too, and a workflow started right after a restart gets its
 * first job in milliseconds rather than in a job timeout.
 * <p>
 * Spring Boot's {@code Camunda8RestartDeliveryIT} measures the same thing, and the
 * duplication is the point. The order the two lifecycles produce is a property of the
 * platform's glue and not of the adapter's neutral core: Spring stops its
 * {@code SmartLifecycle} beans before it destroys them, Quarkus fires its
 * {@code ShutdownEvent} before it disposes the client's producer, and only running it
 * shows that either of them reaches the adapter.
 * <p>
 * The application is stopped and started again INSIDE the test method rather than by the
 * extension, which is what makes a restart observable in a prod-mode test at all. Between
 * the two runs the log of the forked application says which came first, the drain or the
 * closing client.
 * <p>
 * It brings a cluster of its own, which the lifecycle test deliberately avoids for every
 * other feature. Two reasons make it worth the container here: this scenario owns the
 * application's lifecycle, and delivering a job needs no secondary storage, so this one is
 * a broker alone rather than the pair the query API costs. Testcontainers removes it when
 * the JVM of the test run exits.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
public class Camunda8RestartDeliveryTest {

  /**
   * The lock of the job under test, as {@code c8-restart/application.yaml} configures it.
   * A job swallowed by an activation request which outlived its client comes back exactly
   * this late.
   */
  private static final Duration JOB_TIMEOUT = Duration.ofSeconds(20);

  /**
   * How long the application stays down. Below the client's {@code request-timeout} of
   * ten seconds, because that is how long an activation request of the stopped
   * application can outlive it.
   */
  private static final Duration GAP = Duration.ofSeconds(5);

  /**
   * What the first job may take before the test calls it a delivery which waited for the
   * lock.
   */
  private static final Duration DELIVERED_IN_SECONDS = Duration.ofSeconds(8);

  private static final Duration CONTAINER_STARTUP = Duration.ofMinutes(5);

  private static final Path LOG_FILE = Path
      .of("target", "c8-restart-application.log")
      .toAbsolutePath();

  /**
   * A cluster without secondary storage: delivering a job needs no query API, and this
   * test is about nothing else.
   */
  static final GenericContainer<?> CAMUNDA = new GenericContainer<>(ClusterImage.of())
      .withLogConsumer(ClusterLog.of("restart-cluster"))
      .withExposedPorts(8080, 26500, 9600)
      .withEnv("SPRING_PROFILES_ACTIVE", "broker")
      .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "none")
      .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
      .waitingFor(Wait
          .forHttp("/actuator/health/readiness")
          .forPort(9600)
          .forStatusCode(200)
          .withStartupTimeout(CONTAINER_STARTUP));

  /*
   * Started here rather than by the Testcontainers extension: the application's runtime
   * properties need the mapped ports, and they are read while the field below is
   * initialized.
   */
  static {
    CAMUNDA.start();
  }

  private static final int HTTP_PORT = FreePortUtil.getFreePort();

  @RegisterExtension
  static final QuarkusProdModeTest prodModeTest = new QuarkusProdModeTest()
      .withApplicationRoot(jar -> jar
          .addPackage("io.vanillabp.camunda8.quarkus.test.restart")
          .addAsResource("c8-restart/application.yaml", "application.yaml")
          .addAsResource("c8-restart/processes/restart.bpmn", "c8-restart/processes/restart.bpmn")
          .addAsResource("workflow-module-descriptor/workflow-module", "META-INF/workflow-module"))
      .setJVMArgs(testCoverageJavaAgent(quarkusProdModeTestDefaults()))
      .setRun(true)
      .setRuntimeProperties(Map
          .of(
              "quarkus.http.port",
              Integer.toString(HTTP_PORT),
              "vanillabp.adapters.c8.rest-address",
              "http://%s:%d".formatted(CAMUNDA.getHost(), CAMUNDA.getMappedPort(8080)),
              "vanillabp.adapters.c8.grpc-address",
              "http://%s:%d".formatted(CAMUNDA.getHost(), CAMUNDA.getMappedPort(26500)),
              // the application runs in a forked JVM, and its log is where the order of
              // its shutdown can be read
              "quarkus.log.file.enable",
              "true",
              "quarkus.log.file.path",
              LOG_FILE.toString()));

  private static void startWorkflow() {

    RestAssured
        .given()
        .baseUri("http://localhost")
        .port(HTTP_PORT)
        .post("/restart/start")
        .then()
        .statusCode(204);

  }

  /**
   * @return How long the first job of the last started workflow took, or -1 while it has
   *         not been delivered yet
   */
  @SuppressWarnings("unchecked")
  private static long deliveredAfterMillis() {

    final var delivery = RestAssured
        .given()
        .baseUri("http://localhost")
        .port(HTTP_PORT)
        .get("/restart/delivery")
        .then()
        .statusCode(200)
        .extract()
        .as(Map.class);
    return ((Number) ((Map<String, Object>) delivery).get("millis")).longValue();

  }

  private static long awaitDelivery(
      final Duration atMost) throws InterruptedException {

    final var deadline = System.nanoTime() + atMost.toNanos();
    while (System.nanoTime() < deadline) {
      final var millis = deliveredAfterMillis();
      if (millis >= 0) {
        return millis;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    return -1;

  }

  private static String applicationLog() {

    try {
      return Files.readString(LOG_FILE);
    } catch (final IOException e) {
      throw new UncheckedIOException("Cannot read the log of the application under test", e);
    }

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

    reporter.publishEntry("restart-delivery", measurement);
    try {
      Files.writeString(
          Path.of("target", "restart-delivery.txt"),
          measurement + System.lineSeparator());
    } catch (final IOException e) {
      throw new UncheckedIOException("Cannot write down what was measured", e);
    }

  }

  @Test
  @DisplayName("The workers are closed before the client, and the workflow after the restart is served right away")
  public void aWorkflowStartedAfterARestartIsServedRightAway(
      final TestReporter reporter) throws Exception {

    // the first run, which works one workflow so its workers have an activation request
    // parked at the cluster by the time the application stops
    startWorkflow();
    assertTrue(
        awaitDelivery(Duration.ofMinutes(1)) >= 0,
        "the first run served its own workflow");

    prodModeTest.stop();

    final var shutdownLog = applicationLog();
    final var drainedAt = shutdownLog.indexOf("no activation request of theirs left at the cluster");
    final var clientClosedAt = shutdownLog.indexOf("Closing Camunda 8 client");
    assertTrue(drainedAt >= 0, "the drain reported a module which is quiet: "
        + shutdownLog);
    assertTrue(clientClosedAt >= 0, "and the client was closed: "
        + shutdownLog);
    assertTrue(
        drainedAt < clientClosedAt,
        "the workers of the module were closed and released BEFORE its client went down");
    assertFalse(
        shutdownLog.contains("did not stop workflow processing"),
        "the Quarkus shutdown event reaches the adapter, so the backstop of the client factory stays silent: "
            + shutdownLog);

    TimeUnit.MILLISECONDS.sleep(GAP.toMillis());

    // and the second run, whose worker is open before the workflow exists
    prodModeTest.start();
    startWorkflow();
    final var deliveredAfterMillis = awaitDelivery(JOB_TIMEOUT.multipliedBy(2));

    // written down rather than printed: the output of a test which passes is suppressed,
    // and this number is the whole point of the test
    report(
        reporter,
        "the first job of a workflow started %s after a restart was delivered after %d ms (job timeout %s)"
            .formatted(GAP, deliveredAfterMillis, JOB_TIMEOUT));

    assertTrue(deliveredAfterMillis >= 0, "the job reached the handler at all");
    assertTrue(
        deliveredAfterMillis < DELIVERED_IN_SECONDS.toMillis(),
        "the first job of the restarted application was delivered in seconds rather than in a job timeout (was "
            + deliveredAfterMillis
            + " ms, the lock is "
            + JOB_TIMEOUT
            + ")");

  }

}
