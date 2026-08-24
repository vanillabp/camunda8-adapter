package io.vanillabp.camunda8.quarkus.nativeimage.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Runs the native binary of this module against a real Camunda 8 cluster.
 * <p>
 * The image was built by the Quarkus plugin in the <code>package</code> phase, which is
 * why this is an integration test: at <code>test</code> time there is no binary yet. And
 * a binary which was built is not a binary which runs - the whole reason this module
 * exists is a deployment which parses BPMN, which the image could not do until the
 * adapter's extension registered what the parser reads. So the test starts the binary,
 * gives it a cluster and reads its exit code; the application's own main decides what
 * that code is (see {@code NativeImageApplication}).
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8NativeImageIT {

  /**
   * How long the binary may take to boot, deploy, run its workflow and exit. Its own
   * main waits two minutes for the job, so this is that plus room for the boot.
   */
  private static final Duration UNTIL_EXITED = Duration.ofMinutes(4);

  private static final Duration CONTAINER_STARTUP = Duration.ofMinutes(5);

  private static final Network NETWORK = Network.newNetwork();

  private static final GenericContainer<?> ELASTICSEARCH = new GenericContainer<>(
      DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.17.0"))
      .withNetwork(NETWORK)
      .withNetworkAliases("elasticsearch")
      .withEnv("discovery.type", "single-node")
      .withEnv("xpack.security.enabled", "false")
      .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
      .withExposedPorts(9200)
      .waitingFor(Wait
          .forHttp("/_cluster/health")
          .forPort(9200)
          .forStatusCode(200)
          .withStartupTimeout(CONTAINER_STARTUP));

  private static final GenericContainer<?> CAMUNDA = new GenericContainer<>(ClusterImage.of())
      .withNetwork(NETWORK)
      .withExposedPorts(8080, 9600)
      .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "elasticsearch")
      .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_ELASTICSEARCH_URL", "http://elasticsearch:9200")
      // an unprotected API keeps an authentication provider out of this test - what
      // credentials reaching the cluster look like has tests of its own
      .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
      // the readiness probe turns UP only once the partition leader accepts
      // deployments, which avoids a transient 503 on the first deploy at startup
      .waitingFor(Wait
          .forHttp("/actuator/health/readiness")
          .forPort(9600)
          .forStatusCode(200)
          .withStartupTimeout(CONTAINER_STARTUP));

  @Test
  @DisplayName("The native binary boots, deploys its workflow module and runs a workflow")
  public void theNativeBinaryRunsAWorkflow() throws Exception {

    final var binary = Path.of(System.getProperty("native.image.runner"));
    assertTrue(
        Files.isExecutable(binary),
        () -> """
            '%s' is not an executable binary. This test runs what the native build \
            produced, so it needs the profile which builds it: \
            'mvn -Dnative -pl quarkus/native-image-tests verify'."""
            .formatted(binary));

    ELASTICSEARCH.start();
    CAMUNDA.start();
    try {
      final var output = runBinary(binary);
      assertEquals(
          0,
          output.exitCode(),
          () -> "The native binary exited with %d. Its output was:%n%s"
              .formatted(output.exitCode(), output.text()));
      assertTrue(
          output
              .text()
              .contains("status 'served'"),
          () -> "The native binary exited successfully without saying it ran the workflow:%n%s"
              .formatted(output.text()));
    } finally {
      CAMUNDA.stop();
      ELASTICSEARCH.stop();
    }

  }

  /**
   * What the binary printed and what it returned. Its output is the only report a
   * failing run leaves behind, so it travels with the exit code rather than into the
   * void.
   *
   * @param exitCode The exit code of the binary
   * @param text Everything it wrote to stdout and stderr
   */
  private record BinaryOutput(int exitCode, String text) {
  }

  private BinaryOutput runBinary(
      final Path binary) throws IOException, InterruptedException {

    final var log = binary
        .resolveSibling("native-image-application.log");
    final var process = new ProcessBuilder(binary.toString())
        // only the test knows the port Testcontainers mapped
        .redirectErrorStream(true)
        .redirectOutput(log.toFile())
        .directory(binary
            .getParent()
            .toFile());
    process
        .environment()
        .put(
            "CAMUNDA_REST_ADDRESS",
            "http://%s:%d".formatted(CAMUNDA.getHost(), CAMUNDA.getMappedPort(8080)));

    final var running = process.start();
    if (!running.waitFor(UNTIL_EXITED.toSeconds(), TimeUnit.SECONDS)) {
      running.destroyForcibly();
      return new BinaryOutput(
          -1, "The binary did not exit within %s. What it logged until then:%n%s"
              .formatted(UNTIL_EXITED, Files.readString(log)));
    }
    return new BinaryOutput(running.exitValue(), Files.readString(log));

  }

}
