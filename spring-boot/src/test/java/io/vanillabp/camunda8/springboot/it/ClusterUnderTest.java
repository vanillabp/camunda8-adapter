package io.vanillabp.camunda8.springboot.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Properties;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

/**
 * The Camunda 8 cluster the integration tests of this module run against.
 * <p>
 * The image is not written into the tests. It is filtered into
 * {@code camunda8-cluster.properties} at build time from the Camunda client the active
 * release line pins ({@code camunda8.version}), so activating another line moves the
 * client and the cluster together. That is what makes the supported cluster versions of
 * the README provable instead of claimed: a line's tests meet the oldest cluster its
 * artifacts accept (story 53).
 * <p>
 * Override for a single run with {@code mvn verify -Dcamunda8.cluster.image=...}. The
 * property is resolved while the test resources are filtered, so a run from the IDE uses
 * whatever the last Maven build wrote.
 * <p>
 * The image is {@code camunda/camunda}, the orchestration cluster of Camunda 8, and not
 * the older {@code camunda/zeebe}: the latter received no tags beyond 8.9.11 and none at
 * all for 8.10, so a per-line matrix cannot be built on it. The orchestration cluster
 * boots the whole application, which without secondary storage fails on its own
 * authentication ("Basic Authentication is not supported when secondary storage is
 * disabled"). The Spring profile {@code broker} is what the tests here need: broker plus
 * gateway plus the v2 API, and no user data.
 */
public final class ClusterUnderTest {

  private static final String RESOURCE = "/camunda8-cluster.properties";

  private static final String IMAGE = readImage();

  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

  private ClusterUnderTest() {
    // static helper
  }

  /**
   * @return The image of the cluster under test, e.g. {@code camunda/camunda:8.8.31}.
   */
  public static DockerImageName image() {

    return DockerImageName.parse(IMAGE);

  }

  /**
   * A cluster WITHOUT secondary storage: the query API is unavailable, which is what the
   * adapter's optimistic fallbacks are tested against.
   *
   * @return A container to be used as a Testcontainers {@code @Container} field.
   */
  public static GenericContainer<?> standaloneBroker() {

    return new GenericContainer<>(image())
        .withExposedPorts(8080, 26500, 9600)
        .withEnv("SPRING_PROFILES_ACTIVE", "broker")
        .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "none")
        // an unprotected API keeps an authentication provider out of the test
        .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
        // the readiness probe turns UP only once the partition leader accepts
        // deployments, which avoids a transient 503 on the first deploy at startup
        .waitingFor(Wait
            .forHttp("/actuator/health/readiness")
            .forPort(9600)
            .forStatusCode(200)
            .withStartupTimeout(STARTUP_TIMEOUT));

  }

  /**
   * A cluster WITH secondary storage, exporting into an Elasticsearch reachable in
   * {@code network} under the alias {@code elasticsearch}. Needed by every test using the
   * query API (workflow awareness, the viewer, process versions).
   *
   * @param network      The network shared with the Elasticsearch container.
   * @param elasticsearch The Elasticsearch container, started first.
   * @return A container to be used as a Testcontainers {@code @Container} field.
   */
  public static GenericContainer<?> withSecondaryStorage(
      final Network network,
      final Startable elasticsearch) {

    return new GenericContainer<>(image())
        .withNetwork(network)
        .dependsOn(elasticsearch)
        .withExposedPorts(8080, 26500, 9600)
        .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "elasticsearch")
        .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_ELASTICSEARCH_URL", "http://elasticsearch:9200")
        .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
        .waitingFor(Wait
            .forHttp("/actuator/health/readiness")
            .forPort(9600)
            .forStatusCode(200)
            .withStartupTimeout(STARTUP_TIMEOUT));

  }

  private static String readImage() {

    final var properties = new Properties();
    try (var resource = ClusterUnderTest.class.getResourceAsStream(RESOURCE)) {
      if (resource == null) {
        throw new IllegalStateException(
            "'%s' is missing from the test classpath. Maven filters it, so build the module once ('mvn test-compile') before running an integration test from the IDE."
                .formatted(RESOURCE));
      }
      properties.load(resource);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read '%s'".formatted(RESOURCE), e);
    }

    final var image = properties.getProperty("cluster.image");
    if ((image == null) || image.isBlank() || image.contains("${")) {
      throw new IllegalStateException(
          "'cluster.image' of '%s' is '%s' instead of an image. The test resources of this module have to be filtered: check the 'testResources' section of the module's pom.xml and the property 'camunda8.cluster.image' of the parent pom."
              .formatted(RESOURCE, image));
    }
    return image;

  }

}
