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
 * artifacts accept.
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
        .withLogConsumer(ClusterLog.of("broker"))
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
        .withLogConsumer(ClusterLog.of("secondary-storage"))
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

  /**
   * The user the authenticated cluster is initialized with, and its password. Both are
   * test values and deliberately visible: what matters here is that they REACH the
   * cluster, not that they are secret.
   */
  public static final String USERNAME = "demo";

  /**
   * @see #USERNAME
   */
  public static final String PASSWORD = "demo";

  /**
   * A cluster with its authentication SWITCHED ON - what a self-managed installation
   * normally looks like, and what every other cluster here deliberately is not.
   * Secondary storage comes with it: the orchestration cluster refuses basic
   * authentication without it ("Basic Authentication is not supported when secondary
   * storage is disabled"), so an authenticated cluster is an Elasticsearch cluster.
   *
   * @param network      The network shared with the Elasticsearch container.
   * @param elasticsearch The Elasticsearch container, started first.
   * @return A container to be used as a Testcontainers {@code @Container} field.
   */
  public static GenericContainer<?> withAuthentication(
      final Network network,
      final Startable elasticsearch) {

    return new GenericContainer<>(image())
        .withLogConsumer(ClusterLog.of("authenticated"))
        .withNetwork(network)
        .dependsOn(elasticsearch)
        .withExposedPorts(8080, 26500, 9600)
        .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "elasticsearch")
        .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_ELASTICSEARCH_URL", "http://elasticsearch:9200")
        // no UNPROTECTEDAPI here: every request has to carry credentials
        .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_METHOD", "BASIC")
        .withEnv("CAMUNDA_SECURITY_AUTHORIZATIONS_ENABLED", "true")
        .withEnv("CAMUNDA_SECURITY_INITIALIZATION_USERS_0_USERNAME", USERNAME)
        .withEnv("CAMUNDA_SECURITY_INITIALIZATION_USERS_0_PASSWORD", PASSWORD)
        .withEnv("CAMUNDA_SECURITY_INITIALIZATION_USERS_0_NAME", "Demo")
        .withEnv("CAMUNDA_SECURITY_INITIALIZATION_USERS_0_EMAIL", "demo@example.org")
        .withEnv("CAMUNDA_SECURITY_INITIALIZATION_DEFAULTROLES_ADMIN_USERS_0", USERNAME)
        // a ready cluster is not yet a cluster which knows this user: the readiness probe
        // answers before the initialization created it, and the first request of the
        // application then gets a 401 it cannot do anything with. So the second condition
        // asks an API which demands authentication, with the credentials the tests use
        .waitingFor(new org.testcontainers.containers.wait.strategy.WaitAllStrategy()
            .withStrategy(Wait
                .forHttp("/actuator/health/readiness")
                .forPort(9600)
                .forStatusCode(200)
                .withStartupTimeout(STARTUP_TIMEOUT))
            .withStrategy(Wait
                .forHttp("/v2/topology")
                .forPort(8080)
                .withBasicCredentials(USERNAME, PASSWORD)
                .forStatusCode(200)
                .withStartupTimeout(STARTUP_TIMEOUT))
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
