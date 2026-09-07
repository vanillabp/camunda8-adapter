package io.vanillabp.camunda8.springboot.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Properties;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
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
 * all for 8.10, so a per-line matrix cannot be built on it.
 * <p>
 * Every cluster here brings secondary storage, because the adapter serves no other kind:
 * a cluster which cannot be searched ends the boot of the application under test. So
 * there is no flavour to choose any more, only whether the cluster's authentication is
 * switched on, and every class pays for an Elasticsearch beside its Zeebe.
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
   * The secondary storage of the cluster, reachable in {@code network} under the alias
   * {@code elasticsearch}. Every integration test class of this module declares one as a
   * {@code @Container} field of its own and hands it to {@link #cluster} or
   * {@link #withAuthentication}, which is what makes the pair start in the right order.
   *
   * @param network The network shared with the cluster container.
   * @return A container to be used as a Testcontainers {@code @Container} field.
   */
  public static GenericContainer<?> elasticsearch(
      final Network network) {

    return new GenericContainer<>(
        DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.17.0"))
        .withNetwork(network)
        .withNetworkAliases("elasticsearch")
        .withEnv("discovery.type", "single-node")
        .withEnv("xpack.security.enabled", "false")
        .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
        .withExposedPorts(9200)
        .waitingFor(Wait
            .forHttp("/_cluster/health")
            .forPort(9200)
            .forStatusCode(200)
            .withStartupTimeout(STARTUP_TIMEOUT));

  }

  /**
   * The cluster of a test, exporting into the Elasticsearch of {@link #elasticsearch}.
   * <p>
   * The readiness probe turns UP only once the partition leader accepts deployments,
   * which avoids a transient 503 on the first deploy at startup.
   *
   * @param network      The network shared with the Elasticsearch container.
   * @param elasticsearch The Elasticsearch container, started first.
   * @return A container to be used as a Testcontainers {@code @Container} field.
   */
  public static GenericContainer<?> cluster(
      final Network network,
      final Startable elasticsearch) {

    return new GenericContainer<>(image())
        .withLogConsumer(ClusterLog.of("cluster"))
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
   * normally looks like, and what every other cluster here deliberately is not, so that
   * an authentication provider stays out of the tests which are about something else.
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
        .waitingFor(new WaitAllStrategy()
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
