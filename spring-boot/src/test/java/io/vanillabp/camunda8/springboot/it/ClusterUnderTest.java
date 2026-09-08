package io.vanillabp.camunda8.springboot.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Properties;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
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
 * a cluster which cannot be searched ends the boot of the application under test. WHERE
 * that storage lives is the second value the same properties file carries, see
 * {@code camunda8.cluster.secondary-storage} of the parent POM, and it belongs to the
 * release line as much as the image does: a line whose cluster can keep the storage in a
 * database of its own process starts one container per test class, an older line starts
 * an Elasticsearch beside it. A test class sees neither, it asks for a cluster and gets
 * one - see decision 22 in the repository's DECISIONS.md.
 */
public final class ClusterUnderTest {

  private static final String RESOURCE = "/camunda8-cluster.properties";

  private static final Properties PROPERTIES = read();

  private static final String IMAGE = property("cluster.image");

  /**
   * What {@code camunda.data.secondary-storage.type} of the cluster is set to. Only the
   * two values the tests know are expected here: {@code rdbms}, which the cluster serves
   * from an embedded H2 inside its own container, and {@code elasticsearch}, which needs
   * a container of its own.
   */
  private static final String SECONDARY_STORAGE = property("cluster.secondary-storage");

  private static final String ELASTICSEARCH = "elasticsearch";

  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

  private ClusterUnderTest() {
    // static helper
  }

  /**
   * @return The image of the cluster under test, e.g. {@code camunda/camunda:8.9.18}.
   */
  public static DockerImageName image() {

    return DockerImageName.parse(IMAGE);

  }

  /**
   * The cluster of a test, ready to be searched.
   * <p>
   * The readiness probe turns UP only once the partition leader accepts deployments,
   * which avoids a transient 503 on the first deploy at startup.
   *
   * @return A container to be used as a Testcontainers {@code @Container} field.
   */
  public static GenericContainer<?> cluster() {

    return newCluster("cluster")
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
   * @return A container to be used as a Testcontainers {@code @Container} field.
   */
  public static GenericContainer<?> withAuthentication() {

    return newCluster("authenticated")
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

  private static ClusterContainer newCluster(
      final String logName) {

    final var container = new ClusterContainer(image())
        .withLogConsumer(ClusterLog.of(logName))
        .withExposedPorts(8080, 26500, 9600)
        .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", SECONDARY_STORAGE);
    return SECONDARY_STORAGE.equals(ELASTICSEARCH)
        ? container.exportingToAnElasticsearchOfItsOwn()
        : container.withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_RDBMS_URL", H2_URL)
            .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_RDBMS_USERNAME", H2_USER)
            .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_RDBMS_PASSWORD", H2_PASSWORD);

  }

  /**
   * The database the cluster keeps its secondary storage in where the release line has
   * one: an H2 in the memory of the cluster's own JVM, whose driver the image ships. It
   * outlives every connection ({@code DB_CLOSE_DELAY=-1}) and dies with the container,
   * which is what a test class wants - the cluster of the next class starts empty
   * without anybody deleting anything.
   */
  private static final String H2_URL = "jdbc:h2:mem:camunda;DB_CLOSE_DELAY=-1";

  /**
   * @see #H2_URL
   */
  private static final String H2_USER = "sa";

  /**
   * @see #H2_URL
   */
  private static final String H2_PASSWORD = "sa";

  /**
   * A cluster which takes its Elasticsearch along where it needs one.
   * <p>
   * Testcontainers starts what a container depends on, but it stops only what a test
   * class declared, and an Elasticsearch nobody declared would then outlive its cluster:
   * a module of twenty classes would hold twenty of them by the end of its run, each with
   * its own heap. So the cluster stops the storage it brought, and the test class keeps
   * the one field it asked for.
   */
  private static final class ClusterContainer extends GenericContainer<ClusterContainer> {

    private Network network;

    private GenericContainer<?> elasticsearch;

    private ClusterContainer(
        final DockerImageName image) {

      super(image);

    }

    private ClusterContainer exportingToAnElasticsearchOfItsOwn() {

      network = Network.newNetwork();
      elasticsearch = new GenericContainer<>(
          DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.17.0"))
          .withNetwork(network)
          .withNetworkAliases(ELASTICSEARCH)
          .withEnv("discovery.type", "single-node")
          .withEnv("xpack.security.enabled", "false")
          .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
          .withExposedPorts(9200)
          .waitingFor(Wait
              .forHttp("/_cluster/health")
              .forPort(9200)
              .forStatusCode(200)
              .withStartupTimeout(STARTUP_TIMEOUT));
      return withNetwork(network)
          .dependsOn(elasticsearch)
          .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_ELASTICSEARCH_URL", "http://elasticsearch:9200");

    }

    @Override
    public void stop() {

      try {
        super.stop();
      } finally {
        if (elasticsearch != null) {
          elasticsearch.stop();
          network.close();
        }
      }

    }

  }

  private static Properties read() {

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
    return properties;

  }

  private static String property(
      final String name) {

    final var value = PROPERTIES.getProperty(name);
    if ((value == null) || value.isBlank() || value.contains("${")) {
      throw new IllegalStateException(
          "'%s' of '%s' is '%s' instead of a value. The test resources of this module have to be filtered: check the 'testResources' section of the module's pom.xml and the properties 'camunda8.cluster.image' and 'camunda8.cluster.secondary-storage' of the parent pom."
              .formatted(name, RESOURCE, value));
    }
    return value;

  }

}
