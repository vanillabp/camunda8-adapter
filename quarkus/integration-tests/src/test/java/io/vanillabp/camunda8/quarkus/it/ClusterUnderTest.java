package io.vanillabp.camunda8.quarkus.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Properties;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * The Camunda 8 cluster the integration tests of this module run against.
 * <p>
 * Image and secondary storage are filtered into {@code camunda8-cluster.properties} at
 * build time from the release line the build activated, the same mechanism the Spring
 * Boot module uses: activating another line moves the client and the cluster together, so
 * a line's tests meet the oldest cluster its artifacts accept.
 * <p>
 * Every cluster here brings secondary storage, because the adapter serves no other kind.
 * Where it lives belongs to the line: a cluster which can keep it in a database of its
 * own process is one container, an older line's cluster brings an Elasticsearch along and
 * stops it again with itself. A test asks for a cluster and gets one - see decision 22 in
 * the repository's DECISIONS.md.
 */
public final class ClusterUnderTest {

  private static final String RESOURCE = "/camunda8-cluster.properties";

  private static final Properties PROPERTIES = read();

  /**
   * What {@code camunda.data.secondary-storage.type} of the cluster is set to. Only the
   * two values these tests know are expected here: {@code rdbms}, which the cluster
   * serves from an embedded H2 inside its own container, and {@code elasticsearch}, which
   * needs a container of its own.
   */
  private static final String SECONDARY_STORAGE = property("cluster.secondary-storage");

  private static final String ELASTICSEARCH = "elasticsearch";

  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);

  private ClusterUnderTest() {
    // static helper
  }

  /**
   * @return The image of the cluster under test, e.g. {@code camunda/camunda:8.9.18}
   */
  public static DockerImageName image() {

    return DockerImageName.parse(property("cluster.image"));

  }

  /**
   * The cluster of a test, ready to be searched.
   *
   * @param logName How the cluster's own output is prefixed in the test's log
   * @return A container, to be started by whoever declared it
   */
  public static GenericContainer<?> cluster(
      final String logName) {

    final var container = new ClusterContainer(image())
        .withLogConsumer(ClusterLog.of(logName))
        .withExposedPorts(8080, 26500, 9600)
        .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", SECONDARY_STORAGE)
        // an unprotected API keeps an authentication provider out of these tests - what
        // credentials reaching the cluster look like has a test of its own
        .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
        // the readiness probe turns UP only once the partition leader accepts
        // deployments, which avoids a transient 503 on the first deploy at startup
        .waitingFor(Wait
            .forHttp("/actuator/health/readiness")
            .forPort(9600)
            .forStatusCode(200)
            .withStartupTimeout(STARTUP_TIMEOUT));
    return SECONDARY_STORAGE.equals(ELASTICSEARCH)
        ? container.exportingToAnElasticsearchOfItsOwn()
        : container.withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_RDBMS_URL", H2_URL)
            .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_RDBMS_USERNAME", H2_USER)
            .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_RDBMS_PASSWORD", H2_PASSWORD);

  }

  /**
   * The database the cluster keeps its secondary storage in where the release line has
   * one: an H2 in the memory of the cluster's own JVM, whose driver the image ships. It
   * outlives every connection ({@code DB_CLOSE_DELAY=-1}) and dies with the container.
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
   * A cluster which takes its Elasticsearch along where it needs one, and stops it again
   * with itself: Testcontainers starts what a container depends on, but stopping is left
   * to whoever holds the container.
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
            "'%s' is missing from the test classpath. Maven filters it, so build the module once ('mvn test-compile') before running this test from the IDE."
                .formatted(RESOURCE));
      }
      properties.load(resource);
    } catch (final IOException e) {
      throw new UncheckedIOException("Cannot read '%s'".formatted(RESOURCE), e);
    }
    return properties;

  }

  private static String property(
      final String name) {

    final var value = PROPERTIES.getProperty(name);
    if ((value == null) || value.isBlank() || value.contains("${")) {
      throw new IllegalStateException(
          "'%s' is '%s' instead of a value - the test resources of this module have to be filtered."
              .formatted(name, value));
    }
    return value;

  }

}
