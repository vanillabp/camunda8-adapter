package io.vanillabp.camunda8.springboot.election;

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
 * The Camunda 8 cluster the two election tests of this module run against - the same
 * mechanism the Spring Boot module's {@code ClusterUnderTest} and the Quarkus module's
 * {@code ClusterImage} use, kept local like theirs: the image is filtered into
 * {@code camunda8-cluster.properties} at build time from the Camunda client the active
 * release line pins ({@code camunda8.version}), so activating another line moves the
 * client and the cluster together.
 * <p>
 * Two flavours, which is all these tests need: a cluster WITHOUT secondary storage, where
 * two adapter ids sharing it cannot be told apart and the boot has to say so, and one WITH
 * it, where the election finds the workflow of the older scope.
 */
public final class ElectionCluster {

  private static final String RESOURCE = "/camunda8-cluster.properties";

  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

  private ElectionCluster() {
    // static helper
  }

  /**
   * @return The image of the cluster under test, e.g. {@code camunda/camunda:8.8.31}
   */
  public static DockerImageName image() {

    final var properties = new Properties();
    try (var resource = ElectionCluster.class.getResourceAsStream(RESOURCE)) {
      if (resource == null) {
        throw new IllegalStateException(
            "'%s' is missing from the test classpath. Maven filters it, so build the module once ('mvn test-compile') before running an integration test from the IDE."
                .formatted(RESOURCE));
      }
      properties.load(resource);
    } catch (final IOException e) {
      throw new UncheckedIOException("Cannot read '%s'".formatted(RESOURCE), e);
    }

    final var image = properties.getProperty("cluster.image");
    if ((image == null) || image.isBlank() || image.contains("${")) {
      throw new IllegalStateException(
          "'cluster.image' of '%s' is '%s' instead of an image. The test resources of this module have to be filtered: check the 'testResources' section of the module's pom.xml and the property 'camunda8.cluster.image' of the parent pom."
              .formatted(RESOURCE, image));
    }
    return DockerImageName.parse(image);

  }

  /**
   * A cluster WITHOUT secondary storage: the query API is unavailable, so two adapter ids
   * of one cluster cannot be told apart and the boot ends saying so.
   *
   * @return A container to be used as a Testcontainers {@code @Container} field
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
   * {@code network} under the alias {@code elasticsearch} - what the election needs to map
   * a key to the scope it belongs to.
   *
   * @param network The network shared with the Elasticsearch container
   * @param elasticsearch The Elasticsearch container, started first
   * @return A container to be used as a Testcontainers {@code @Container} field
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

}
