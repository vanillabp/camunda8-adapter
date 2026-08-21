package io.vanillabp.camunda8.quarkus.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

import org.testcontainers.utility.DockerImageName;

/**
 * The image of the Camunda 8 cluster the integration tests of this module run against.
 * <p>
 * It is filtered into {@code camunda8-cluster.properties} at build time from the Camunda
 * client the active release line pins ({@code camunda8.version}), the same mechanism the
 * Spring Boot module uses: activating another line moves the client and the cluster
 * together, so a line's tests meet the oldest cluster its artifacts accept (story 53).
 */
public final class ClusterImage {

  private static final String RESOURCE = "/camunda8-cluster.properties";

  private ClusterImage() {
    // static helper
  }

  /**
   * @return The image of the cluster under test, e.g. {@code camunda/camunda:8.9.16}
   */
  public static DockerImageName of() {

    final var properties = new Properties();
    try (var resource = ClusterImage.class.getResourceAsStream(RESOURCE)) {
      if (resource == null) {
        throw new IllegalStateException(
            "'%s' is missing from the test classpath. Maven filters it, so build the module once ('mvn test-compile') before running this test from the IDE."
                .formatted(RESOURCE));
      }
      properties.load(resource);
    } catch (final IOException e) {
      throw new UncheckedIOException("Cannot read '%s'".formatted(RESOURCE), e);
    }
    final var image = properties.getProperty("cluster.image");
    if ((image == null) || image.isBlank() || image.contains("${")) {
      throw new IllegalStateException(
          "'cluster.image' is '%s' instead of an image - the test resources of this module have to be filtered."
              .formatted(image));
    }
    return DockerImageName.parse(image);

  }

}
