package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * A <code>&#64;WorkflowTask</code> method matching no task of any BPMN process of its
 * workflow module ends the boot, naming the method and the fix.
 * <p>
 * The check belongs to the platform's core - this adapter used to call it itself, and
 * Camunda 7 used to forget it. What this test adds is the proof that it still fires for
 * Camunda 8 now that nobody calls it here: the model reaches a REAL cluster, so the
 * deployment really finishes, which is the moment the core is waiting for. The cluster
 * needs no secondary storage for it; wiring a model and reporting a method are questions
 * of the model and the application, not of the query API.
 */
@ExtendWith(SuppressOutputExtension.class)
@Testcontainers(disabledWithoutDocker = true)
public class Camunda8OrphanMethodIT {

  @Container
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.standaloneBroker();

  @Test
  @DisplayName("A method matching no task ends the boot, naming the method and the fix")
  public void anOrphanMethodEndsTheBoot() {

    final var failure = assertThrows(
        RuntimeException.class,
        () -> new SpringApplicationBuilder(DockerTestApplication.class)
            .run(
                "--spring.config.name=camunda8-it",
                "--spring.profiles.active=orphan-method",
                "--vanillabp.adapters.c8.rest-address=http://%s:%d".formatted(
                    CAMUNDA.getHost(),
                    CAMUNDA.getMappedPort(8080)),
                "--vanillabp.adapters.c8.grpc-address=http://%s:%d".formatted(
                    CAMUNDA.getHost(),
                    CAMUNDA.getMappedPort(26500)),
                "--vanillabp.workflow-modules.test-app.adapters.c8.resources-location=classpath*:orphan-method")
            .close());

    final var message = rootMessage(failure);
    assertTrue(message.contains("orphanTypo"), message);
    assertTrue(message.contains("activityNobodyModelled"), message);
    assertTrue(message.contains("fix the annotation"), message);

  }

  private static String rootMessage(
      final Throwable throwable) {

    var cause = throwable;
    while ((cause.getCause() != null) && (cause.getCause() != cause)) {
      cause = cause.getCause();
    }
    return String.valueOf(cause.getMessage());

  }

}
