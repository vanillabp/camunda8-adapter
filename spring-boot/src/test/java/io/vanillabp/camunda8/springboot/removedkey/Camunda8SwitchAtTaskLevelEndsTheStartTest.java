package io.vanillabp.camunda8.springboot.removedkey;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

import io.vanillabp.camunda8.springboot.refusedstart.RefusedStartTestApplication;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * <code>allow-connectors</code> and <code>allow-listeners</code> are not read at task level. A
 * value set there ends the start, and the message names the key and the levels where it works.
 * The start ends before the adapter talks to a cluster. So this test needs no cluster.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8SwitchAtTaskLevelEndsTheStartTest {

  private static final String TASK_LEVEL = "vanillabp.workflow-modules.test-app.workflows.RefusedStartProcess.tasks.someTask.adapters.c8.";

  @Test
  @DisplayName("allow-connectors at task level ends the start, and the message says where the key works")
  public void allowConnectorsAtTaskLevelEndsTheStart() {

    final var message = messageOfAStartWith(TASK_LEVEL
        + "allow-connectors=true");

    assertTrue(
        message.contains("Camunda 8 adapter 'c8' has 'allow-connectors' set at TASK level, where nothing reads it"),
        "expected the guiding startup failure but got:\n"
            + message);
    assertTrue(message.contains(TASK_LEVEL
        + "allow-connectors"), message);
    assertTrue(message.contains("vanillabp.workflow-modules.<m>.workflows.<w>.adapters.c8.allow-connectors"), message);

  }

  @Test
  @DisplayName("allow-listeners at task level ends the start, and the message says where the key works")
  public void allowListenersAtTaskLevelEndsTheStart() {

    final var message = messageOfAStartWith(TASK_LEVEL
        + "allow-listeners=true");

    assertTrue(
        message.contains("Camunda 8 adapter 'c8' has 'allow-listeners' set at TASK level, where nothing reads it"),
        "expected the guiding startup failure but got:\n"
            + message);
    assertTrue(message.contains(TASK_LEVEL
        + "allow-listeners"), message);
    assertTrue(message.contains("vanillabp.workflow-modules.<m>.workflows.<w>.adapters.c8.allow-listeners"), message);

  }

  private static String messageOfAStartWith(
      final String property) {

    final var failure = assertThrows(
        RuntimeException.class,
        () -> new SpringApplicationBuilder(RefusedStartTestApplication.class)
            .web(WebApplicationType.NONE)
            .properties(
                "spring.config.name=camunda8-refused-start-it",
                property)
            .run()
            .close());

    final var causes = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      causes
          .append(cause.getMessage())
          .append('\n');
    }
    return causes.toString();

  }

}
