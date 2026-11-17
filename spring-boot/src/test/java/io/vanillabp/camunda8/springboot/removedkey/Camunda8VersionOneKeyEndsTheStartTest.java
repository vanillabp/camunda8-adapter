package io.vanillabp.camunda8.springboot.removedkey;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

import io.vanillabp.camunda8.springboot.refusedstart.RefusedStartTestApplication;
import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * A key of the Camunda 8 adapter of VanillaBP 1 which this version does not read ends the start.
 * The message names each such key and what to write instead. The start ends before the adapter
 * talks to a cluster. So this test needs no cluster.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8VersionOneKeyEndsTheStartTest {

  private static final String TASK_TIMEOUT = "vanillabp.workflow-modules.test-app.workflows.RefusedStartProcess.adapters.c8.task-timeout";

  @Test
  @DisplayName("keys of version 1 end the start, and the message says what to write instead of each")
  public void keysOfVersionOneEndTheStart(
      final CapturedOutput output) {

    final var failure = assertThrows(
        RuntimeException.class,
        () -> new SpringApplicationBuilder(RefusedStartTestApplication.class)
            .web(WebApplicationType.NONE)
            .properties(
                "spring.config.name=camunda8-refused-start-it",
                TASK_TIMEOUT
                    + "=PT1M",
                "vanillabp.allow-connectors=true")
            .run()
            .close());

    final var causes = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      causes
          .append(cause.getMessage())
          .append('\n');
    }
    final var message = causes.toString();
    assertTrue(
        message.contains("The Camunda 8 adapter does not read these keys of VanillaBP 1"),
        "expected the guiding startup failure but got:\n"
            + message);
    assertTrue(
        message.contains("  - '%s': It was renamed. Write '%s' instead.".formatted(
            TASK_TIMEOUT,
            "vanillabp.workflow-modules.test-app.workflows.RefusedStartProcess.adapters.c8.job-timeout")),
        message);
    assertTrue(
        message.contains(
            "  - 'vanillabp.allow-connectors' moved into the adapter section. Write "
                + "'vanillabp.adapters.<id>.allow-connectors' instead, once for each Camunda 8 adapter id (c8)."),
        message);
    // the developer reads the reason in the log of the failed start, not in a stack trace
    final var logged = output.getOut() + output.getErr();
    assertTrue(logged.contains(TASK_TIMEOUT), logged);

  }

}
