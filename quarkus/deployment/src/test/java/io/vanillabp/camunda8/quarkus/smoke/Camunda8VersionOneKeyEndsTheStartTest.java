package io.vanillabp.camunda8.quarkus.smoke;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * A key of the Camunda 8 adapter of VanillaBP 1 which this version does not read ends the build
 * of the application. The message names each such key and what to write instead. Quarkus alone
 * would only say that no mapping knows the key.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8VersionOneKeyEndsTheStartTest {

  private static final String TASK_TIMEOUT = "vanillabp.workflow-modules.test-app.workflows.TaskProcess.adapters.c8.task-timeout";

  @RegisterExtension
  static final QuarkusExtensionTest extensionTest = new QuarkusExtensionTest()
      .setArchiveProducer(() -> ShrinkWrap
          .create(JavaArchive.class)
          .addClass(Aggregate.class)
          .addClass(SampleWorkflowService.class)
          .addClass(TestPhaseTwoOutbox.class)
          .addAsResource("application.yaml")
          .addAsResource("workflow-module-descriptor/workflow-module", "META-INF/workflow-module"))
      .overrideConfigKey(TASK_TIMEOUT, "PT1M")
      .overrideConfigKey("vanillabp.allow-connectors", "true")
      .assertException(throwable -> {
        final var causes = new StringBuilder();
        for (var cause = throwable; cause != null; cause = cause.getCause()) {
          causes
              .append(cause.getMessage())
              .append('\n');
        }
        final var message = causes.toString();
        Assertions.assertTrue(
            message.contains("The Camunda 8 adapter does not read these keys of VanillaBP 1"),
            "expected the guiding startup failure but got:\n"
                + message);
        Assertions.assertTrue(
            message.contains("  - '%s': It was renamed. Write '%s' instead.".formatted(
                TASK_TIMEOUT,
                "vanillabp.workflow-modules.test-app.workflows.TaskProcess.adapters.c8.job-timeout")),
            message);
        Assertions.assertTrue(
            message.contains(
                "  - 'vanillabp.allow-connectors' moved into the adapter section. Write "
                    + "'vanillabp.adapters.<id>.allow-connectors' instead, once for each Camunda 8 adapter id (c8)."),
            message);
      });

  @Test
  public void keysOfVersionOneEndTheStart() {
    // never runs, because the start fails as expected
  }

}
