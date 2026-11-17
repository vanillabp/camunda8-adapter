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
 * <code>allow-connectors</code> and <code>allow-listeners</code> are not read at task level. A
 * value set there ends the start, and the message names the key and the levels where it works.
 * Quarkus binds both keys at task level, so it is the adapter which ends the start, not Quarkus.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8SwitchAtTaskLevelEndsTheStartTest {

  private static final String TASK_LEVEL = "vanillabp.workflow-modules.test-app.workflows.TaskProcess.tasks.happyTask.adapters.c8.";

  @RegisterExtension
  static final QuarkusExtensionTest extensionTest = new QuarkusExtensionTest()
      .setArchiveProducer(() -> ShrinkWrap
          .create(JavaArchive.class)
          .addClass(Aggregate.class)
          .addClass(SampleWorkflowService.class)
          .addClass(TestPhaseTwoOutbox.class)
          .addAsResource("application.yaml")
          .addAsResource("workflow-module-descriptor/workflow-module", "META-INF/workflow-module"))
      .overrideConfigKey(TASK_LEVEL
          + "allow-connectors", "true")
      .assertException(throwable -> {
        final var causes = new StringBuilder();
        for (var cause = throwable; cause != null; cause = cause.getCause()) {
          causes
              .append(cause.getMessage())
              .append('\n');
        }
        final var message = causes.toString();
        Assertions.assertTrue(
            message.contains("Camunda 8 adapter 'c8' has 'allow-connectors' set at TASK level, where nothing reads it"),
            "expected the guiding startup failure but got:\n"
                + message);
        Assertions.assertTrue(message.contains(TASK_LEVEL
            + "allow-connectors"), message);
        Assertions.assertTrue(
            message.contains("vanillabp.workflow-modules.<m>.workflows.<w>.adapters.c8.allow-connectors"),
            message);
      });

  @Test
  public void aSwitchAtTaskLevelEndsTheStart() {
    // never runs, because the start fails as expected
  }

}
