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
 * Startup-validation boot test (story 26c) on Quarkus: an INCONSISTENTLY configured
 * first-priority adapter (here <code>mode: saas</code> without any credential) fails
 * the boot with a message naming the missing property keys - the developer learns
 * about the defect at startup, not when the first workflow is started.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8InconsistentConfigurationTest {

  @RegisterExtension
  static final QuarkusExtensionTest extensionTest = new QuarkusExtensionTest()
      .setArchiveProducer(() -> ShrinkWrap
          .create(JavaArchive.class)
          .addClass(Aggregate.class)
          .addClass(SampleWorkflowService.class)
          .addClass(TestPhaseTwoOutbox.class)
          .addAsResource("application.yaml")
          .addAsResource("workflow-module-descriptor/workflow-module", "META-INF/workflow-module"))
      .overrideConfigKey("vanillabp.adapters.c8.mode", "saas")
      .assertException(throwable -> {
        var message = "";
        for (var cause = throwable; cause != null; cause = cause.getCause()) {
          message += cause.getMessage()
              + "\n";
        }
        Assertions.assertTrue(
            message.contains("Camunda 8 adapter 'c8' is configured inconsistently"),
            "expected the guiding startup failure but got:\n"
                + message);
        Assertions.assertTrue(message.contains("vanillabp.adapters.c8.cluster-id"));
        Assertions.assertTrue(message.contains("vanillabp.adapters.c8.client-secret"));
        Assertions.assertTrue(message.contains("vanillabp.adapters.c8.deployment-failure"));
      });

  @Test
  public void inconsistentConfigurationFailsTheBoot() {
    // should never be executed due to the expected startup exception
  }

}
