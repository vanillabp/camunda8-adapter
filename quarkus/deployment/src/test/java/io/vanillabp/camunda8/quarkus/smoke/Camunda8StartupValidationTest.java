package io.vanillabp.camunda8.quarkus.smoke;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusUnitTest;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Startup-validation boot test (story 26c) on Quarkus: an adapter WITHOUT any
 * connection configuration still boots - the {@code StartupEvent} observer forces
 * the validation which emits a guiding WARN naming the exact property keys to add.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8StartupValidationTest {

  @RegisterExtension
  static final QuarkusUnitTest unitTest = new QuarkusUnitTest()
      .setArchiveProducer(() -> ShrinkWrap
          .create(JavaArchive.class)
          .addClass(Aggregate.class)
          .addClass(SampleWorkflowService.class)
          .addClass(TestPhaseTwoOutbox.class)
          .addAsResource("application.yaml")
          .addAsResource("workflow-module-descriptor/workflow-module", "META-INF/workflow-module"))
      .setLogRecordPredicate(record -> record.getLevel().intValue() >= java.util.logging.Level.WARNING.intValue())
      .assertLogRecords(records -> {
        final var messages = records
            .stream()
            .map(record -> record.getMessage() == null
                ? ""
                : String.format(record.getMessage(), record.getParameters()))
            .toList();
        Assertions.assertTrue(
            messages
                .stream()
                .anyMatch(
                    message -> message.contains("Camunda 8 adapter 'c8' has no connection configuration yet") && message
                        .contains(
                            "vanillabp.adapters.c8.rest-address") && message.contains("vanillabp.adapters.c8.mode")),
            "expected the guiding startup warning naming the property keys but got: "
                + messages);
      });

  @Test
  public void unconfiguredAdapterBootsWithGuidingWarning() {
    // the assertion happens on the collected log records after shutdown
  }

}
