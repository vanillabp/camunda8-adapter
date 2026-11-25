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
 * The <code>mtls</code> block binds on Quarkus, and a certificate which cannot be used ends
 * the start. On the 8.9 and 8.10 lines the file is not there. On the 8.8 line the client has
 * no mutual TLS, and the block is refused as a whole. Either way the message names a key of the
 * block, and never the passphrase.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8MtlsEndsTheStartTest {

  private static final String PASSPHRASE = "super-secret-passphrase-4711";

  @RegisterExtension
  static final QuarkusExtensionTest extensionTest = new QuarkusExtensionTest()
      .setArchiveProducer(() -> ShrinkWrap
          .create(JavaArchive.class)
          .addClass(Aggregate.class)
          .addClass(SampleWorkflowService.class)
          .addClass(TestPhaseTwoOutbox.class)
          .addAsResource("application.yaml")
          .addAsResource("workflow-module-descriptor/workflow-module", "META-INF/workflow-module"))
      .overrideConfigKey("vanillabp.adapters.c8.rest-address", "https://localhost:8443")
      .overrideConfigKey("vanillabp.adapters.c8.grpc-address", "https://localhost:26500")
      .overrideConfigKey("vanillabp.adapters.c8.mtls.cert-path", "/not/mounted/client.pem")
      .overrideConfigKey("vanillabp.adapters.c8.mtls.key-path", "/not/mounted/client.key")
      .overrideConfigKey("vanillabp.adapters.c8.mtls.key-passphrase", PASSPHRASE)
      .assertException(throwable -> {
        final var causes = new StringBuilder();
        for (var cause = throwable; cause != null; cause = cause.getCause()) {
          causes
              .append(cause.getMessage())
              .append('\n');
        }
        final var message = causes.toString();
        Assertions.assertTrue(
            message.contains("vanillabp.adapters.c8.mtls.cert-path"),
            "expected the guiding startup failure but got:\n"
                + message);
        Assertions.assertFalse(message.contains(PASSPHRASE), "a passphrase never reaches a message");
      });

  @Test
  public void aCertificateWhichCannotBeUsedEndsTheStart() {
    // never runs, because the start fails as expected
  }

}
