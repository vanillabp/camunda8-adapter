package io.vanillabp.camunda8.quarkus.smoke;

import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.spi.PhaseTwoOutbox;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * A {@link PhaseTwoOutbox} stub for smoke tests booting WITHOUT a database: the
 * Camunda 8 adapter requires a two-phase commit for starting workflows, so since
 * story 26i of the platform an outbox has to be RESOLVABLE at startup. The smoke
 * tests never start workflows - any usage of the stub fails loudly.
 */
@ApplicationScoped
public class TestPhaseTwoOutbox implements PhaseTwoOutbox {

  @Override
  public boolean schedule(
      final PhaseTwoCall call) {

    throw new UnsupportedOperationException("no outbox in this test");

  }

}
