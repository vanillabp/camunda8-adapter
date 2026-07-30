package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests of {@link Camunda8ClientFactory} / {@link Camunda8AdapterConfiguration}: an
 * application without connection configuration still boots (validation is lazy) and a
 * missing property is reported on first use, naming the exact property. Building the
 * self-managed client does not contact any cluster.
 */
public class Camunda8ClientFactoryTest {

  @Test
  @DisplayName("self-managed without rest-address fails naming the exact property")
  public void selfManagedMissingRestAddressNamesProperty() {

    final var factory = new Camunda8ClientFactory("c8", new Camunda8AdapterConfiguration());

    final var exception = assertThrows(IllegalStateException.class, factory::validateConfigured);
    assertTrue(exception.getMessage().contains("vanillabp.adapters.c8.rest-address"),
        "message should name the missing property, but was: "
            + exception.getMessage());

  }

  @Test
  @DisplayName("saas without required properties fails naming the exact property")
  public void saasMissingPropertiesNamesProperty() {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setMode(Camunda8AdapterConfiguration.Mode.SAAS);
    final var factory = new Camunda8ClientFactory("cloud", configuration);

    final var exception = assertThrows(IllegalStateException.class, factory::validateConfigured);
    assertTrue(exception.getMessage().contains("vanillabp.adapters.cloud.cluster-id"),
        "message should name the missing property, but was: "
            + exception.getMessage());

  }

  @Test
  @DisplayName("a configured self-managed adapter validates and builds a client without contacting a cluster")
  public void selfManagedConfiguredBuildsClient() {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setRestAddress("http://localhost:8080");
    configuration.setGrpcAddress("http://localhost:26500");
    final var factory = new Camunda8ClientFactory("c8", configuration);

    assertDoesNotThrow(factory::validateConfigured);

    final var client = factory.getClient();
    assertNotNull(client);
    // the client is cached (single instance per adapter)
    assertSame(client, factory.getClient());

    factory.close();

  }

}
