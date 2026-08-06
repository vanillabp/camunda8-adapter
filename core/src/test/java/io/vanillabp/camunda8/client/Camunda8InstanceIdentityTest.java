package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Two <code>camunda8</code> adapter ids only make sense if they address DIFFERENT
 * clusters - or one cluster with different credentials/tenants (story 34).
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8InstanceIdentityTest {

  private static Camunda8AdapterConfiguration selfManaged(
      final String restAddress) {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setRestAddress(restAddress);
    return configuration;

  }

  private static Camunda8AdapterConfiguration saas(
      final String clusterId,
      final String clientId) {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setMode(Camunda8AdapterConfiguration.Mode.SAAS);
    configuration.setClusterId(clusterId);
    configuration.setRegion("bru-2");
    configuration.setClientId(clientId);
    configuration.setClientSecret("secret");
    return configuration;

  }

  private static void validate(
      final Map<String, Camunda8AdapterConfiguration> configurations) {

    Camunda8InstanceIdentity.validateDistinct(List.copyOf(configurations.keySet()), configurations::get);

  }

  @Test
  @DisplayName("Two ids on the same self-managed address are the same cluster")
  public void sameSelfManagedAddressFails() {

    final var exception = assertThrows(
        IllegalStateException.class,
        () -> validate(Map.of(
            "c8-old", selfManaged("http://localhost:8080"),
            "c8-new", selfManaged("http://localhost:8080"))));

    assertTrue(exception.getMessage().contains("c8-old"), exception::getMessage);
    assertTrue(exception.getMessage().contains("rest-address"), exception::getMessage);

  }

  @Test
  @DisplayName("SaaS: the COMBINATION of cluster id and client id decides")
  public void saasIsDistinguishedByClusterAndClient() {

    // same cluster AND same client: the same instance
    assertThrows(
        IllegalStateException.class,
        () -> validate(Map.of(
            "c8-a", saas("cluster-1", "client-1"),
            "c8-b", saas("cluster-1", "client-1"))));

    // same cluster, DIFFERENT clients: a legitimate setup (separated permissions)
    assertDoesNotThrow(
        () -> validate(Map.of(
            "c8-a", saas("cluster-1", "client-1"),
            "c8-b", saas("cluster-1", "client-2"))));

    // different clusters
    assertDoesNotThrow(
        () -> validate(Map.of(
            "c8-a", saas("cluster-1", "client-1"),
            "c8-b", saas("cluster-2", "client-1"))));

  }

  @Test
  @DisplayName("Distinct addresses and distinct tenants make two ids distinct")
  public void distinctInstancesAreAccepted() {

    assertDoesNotThrow(
        () -> validate(Map.of(
            "c8-old", selfManaged("http://old:8080"),
            "c8-new", selfManaged("http://new:8080"))));

    final var tenantA = selfManaged("http://localhost:8080");
    tenantA.setTenantId("tenant-a");
    final var tenantB = selfManaged("http://localhost:8080");
    tenantB.setTenantId("tenant-b");
    assertDoesNotThrow(() -> validate(Map.of("c8-a", tenantA, "c8-b", tenantB)));

  }

  @Test
  @DisplayName("A single id is never checked, and an unavailable resolver skips the check")
  public void singleIdAndMissingResolverAreNoOps() {

    assertDoesNotThrow(() -> Camunda8InstanceIdentity.validateDistinct(List.of("c8"), id -> null));
    assertDoesNotThrow(() -> Camunda8InstanceIdentity.validateDistinct(List.of("a", "b"), null));
    assertDoesNotThrow(() -> Camunda8InstanceIdentity.validateDistinct(null, id -> null));

  }

}
