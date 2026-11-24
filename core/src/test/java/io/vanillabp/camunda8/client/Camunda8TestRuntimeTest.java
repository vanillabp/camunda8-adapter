package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * An adapter whose cluster a test runtime hands over: no client at startup, a client per
 * test method, and a start which says that the configured connection is not used. Nothing
 * here reaches a cluster, because building a client sends nothing.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8TestRuntimeTest {

  private static final Camunda8TestRuntimeConnection CONNECTION = new Camunda8TestRuntimeConnection(
      URI.create("http://test-runtime:8080"), URI.create("http://test-runtime:26500"), null);

  private static final String NOT_USED = "These keys are not used while the test runtime hands over the cluster";

  @Test
  @DisplayName("The factory builds a client per handover and tells its participants in order")
  public void theFactoryBuildsAClientPerHandover() {

    final var events = new ArrayList<String>();
    try (var factory = new Camunda8ClientFactory("c8", new Camunda8AdapterConfiguration(), true)) {
      assertTrue(factory.followsATestRuntime());
      assertDoesNotThrow(factory::validateConfigured);
      assertNull(factory.clusterIdentity());
      final var noClient = assertThrows(IllegalStateException.class, factory::getClient);
      assertTrue(noClient.getMessage().contains("only while a test method runs"), noClient.getMessage());
      // nothing to close yet
      factory.disconnectFromTheTestRuntime();
      factory.takePartInTheTestRuntime(participant("first", events));
      factory.takePartInTheTestRuntime(participant("second", events));

      factory.connectToTheTestRuntime(CONNECTION);
      final var firstClient = factory.getClient();
      assertEquals(URI.create("http://test-runtime:8080"), firstClient.getConfiguration().getRestAddress());
      // a handover without a goodbye closes the client of the method before
      factory.connectToTheTestRuntime(CONNECTION);
      assertNotNull(factory.getClient());
      factory.disconnectFromTheTestRuntime();

      assertEquals(
          List.of("first arrived", "second arrived", "second leaves", "first leaves", "first arrived",
              "second arrived", "second leaves", "first leaves"),
          events);
      assertThrows(IllegalStateException.class, factory::getClient);
    }

  }

  @Test
  @DisplayName("A factory on its configured cluster refuses the handover of a test runtime")
  public void aFactoryOnItsConfiguredClusterRefusesTheHandover() {

    try (var factory = new Camunda8ClientFactory("c8", new Camunda8AdapterConfiguration())) {
      assertFalse(factory.followsATestRuntime());
      final var refused = assertThrows(IllegalStateException.class, () -> factory.connectToTheTestRuntime(CONNECTION));
      assertTrue(refused.getMessage().contains("connects to its configured cluster"), refused.getMessage());
      // and a goodbye changes nothing
      factory.disconnectFromTheTestRuntime();
      assertNotNull(factory.getClient());
    }

  }

  @Test
  @DisplayName("Every adapter id connects to the one cluster of the test runtime")
  public void everyAdapterIdConnectsToTheOneCluster() {

    final var first = new Camunda8AdapterConfiguration();
    first.setRestAddress("http://one:8080");
    final var second = new Camunda8AdapterConfiguration();
    second.setRestAddress("http://two:8080");
    try (var registry = new Camunda8ClientFactoryRegistry(Map.of("one", first, "two", second), true)) {
      assertEquals(List.of("two"), registry.getFactory("one").getAdapterIdsSharingTheCluster());
      registry.connectToTheTestRuntime(CONNECTION);
      assertEquals(
          URI.create("http://test-runtime:8080"),
          registry.getFactory("two").getClient().getConfiguration().getRestAddress());
      registry.disconnectFromTheTestRuntime();
      assertThrows(IllegalStateException.class, () -> registry.getFactory("one").getClient());
    }

  }

  @Test
  @DisplayName("The start names the connection keys a test runtime makes unused")
  public void theStartNamesTheUnusedConnectionKeys() {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setJobLease(Camunda8AdapterConfiguration.JobLease.DO_NOT_USE);
    configuration.setMode(Camunda8AdapterConfiguration.Mode.SAAS);
    configuration.setRestAddress("http://somewhere:8080");
    configuration.setGrpcAddress("http://somewhere:26500");
    configuration.setClusterId("cluster");
    configuration.setRegion("bru-2");
    configuration.setClientId("client");
    configuration.setClientSecret("secret");
    final var lines = new ArrayList<String>();

    // a SaaS adapter without its keys would end the start. Here nothing is missing
    Camunda8StartupValidation.validateAtStartupForATestRuntime("c8", configuration, Duration.ofDays(7), lines::add,
        lines::add);

    final var line = lines.getLast();
    assertTrue(line.contains("Camunda 8 adapter 'c8' connects to the cluster of the test runtime"), line);
    assertTrue(line.contains(NOT_USED), line);
    assertTrue(line.contains("'vanillabp.adapters.c8.mode', 'vanillabp.adapters.c8.rest-address', "
        + "'vanillabp.adapters.c8.grpc-address', 'vanillabp.adapters.c8.cluster-id', 'vanillabp.adapters.c8.region', "
        + "'vanillabp.adapters.c8.client-id', 'vanillabp.adapters.c8.client-secret'"), line);

  }

  @Test
  @DisplayName("Without connection keys the start names none")
  public void withoutConnectionKeysTheStartNamesNone() {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setJobLease(Camunda8AdapterConfiguration.JobLease.DO_NOT_USE);
    final var lines = new ArrayList<String>();

    Camunda8StartupValidation.validateAtStartupForATestRuntime("c8", configuration, Duration.ofDays(7), lines::add,
        lines::add);

    final var line = lines.getLast();
    assertTrue(line.contains("Camunda 8 adapter 'c8' connects to the cluster of the test runtime"), line);
    assertFalse(line.contains(NOT_USED), line);
    // and the warning about the local cluster stays out, because nothing connects there
    assertTrue(lines.stream().noneMatch(text -> text.contains("connects to the local cluster")), lines.toString());

  }

  private static Camunda8ClientFactory.TestRuntimeParticipant participant(
      final String name,
      final List<String> events) {

    return new Camunda8ClientFactory.TestRuntimeParticipant() {

      @Override
      public void clusterArrived() {

        events.add(name
            + " arrived");

      }

      @Override
      public void clusterLeaves() {

        events.add(name
            + " leaves");

      }

    };

  }

}
