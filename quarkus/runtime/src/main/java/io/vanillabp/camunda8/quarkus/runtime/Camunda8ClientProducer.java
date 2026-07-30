package io.vanillabp.camunda8.quarkus.runtime;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.microprofile.config.ConfigProvider;

import io.smallrye.config.SmallRyeConfig;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.integration.adapter.migration.config.MigrationAdapterProperties;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

/**
 * Produces the {@link Camunda8ClientFactoryRegistry} (one lazily built
 * {@code CamundaClient} per Camunda 8 adapter ID) from the canonical per-adapter
 * configuration <code>vanillabp.adapters.&lt;id&gt;.*</code>, bound by the adapter's
 * overlay mapping ({@link VanillaBpCamunda8Properties}) - property names are NEVER
 * parsed programmatically (that pattern is lossy for environment-variable sources).
 * <p>
 * The adapter-id set comes from the platform's core properties (adapter ids of type
 * {@code camunda8}); the overlay map is used as a per-known-id lookup only. An
 * application configuring a Camunda 8 adapter id without any connection properties
 * still boots - a missing property surfaces only on first use of the client. The
 * registry is closed on application shutdown via the
 * {@link #close(Camunda8ClientFactoryRegistry) disposer}, closing all clients.
 */
@ApplicationScoped
public class Camunda8ClientProducer {

  @Produces
  @Singleton
  public Camunda8ClientFactoryRegistry camunda8ClientFactoryRegistry(
      final MigrationAdapterProperties properties) {

    final var overlay = ConfigProvider
        .getConfig()
        .unwrap(SmallRyeConfig.class)
        .getConfigMapping(VanillaBpCamunda8Properties.class);

    final Map<String, Camunda8AdapterConfiguration> configurations = new HashMap<>();
    properties
        .adapterTypes()
        .entrySet()
        .stream()
        .filter(adapter -> Camunda8DeploymentService.ADAPTER_TYPE.equals(adapter.getValue()))
        .map(Map.Entry::getKey)
        .forEach(adapterId -> configurations.put(
            adapterId,
            toConfiguration(overlay.adapters().get(adapterId))));

    return new Camunda8ClientFactoryRegistry(configurations);

  }

  private static Camunda8AdapterConfiguration toConfiguration(
      final VanillaBpCamunda8Properties.Camunda8AdapterKeys keys) {

    final var configuration = new Camunda8AdapterConfiguration();
    if (keys == null) {
      return configuration;
    }
    keys.mode().ifPresent(configuration::setMode);
    keys.restAddress().ifPresent(configuration::setRestAddress);
    keys.grpcAddress().ifPresent(configuration::setGrpcAddress);
    keys.preferRestOverGrpc().ifPresent(configuration::setPreferRestOverGrpc);
    keys.tenantId().ifPresent(configuration::setTenantId);
    keys.clusterId().ifPresent(configuration::setClusterId);
    keys.region().ifPresent(configuration::setRegion);
    keys.clientId().ifPresent(configuration::setClientId);
    keys.clientSecret().ifPresent(configuration::setClientSecret);
    return configuration;

  }

  public void close(
      @Disposes final Camunda8ClientFactoryRegistry registry) {

    registry.close();

  }

}
