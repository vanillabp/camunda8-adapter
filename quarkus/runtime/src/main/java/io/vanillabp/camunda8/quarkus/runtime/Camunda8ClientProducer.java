package io.vanillabp.camunda8.quarkus.runtime;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

/**
 * Produces the {@link Camunda8ClientFactoryRegistry} (one lazily built {@code CamundaClient}
 * per Camunda 8 adapter ID) from the provisional flat configuration
 * ({@code camunda8-adapter.<adapter-id>.*}, see {@link Camunda8AdapterConfiguration}).
 * <p>
 * The configuration is read programmatically from the MicroProfile {@link Config} at
 * runtime (rather than a {@code @ConfigMapping}) so an application configuring a Camunda 8
 * adapter but without any connection properties still boots - a missing property surfaces
 * only on first use of the client. The registry is closed on application shutdown via the
 * {@link #close(Camunda8ClientFactoryRegistry) disposer}, closing all clients.
 */
@ApplicationScoped
public class Camunda8ClientProducer {

  @Produces
  @Singleton
  public Camunda8ClientFactoryRegistry camunda8ClientFactoryRegistry() {

    final var config = ConfigProvider.getConfig();
    final var prefix = Camunda8AdapterConfiguration.CONFIGURATION_PREFIX
        + ".";

    final Map<String, Camunda8AdapterConfiguration> configurations = new HashMap<>();
    for (final var propertyName : config.getPropertyNames()) {
      if (!propertyName.startsWith(prefix)) {
        continue;
      }
      final var adapterIdAndKey = propertyName.substring(prefix.length());
      final var separator = adapterIdAndKey.indexOf('.');
      if (separator < 0) {
        continue;
      }
      final var adapterId = adapterIdAndKey.substring(0, separator);
      final var key = adapterIdAndKey.substring(separator + 1);
      final var configuration = configurations.computeIfAbsent(
          adapterId,
          id -> new Camunda8AdapterConfiguration());
      applyProperty(config, propertyName, key, configuration);
    }

    return new Camunda8ClientFactoryRegistry(configurations);

  }

  private void applyProperty(
      final Config config,
      final String propertyName,
      final String key,
      final Camunda8AdapterConfiguration configuration) {

    final var value = config
        .getOptionalValue(propertyName, String.class)
        .orElse(null);
    if (value == null || value.isBlank()) {
      return;
    }
    switch (key) {
      case "mode" -> configuration.setMode(
          Camunda8AdapterConfiguration.Mode.valueOf(value.trim().toUpperCase().replace('-', '_')));
      case "rest-address" -> configuration.setRestAddress(value);
      case "grpc-address" -> configuration.setGrpcAddress(value);
      case "prefer-rest-over-grpc" -> configuration.setPreferRestOverGrpc(Boolean.parseBoolean(value));
      case "tenant-id" -> configuration.setTenantId(value);
      case "cluster-id" -> configuration.setClusterId(value);
      case "region" -> configuration.setRegion(value);
      case "client-id" -> configuration.setClientId(value);
      case "client-secret" -> configuration.setClientSecret(value);
      default -> {
        // ignore unknown keys of the provisional namespace
      }
    }

  }

  public void close(
      @Disposes final Camunda8ClientFactoryRegistry registry) {

    registry.close();

  }

}
