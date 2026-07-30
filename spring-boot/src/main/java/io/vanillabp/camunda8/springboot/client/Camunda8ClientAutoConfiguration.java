package io.vanillabp.camunda8.springboot.client;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.integration.config.VanillaBpConfigurationProperties;

/**
 * Exposes the {@link Camunda8ClientFactoryRegistry} built from the canonical
 * per-adapter configuration <code>vanillabp.adapters.&lt;id&gt;.*</code> (bound by the
 * adapter's overlay {@link VanillaBpCamunda8Properties}). The registry owns one lazily
 * built {@code CamundaClient} per adapter ID; it is a managed bean so its
 * {@code close()} is called on application shutdown, closing all clients.
 * <p>
 * The adapter-id set comes from the platform's core properties (adapter ids of type
 * {@code camunda8}); the overlay map is used as a per-known-id lookup only. An
 * application configuring a Camunda 8 adapter id without any connection properties
 * still boots - a missing property surfaces only on first use of the client.
 */
@AutoConfiguration
@EnableConfigurationProperties({
    VanillaBpConfigurationProperties.class, VanillaBpCamunda8Properties.class
})
public class Camunda8ClientAutoConfiguration {

  @Bean(destroyMethod = "close")
  public Camunda8ClientFactoryRegistry camunda8ClientFactoryRegistry(
      final VanillaBpConfigurationProperties coreProperties,
      final VanillaBpCamunda8Properties overlay) {

    final Map<String, Camunda8AdapterConfiguration> configurations = new HashMap<>();
    coreProperties
        .adapterTypes()
        .entrySet()
        .stream()
        .filter(adapter -> Camunda8DeploymentService.ADAPTER_TYPE.equals(adapter.getValue()))
        .map(Map.Entry::getKey)
        .forEach(adapterId -> configurations.put(
            adapterId,
            overlay
                .getAdapters()
                .getOrDefault(adapterId, new Camunda8AdapterConfiguration())));

    return new Camunda8ClientFactoryRegistry(configurations);

  }

}
