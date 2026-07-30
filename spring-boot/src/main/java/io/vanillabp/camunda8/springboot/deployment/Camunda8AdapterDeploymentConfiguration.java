package io.vanillabp.camunda8.springboot.deployment;

import java.util.Map;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;

import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.camunda8.springboot.Camunda8AdapterConfiguration;
import io.vanillabp.integration.adapter.migration.config.MigrationAdapterProperties;
import io.vanillabp.integration.processservice.SpringBootMigrationAdapterAutoConfiguration;

/**
 * Registers the {@link Camunda8DeploymentService} as an <i>element</i> bean - never
 * as a bean of type <code>List&lt;AdapterDeploymentService&gt;</code>: the platform
 * collects all adapters' deployment services via <code>ObjectProvider</code> streams,
 * and only element beans allow several adapter types to coexist in one application
 * (the central migration scenario; a List bean per adapter breaks collection
 * injection as soon as a second adapter is present).
 * <p>
 * Currently ONE instance is built for the first configured adapter id of type
 * {@code camunda8} - per-adapter-id multiplicity (one element bean per configured
 * id, e.g. on-prem and SaaS clusters side by side during a migration) is introduced
 * by the adapter-config-model story (26d).
 */
@AutoConfiguration(after = SpringBootMigrationAdapterAutoConfiguration.class)
public class Camunda8AdapterDeploymentConfiguration {

  @Bean
  public Camunda8DeploymentService camunda8DeploymentService(
      final MigrationAdapterProperties properties,
      final Camunda8ClientFactoryRegistry clientFactoryRegistry) {

    final var adapterId = properties
        .adapterTypes()
        .entrySet()
        .stream()
        .filter(adapter -> adapter.getValue().equals(Camunda8AdapterConfiguration.ADAPTER_TYPE))
        .map(Map.Entry::getKey)
        .findFirst()
        .orElse("");

    return new Camunda8DeploymentService(adapterId, clientFactoryRegistry.getFactory(adapterId));

  }

}
