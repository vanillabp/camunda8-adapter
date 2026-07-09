package io.vanillabp.camunda8.springboot.processservice;

import java.util.Map;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;

import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.processservice.Camunda8ProcessService;
import io.vanillabp.camunda8.springboot.Camunda8AdapterConfiguration;
import io.vanillabp.integration.adapter.migration.config.MigrationAdapterProperties;
import io.vanillabp.integration.adapter.spi.MigratableProcessService;

/**
 * Provides the Camunda 8 adapter's {@link MigratableProcessService} bean picked up by
 * the {@link io.vanillabp.spi.process.ProcessService} beans built by the VanillaBP
 * Spring Boot integration.
 * <p>
 * The adapter ID is resolved from the configuration (first adapter of type
 * {@code camunda8}). An {@link ObjectProvider} is used because this bean may be created
 * very early during bootstrapping (before configuration-properties beans are bound):
 * calling {@link ObjectProvider#getObject()} forces the properties to be bound at that
 * point.
 */
@AutoConfiguration
public class Camunda8AdapterProcessServiceConfiguration {

  @Bean
  public MigratableProcessService<?> camunda8MigratableProcessService(
      final ObjectProvider<MigrationAdapterProperties> properties,
      final ObjectProvider<Camunda8ClientFactoryRegistry> clientFactoryRegistry) {

    final var adapterId = properties
        .getObject()
        .getAdapters()
        .entrySet()
        .stream()
        .filter(adapter -> Camunda8AdapterConfiguration.ADAPTER_TYPE.equals(adapter.getValue()))
        .map(Map.Entry::getKey)
        .findFirst()
        .orElse("");

    return new Camunda8ProcessService<>(
        adapterId, clientFactoryRegistry
            .getObject()
            .getFactory(adapterId));

  }

}
