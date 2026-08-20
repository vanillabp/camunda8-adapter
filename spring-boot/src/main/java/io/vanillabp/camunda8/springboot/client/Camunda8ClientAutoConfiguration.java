package io.vanillabp.camunda8.springboot.client;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.client.Camunda8StartupValidation;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.integration.config.VanillaBpConfigurationProperties;
import lombok.extern.slf4j.Slf4j;

/**
 * Exposes the {@link Camunda8ClientFactoryRegistry} built from the canonical
 * per-adapter configuration <code>vanillabp.adapters.&lt;id&gt;.*</code> (bound by the
 * adapter's overlay {@link VanillaBpCamunda8Properties}). The registry owns one lazily
 * built {@code CamundaClient} per adapter ID; it is a managed bean so its
 * {@code close()} is called on application shutdown, closing all clients.
 * <p>
 * The adapter-id set comes from the platform's core properties (adapter ids of type
 * {@code camunda8}); the overlay map is used as a per-known-id lookup only. Every
 * instance's connection configuration is VALIDATED AT STARTUP
 * ({@link Camunda8StartupValidation}): an entirely unconfigured adapter boots with a
 * guiding warning, an inconsistently configured one fails the boot (unless it is
 * nowhere first priority and its deployment-failure policy is 'warn'); clients of
 * completely configured adapters are built eagerly.
 */
@Slf4j
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
        .forEach(adapterId -> {
          final var configuration = overlay
              .getAdapters()
              .getOrDefault(adapterId, new Camunda8AdapterConfiguration());
          Camunda8StartupValidation.validateAtStartup(
              adapterId,
              configuration,
              coreProperties.isFirstPriorityAnywhere(adapterId),
              coreProperties.getDeploymentFailureFor(
                  adapterId) == io.vanillabp.integration.adapter.migration.config.DeploymentFailurePolicy.WARN,
              coreProperties.getOutbox() == null
                  ? null
                  : coreProperties.getOutbox().getRetention(),
              log::warn);
          configurations.put(adapterId, configuration);
        });

    return new Camunda8ClientFactoryRegistry(configurations);

  }

  /**
   * What this adapter measures on top of the core's meters (story 92): the client's own
   * job counters per worker and the execution slots of every adapter instance.
   * Micrometer is optional, so the whole configuration is conditional on it - an
   * application without Micrometer boots unchanged and reports nothing.
   */
  @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
  // by NAME, not by class literal: the annotation of a nested configuration class is
  // read reflectively, so a class literal of an absent optional dependency would fail
  // before the condition is ever evaluated
  @org.springframework.boot.autoconfigure.condition.ConditionalOnClass(
      name = "io.micrometer.core.instrument.MeterRegistry")
  public static class Camunda8MetricsConfiguration {

    /**
     * @return The meter binder of this adapter's own numbers
     */
    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
    public io.vanillabp.camunda8.observability.MicrometerCamunda8Metrics camunda8Metrics() {

      return new io.vanillabp.camunda8.observability.MicrometerCamunda8Metrics();

    }

  }

}
