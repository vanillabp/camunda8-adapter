package io.vanillabp.camunda8.quarkus.runtime;

import java.util.List;
import java.util.Map;

import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.camunda8.processservice.Camunda8ProcessService;
import io.vanillabp.integration.adapter.migration.config.MigrationAdapterProperties;
import io.vanillabp.integration.adapter.spi.MigratableProcessService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * Provides the Camunda 8 adapter's {@link Camunda8ProcessService} instances - ONE per
 * configured adapter id of type {@code camunda8} (multiple ids of one BPMS type = the
 * migration scenario, e.g. an on-prem and a SaaS cluster side by side). A CDI producer
 * cannot yield N element beans for N runtime-configured ids, so the adapter produces
 * ONE bean of type <code>List&lt;MigratableProcessService&gt;</code>; the platform's
 * collection point flattens List beans alongside element beans. The adapter id is a
 * CONSTRUCTOR parameter of each instance.
 * <p>
 * The adapter-id set ALWAYS comes from the platform's core properties
 * ({@code adapterTypes()}); the adapter's configuration overlay is a per-known-id
 * lookup only.
 */
@ApplicationScoped
public class Camunda8ProcessServiceProducer {

  @Produces
  public List<MigratableProcessService<Object>> camunda8MigratableProcessServices(
      final MigrationAdapterProperties properties,
      final Camunda8ClientFactoryRegistry clientFactoryRegistry,
      final jakarta.transaction.TransactionSynchronizationRegistry synchronizationRegistry) {

    final var overlay = org.eclipse.microprofile.config.ConfigProvider
        .getConfig()
        .unwrap(io.smallrye.config.SmallRyeConfig.class)
        .getConfigMapping(VanillaBpCamunda8Properties.class);

    return properties
        .adapterTypes()
        .entrySet()
        .stream()
        .filter(adapter -> Camunda8DeploymentService.ADAPTER_TYPE.equals(adapter.getValue()))
        .map(Map.Entry::getKey)
        .sorted()
        .<MigratableProcessService<Object>>map(adapterId -> {
          final var adapterKeys = overlay.adapters().get(adapterId);
          final var asyncTaskTimeout = adapterKeys != null
              ? adapterKeys.asyncTaskTimeout().orElse(java.time.Duration.ofDays(14))
              : java.time.Duration.ofDays(14);
          return new Camunda8ProcessService<>(
              adapterId, clientFactoryRegistry
                  .getFactory(adapterId), asyncTaskTimeout, new Camunda8QuarkusPreCommitRegistrar(
                      synchronizationRegistry));
        })
        .toList();

  }

}
