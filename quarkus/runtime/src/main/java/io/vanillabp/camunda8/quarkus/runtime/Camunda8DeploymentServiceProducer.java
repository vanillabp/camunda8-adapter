package io.vanillabp.camunda8.quarkus.runtime;

import java.util.Map;

import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.integration.adapter.migration.config.MigrationAdapterProperties;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * Produces the Camunda 8 adapter's {@link Camunda8DeploymentService} as a CDI bean. The
 * adapter ID is resolved from the configuration (first adapter of type {@code camunda8})
 * and its {@code CamundaClient} is obtained from the
 * {@link Camunda8ClientFactoryRegistry}.
 * <p>
 * <b>Note:</b> The Quarkus platform integration does not yet run the deployment pipeline
 * on startup (there is no {@code StartupEvent} observer building the core
 * {@code DeploymentService} from the adapter deployment services). Producing this bean
 * makes deployment-service and client-factory creation verifiable by a
 * {@code QuarkusUnitTest} and prepares the wiring for when the platform invokes the
 * pipeline on Quarkus.
 */
@ApplicationScoped
public class Camunda8DeploymentServiceProducer {

  @Produces
  @ApplicationScoped
  public Camunda8DeploymentService camunda8DeploymentService(
      final MigrationAdapterProperties properties,
      final Camunda8ClientFactoryRegistry clientFactoryRegistry) {

    final var adapterId = properties
        .adapterTypes()
        .entrySet()
        .stream()
        .filter(adapter -> Camunda8DeploymentService.ADAPTER_TYPE.equals(adapter.getValue()))
        .map(Map.Entry::getKey)
        .findFirst()
        .orElse("");

    return new Camunda8DeploymentService(
        adapterId, clientFactoryRegistry.getFactory(adapterId));

  }

}
