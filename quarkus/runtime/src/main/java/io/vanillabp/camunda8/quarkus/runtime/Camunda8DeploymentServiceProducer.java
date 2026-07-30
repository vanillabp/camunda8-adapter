package io.vanillabp.camunda8.quarkus.runtime;

import java.util.List;
import java.util.Map;

import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.integration.adapter.migration.config.MigrationAdapterProperties;
import io.vanillabp.integration.adapter.spi.AdapterDeploymentService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * Produces the Camunda 8 adapter's {@link Camunda8DeploymentService} instances - ONE
 * per configured adapter id of type {@code camunda8}, as a bean of type
 * <code>List&lt;AdapterDeploymentService&gt;</code> (element type = the SPI interface,
 * required by the platform contract) (the per-adapter-id shape on
 * Quarkus: a CDI producer cannot yield N element beans for N runtime-configured ids).
 * Each instance obtains its {@code CamundaClient} from the
 * {@link Camunda8ClientFactoryRegistry}.
 * <p>
 * <b>Note:</b> The Quarkus platform integration does not yet run the deployment pipeline
 * on startup (there is no {@code StartupEvent} observer building the core
 * {@code DeploymentService} from the adapter deployment services). Producing this bean
 * makes deployment-service and client-factory creation verifiable by a
 * {@code QuarkusUnitTest} and prepares the wiring for when the platform invokes the
 * pipeline on Quarkus - the platform's collection point will flatten List beans
 * alongside element beans (same contract as for the process services).
 */
@ApplicationScoped
public class Camunda8DeploymentServiceProducer {

  @Produces
  @ApplicationScoped
  public List<AdapterDeploymentService<BpmnModelInstance, Camunda8ProcessingContext>> camunda8DeploymentServices(
      final MigrationAdapterProperties properties,
      final Camunda8ClientFactoryRegistry clientFactoryRegistry) {

    return properties
        .adapterTypes()
        .entrySet()
        .stream()
        .filter(adapter -> Camunda8DeploymentService.ADAPTER_TYPE.equals(adapter.getValue()))
        .map(Map.Entry::getKey)
        .sorted()
        .<AdapterDeploymentService<BpmnModelInstance, Camunda8ProcessingContext>>map(
            adapterId -> new Camunda8DeploymentService(adapterId, clientFactoryRegistry.getFactory(adapterId)))
        .toList();

  }

}
