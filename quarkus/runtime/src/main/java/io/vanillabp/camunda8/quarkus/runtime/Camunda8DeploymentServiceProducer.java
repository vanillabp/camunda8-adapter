package io.vanillabp.camunda8.quarkus.runtime;

import java.util.List;
import java.util.Map;

import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.integration.adapter.migration.config.MigrationAdapterProperties;
import io.vanillabp.integration.adapter.spi.AdapterDeploymentService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

/**
 * Produces the Camunda 8 adapter's {@link Camunda8DeploymentService} instances - ONE
 * per configured adapter id of type {@code camunda8} (the per-adapter-id shape on
 * Quarkus: a CDI producer cannot yield N element beans for N runtime-configured
 * ids), consumed by the VanillaBP Quarkus integration's runtime deployment pipeline.
 * Each instance obtains its {@code CamundaClient} from the
 * {@link Camunda8ClientFactoryRegistry}.
 * <p>
 * Platform contract: the List's element type is the SPI interface with BOTH type
 * parameters literally {@code Object} - regardless of the adapter's actual model
 * ({@code BpmnModelInstance}) and context ({@code Camunda8ProcessingContext})
 * classes: CDI's parameterized-type matching of differing type arguments is not
 * reliable across modes, so the platform looks the beans up with the exact type. The
 * pipeline matches models via {@code getModelType()}/{@code getProcessContextType()},
 * never via the generics. The producer method is {@code @Singleton} (deployment
 * services are not client-proxyable).
 */
@ApplicationScoped
public class Camunda8DeploymentServiceProducer {

  @Produces
  @Singleton
  @SuppressWarnings({
      "unchecked", "rawtypes"
  })
  public List<AdapterDeploymentService<Object, Object>> camunda8DeploymentServices(
      final MigrationAdapterProperties properties,
      final Camunda8ClientFactoryRegistry clientFactoryRegistry) {

    return (List) properties
        .adapterTypes()
        .entrySet()
        .stream()
        .filter(adapter -> Camunda8DeploymentService.ADAPTER_TYPE.equals(adapter.getValue()))
        .map(Map.Entry::getKey)
        .sorted()
        .map(adapterId -> new Camunda8DeploymentService(adapterId, clientFactoryRegistry.getFactory(adapterId)))
        .toList();

  }

}
