package io.vanillabp.camunda8.quarkus.smoke;

import java.util.List;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusUnitTest;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.integration.adapter.spi.AdapterDeploymentService;
import io.vanillabp.integration.adapter.spi.MigratableProcessService;
import io.vanillabp.integration.runtime.processservice.ProcessServiceBaseCdiBean;
import io.vanillabp.spi.process.ProcessService;
import jakarta.inject.Inject;

/**
 * Smoke test proving the Camunda 8 adapter is discovered on Quarkus when configured
 * ({@code vanillabp.adapters.c8: camunda8}), without any Camunda 8 cluster or Docker: no
 * BPMN files are provided and no workflow is started, so neither the deployment pipeline
 * nor a client/cluster is required.
 */
public class Camunda8AdapterDiscoveryTest {

  @RegisterExtension
  static final QuarkusUnitTest unitTest = new QuarkusUnitTest()
      .setArchiveProducer(() -> ShrinkWrap
          .create(JavaArchive.class)
          .addClass(Aggregate.class)
          .addClass(SampleWorkflowService.class)
          .addClass(TestPhaseTwoOutbox.class)
          .addAsResource("application.yaml")
          .addAsResource("workflow-module-descriptor/workflow-module", "META-INF/workflow-module"));

  @Inject
  @SuppressWarnings("CdiUnsatisfiedInjection")
  ProcessService<Aggregate> sampleProcessService;

  // the per-adapter-id shape: ONE List bean with one instance per configured id
  @Inject
  List<MigratableProcessService<Object>> migratableProcessServices;

  @Inject
  Camunda8ClientFactoryRegistry clientFactoryRegistry;

  // the platform contract: the List's element type parameters are literally Object
  @Inject
  List<AdapterDeploymentService<Object, Object>> deploymentServices;

  @Test
  public void adapterIsDiscovered() {

    Assertions.assertInstanceOf(ProcessServiceBaseCdiBean.class, sampleProcessService);

    @SuppressWarnings("unchecked")
    final var processServiceBaseBean = (ProcessServiceBaseCdiBean<Aggregate>) sampleProcessService;

    // the Camunda 8 adapter 'c8' is discovered and configured for the workflow module
    final var adaptersConfigured = processServiceBaseBean
        .getMigrationProcessService()
        .getAdapters();
    Assertions.assertEquals(1, adaptersConfigured.size());
    final var adapterId = adaptersConfigured.keySet().iterator().next();
    Assertions.assertEquals("c8", adapterId);
    Assertions.assertEquals("camunda8", adaptersConfigured.get(adapterId));

    // the process service of the adapter is discovered (one instance per configured
    // adapter id) and requires a two-phase commit (Camunda 8 is a remote engine)
    Assertions.assertEquals(1, migratableProcessServices.size());
    final var migratableProcessService = migratableProcessServices.getFirst();
    Assertions.assertEquals("c8", migratableProcessService.getAdapterId());
    Assertions.assertTrue(migratableProcessService.needsTwoPhaseCommitForStartingWorkflows());

    // the client-factory registry and the deployment service are produced (client and
    // deployment-service creation are covered here; a Docker-based start test is not
    // practical in a QuarkusUnitTest - see the Spring Boot module's
    // Camunda8DeploymentAndStartIT and the README)
    Assertions.assertNotNull(clientFactoryRegistry.getFactory("c8"));
    Assertions.assertEquals(1, deploymentServices.size());
    Assertions.assertEquals("c8", deploymentServices.getFirst().getAdapterId());
    Assertions.assertEquals("camunda8", deploymentServices.getFirst().getAdapterType());

  }

}
