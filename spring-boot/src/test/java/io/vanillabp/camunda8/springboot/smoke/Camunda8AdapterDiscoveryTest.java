package io.vanillabp.camunda8.springboot.smoke;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.camunda8.processservice.Camunda8ProcessService;
import io.vanillabp.integration.adapter.spi.AdapterDeploymentService;
import io.vanillabp.integration.adapter.spi.MigratableProcessService;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Smoke test proving the Camunda 8 adapter is discovered on Spring Boot when configured
 * ({@code vanillabp.adapters.c8: camunda8}), without any Camunda 8 cluster or Docker: no
 * BPMN files are provided and the deployment lifecycle is disabled, so the not-yet
 * implemented deployment pipeline (which throws by design) is never exercised.
 * <p>
 * {@code DeploymentAutoConfiguration} is excluded because it runs the deployment
 * pipeline on context start; deploying resources is a later story.
 */
@ExtendWith(SuppressOutputExtension.class)
@SpringBootTest(
    classes = SmokeTestApplication.class,
    properties = "spring.autoconfigure.exclude=io.vanillabp.integration.deployment.DeploymentAutoConfiguration")
public class Camunda8AdapterDiscoveryTest {

  @Autowired
  private ApplicationContext context;

  @Autowired
  private MigratableProcessService<?> migratableProcessService;

  @Test
  public void adapterIsDiscovered() {

    // the deployment-service list bean contains exactly one Camunda 8 deployment
    // service for the configured adapter id 'c8'
    @SuppressWarnings("unchecked")
    final var deploymentServices = context.getBean(
        "camunda8DeploymentServices", List.class);
    Assertions.assertEquals(1, deploymentServices.size(),
        "expected exactly one Camunda 8 deployment service");

    final var deploymentService = (AdapterDeploymentService<?, ?>) deploymentServices.get(0);
    Assertions.assertInstanceOf(Camunda8DeploymentService.class, deploymentService);
    Assertions.assertEquals("c8", deploymentService.getAdapterId());
    Assertions.assertEquals("camunda8", deploymentService.getAdapterType());

    // the process service of the adapter is discovered and requires a two-phase commit
    // (Camunda 8 is a remote engine)
    Assertions.assertInstanceOf(Camunda8ProcessService.class, migratableProcessService);
    Assertions.assertEquals("c8", migratableProcessService.getAdapterId());
    Assertions.assertTrue(migratableProcessService.needsTwoPhaseCommitForStartingWorkflows());

  }

}
