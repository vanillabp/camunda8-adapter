package io.vanillabp.camunda8.springboot.smoke;

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
 * BPMN files are provided and the deployment lifecycle is disabled, so no cluster
 * connection is needed.
 * <p>
 * {@code DeploymentAutoConfiguration} is excluded because it runs the deployment
 * pipeline on context start - real deployment against a cluster is covered by the
 * Testcontainers-based ITs.
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

    // element-bean convention: one AdapterDeploymentService bean per adapter
    // (never a List bean) so several adapter types can coexist
    final var deploymentService = context.getBean(AdapterDeploymentService.class);
    Assertions.assertInstanceOf(Camunda8DeploymentService.class, deploymentService);
    Assertions.assertEquals("c8", deploymentService.getAdapterId());
    Assertions.assertEquals("camunda8", deploymentService.getAdapterType());

    // the process service of the adapter is discovered
    Assertions.assertInstanceOf(Camunda8ProcessService.class, migratableProcessService);
    Assertions.assertEquals("c8", migratableProcessService.getAdapterId());

  }

  @Test
  public void anAdapterWithoutAConnectionIsNotUnhealthy() {

    // This application configures the adapter but no cluster to talk to, which
    // is a legitimate state of a setup in progress. The health endpoint has to say so
    // instead of reporting an outage on top of the guiding warning the boot already gave
    final var deploymentService = context.getBean(AdapterDeploymentService.class);
    final var health = deploymentService.checkHealth();

    Assertions.assertNotNull(health, "an adapter which can check something has to answer");
    Assertions.assertEquals(
        io.vanillabp.integration.adapter.spi.health.AdapterHealth.Status.UNKNOWN,
        health.status());
    Assertions.assertEquals("c8", health.adapterId());
    Assertions.assertTrue(
        health.description().contains("not configured"),
        "and it says why: "
            + health.description());

  }

}
