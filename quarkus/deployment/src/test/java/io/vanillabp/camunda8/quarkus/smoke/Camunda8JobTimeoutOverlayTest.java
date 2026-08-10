package io.vanillabp.camunda8.quarkus.smoke;

import java.time.Duration;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.config.SmallRyeConfig;
import io.vanillabp.camunda8.quarkus.runtime.VanillaBpCamunda8Properties;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Proves the job timeout resolves through all FOUR configuration levels of the
 * Quarkus overlay mapping (story 21c) - task, workflow, workflow-module and
 * adapter level, most specific wins - and that the async-task-timeout is read at
 * adapter level. No BPMN resources are deployed (the workflow module's
 * resources-location is empty), so the boot succeeds without a cluster.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8JobTimeoutOverlayTest {

  @RegisterExtension
  static final QuarkusExtensionTest extensionTest = new QuarkusExtensionTest()
      .setArchiveProducer(() -> ShrinkWrap
          .create(JavaArchive.class)
          .addClass(Aggregate.class)
          .addClass(SampleWorkflowService.class)
          .addClass(TestPhaseTwoOutbox.class)
          .addAsResource("overlay/application.yaml", "application.yaml")
          .addAsResource("workflow-module-descriptor/workflow-module", "META-INF/workflow-module"));

  private VanillaBpCamunda8Properties overlay() {

    return ConfigProvider
        .getConfig()
        .unwrap(SmallRyeConfig.class)
        .getConfigMapping(VanillaBpCamunda8Properties.class);

  }

  @Test
  public void jobTimeoutResolvesThroughAllFourLevels() {

    final var overlay = overlay();

    // task level (most specific)
    Assertions.assertEquals(
        Duration.ofSeconds(2),
        overlay.jobTimeoutFor("test-app", "TaskProcess", "happyTask", "c8"));
    // workflow level
    Assertions.assertEquals(
        Duration.ofSeconds(10),
        overlay.jobTimeoutFor("test-app", "TaskProcess", "otherTask", "c8"));
    // workflow-module level
    Assertions.assertEquals(
        Duration.ofSeconds(20),
        overlay.jobTimeoutFor("test-app", "OtherProcess", "someTask", "c8"));
    // adapter level (base)
    Assertions.assertEquals(
        Duration.ofSeconds(30),
        overlay.jobTimeoutFor("unknown-module", "SomeProcess", "someTask", "c8"));

  }

  @Test
  public void defaultsApplyWithoutAnyConfiguredTimeout() {

    final var overlay = overlay();

    // an adapter id without any configured job-timeout falls back to the default
    Assertions.assertEquals(
        io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT,
        overlay.jobTimeoutFor("test-app", "TaskProcess", "happyTask", "unknown-adapter"));
    // the async-task-timeout is an adapter-level key
    Assertions.assertEquals(
        Duration.ofMinutes(2),
        overlay
            .adapters()
            .get("c8")
            .asyncTaskTimeout()
            .orElseThrow());

  }

}
