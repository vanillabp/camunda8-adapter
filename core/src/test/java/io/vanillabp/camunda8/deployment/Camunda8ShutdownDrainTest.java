package io.vanillabp.camunda8.deployment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.client.api.worker.JobWorker;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Story 90 at the level where the shutdown happens: what
 * {@code stopWorkflowProcessing} does with the handlers the closed workers still have
 * inside the application.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8ShutdownDrainTest {

  private static final Duration GRACE = Duration.ofSeconds(2);

  private Camunda8DeploymentService deploymentService(
      final Duration grace) {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setShutdownGrace(grace);
    return new Camunda8DeploymentService(
        "c8", new Camunda8ClientFactory("c8", configuration), new Camunda8DeploymentServiceTest.NoOpInvoker(), (
            module,
            process,
            task) -> io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, Duration
                .ofHours(1), adapterId -> configuration);

  }

  /**
   * The same, with a connection complete enough for a client to be built - building one
   * contacts no cluster, and {@code startWorkflowProcessing} asks for it.
   */
  private Camunda8DeploymentService deploymentServiceWithClient() {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setShutdownGrace(GRACE);
    configuration.setRestAddress("http://localhost:65535");
    return new Camunda8DeploymentService(
        "c8", new Camunda8ClientFactory("c8", configuration), new Camunda8DeploymentServiceTest.NoOpInvoker(), (
            module,
            process,
            task) -> io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, Duration
                .ofHours(1), adapterId -> configuration);

  }

  private JobWorker openWorker() {

    final var worker = mock(JobWorker.class);
    when(worker.isClosed()).thenReturn(false);
    return worker;

  }

  @Test
  @DisplayName("Stopping a module closes its workers and marks it as shutting down")
  public void stoppingMarksTheModuleAndClosesItsWorkers() {

    final var service = deploymentService(GRACE);
    final var context = new Camunda8ProcessingContext("test-module");
    final var worker = openWorker();
    context.getOpenWorkers().add(worker);

    service.stopWorkflowProcessing("test-module", context);

    verify(worker, times(1)).close();
    assertTrue(
        service.drainOf("test-module").isShuttingDown(),
        "a handler failing from here on knows it is the shutdown, not the application");
    assertTrue(context.getOpenWorkers().isEmpty(), "and the workers are gone");

  }

  @Test
  @DisplayName("A handler which comes back within the grace period is waited for, and no longer")
  public void aHandlerWithinTheGraceIsWaitedFor() {

    final var service = deploymentService(GRACE);
    final var context = new Camunda8ProcessingContext("test-module");
    context.getOpenWorkers().add(openWorker());
    final var drain = service.drainOf("test-module");
    drain.jobStarted(4711L, "task", "someTask", "TestProcess");

    final var handler = new Thread(() -> {
      try {
        TimeUnit.MILLISECONDS.sleep(300);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        drain.jobFinished(4711L);
      }
    });
    handler.start();

    final var startedAt = System.nanoTime();
    service.stopWorkflowProcessing("test-module", context);
    final var waited = (System.nanoTime() - startedAt) / 1_000_000;

    assertTrue(waited >= 250, "the shutdown waited for the handler (was "
        + waited
        + " ms)");
    assertTrue(waited < GRACE.toMillis(), "but not the whole grace period (was "
        + waited
        + " ms)");
    assertTrue(drain.getInFlight().isEmpty());

  }

  @Test
  @DisplayName("A handler outliving the grace period is named and left behind")
  public void aHandlerBeyondTheGraceIsNamed(
      final CapturedOutput output) {

    final var service = deploymentService(Duration.ofMillis(300));
    final var context = new Camunda8ProcessingContext("test-module");
    context.getOpenWorkers().add(openWorker());
    final var drain = service.drainOf("test-module");
    drain.jobStarted(4711L, "task", "someTask", "TestProcess");

    service.stopWorkflowProcessing("test-module", context);

    final var logged = output.getOut() + output.getErr();
    assertTrue(logged.contains("4711"), "the operator learns which job was cut off: "
        + logged);
    assertTrue(logged.contains("someTask"), "and which task it belongs to: "
        + logged);
    assertFalse(drain.getInFlight().isEmpty(), "the handler is still running - it was cut off, not stopped");

  }

  @Test
  @DisplayName("A module which starts processing again is not shutting down any more")
  public void aRestartedModuleIsNotShuttingDown() {

    final var service = deploymentServiceWithClient();
    final var context = new Camunda8ProcessingContext("test-module");

    service.stopWorkflowProcessing("test-module", context);
    assertTrue(service.drainOf("test-module").isShuttingDown());

    // a checkpoint and restore, or a platform restarting its lifecycle beans: the module
    // processes again, and a handler failing now is the application's failure once more
    service.startWorkflowProcessing("test-module", context);

    assertFalse(
        service.drainOf("test-module").isShuttingDown(),
        "the new workers report a failed job again");

  }

  @Test
  @DisplayName("A module with nothing in flight is stopped without waiting")
  public void anIdleModuleIsStoppedImmediately() {

    final var service = deploymentService(Duration.ofSeconds(20));
    final var context = new Camunda8ProcessingContext("test-module");
    // the worker still reports open, which a worker whose long poll is in flight does for
    // as long as that request takes - the shutdown must not pay it
    context.getOpenWorkers().add(openWorker());

    final var startedAt = System.nanoTime();
    service.stopWorkflowProcessing("test-module", context);
    final var waited = (System.nanoTime() - startedAt) / 1_000_000;

    assertTrue(waited < 1000, "nothing was waited for (was "
        + waited
        + " ms)");

  }

}
