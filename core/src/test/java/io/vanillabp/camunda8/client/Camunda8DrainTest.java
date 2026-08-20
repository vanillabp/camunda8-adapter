package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Story 90: what the drain of one workflow module does while the application goes down.
 * The two halves are tested separately here - the waiting, and the rule which keeps a
 * failure of the shutdown from being reported as a failure of the application.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8DrainTest {

  private final Camunda8Drain drain = new Camunda8Drain("c8", "test-module");

  @Test
  @DisplayName("A drain with nothing in flight returns immediately")
  public void anEmptyDrainReturnsImmediately() {

    final var startedAt = System.nanoTime();

    assertTrue(drain.awaitDrained(Duration.ofSeconds(20), () -> false));

    final var waited = (System.nanoTime() - startedAt) / 1_000_000;
    assertTrue(
        waited < 1000,
        "a shutdown with nothing to wait for pays nothing, even where the workers still report open (was "
            + waited
            + " ms)");

  }

  @Test
  @DisplayName("The drain waits for a handler which finishes within the grace period")
  public void aHandlerWithinTheGraceIsWaitedFor() throws Exception {

    drain.jobStarted(4711L, "task", "someTask", "TestProcess");
    final var entered = new CountDownLatch(1);
    final var handler = new Thread(() -> {
      try {
        entered.countDown();
        TimeUnit.MILLISECONDS.sleep(300);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        drain.jobFinished(4711L);
      }
    });
    handler.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "the handler started");

    assertTrue(
        drain.awaitDrained(Duration.ofSeconds(20), () -> true),
        "the handler came back before the grace period passed");
    assertTrue(drain.getInFlight().isEmpty(), "and nothing is left in flight");

    handler.join(TimeUnit.SECONDS.toMillis(5));

  }

  @Test
  @DisplayName("A handler longer than the grace period is given up on and named per job")
  public void aHandlerBeyondTheGraceIsReported() {

    drain.jobStarted(4711L, "task", "someTask", "TestProcess");
    drain.jobStarted(4712L, "user-task listener", "someUserTask", "TestProcess");

    final var startedAt = System.nanoTime();
    assertFalse(
        drain.awaitDrained(Duration.ofMillis(300), () -> true),
        "the handlers did not come back");
    final var waited = (System.nanoTime() - startedAt) / 1_000_000;
    assertTrue(waited >= 300, "the grace period was granted before giving up (was "
        + waited
        + " ms)");

    assertEquals(2, drain.getInFlight().size(), "both are still in flight");

  }

  @Test
  @DisplayName("A grace of zero closes without waiting at all")
  public void aGraceOfZeroDoesNotWait() {

    drain.jobStarted(4711L, "task", "someTask", "TestProcess");

    final var startedAt = System.nanoTime();
    assertFalse(drain.awaitDrained(Duration.ZERO, () -> true));
    assertTrue((System.nanoTime() - startedAt) / 1_000_000 < 200, "nothing was waited for");

  }

  @Test
  @DisplayName("Outside a shutdown a failure stays the application's failure")
  public void aFailureOutsideAShutdownIsReported() {

    assertFalse(drain.isShuttingDown());
    assertFalse(
        drain.leaveJobToItsLock(4711L, "task", "someTask", new IllegalStateException("boom")),
        "the caller has to fail the job");

  }

  @Test
  @DisplayName("While shutting down a failure leaves the job to its lock")
  public void aFailureDuringTheShutdownIsTheShutdown(
      final CapturedOutput output) {

    drain.beginShutdown();

    assertTrue(drain.isShuttingDown());
    assertTrue(
        drain.leaveJobToItsLock(4711L, "task", "someTask", new InterruptedException()),
        "the caller has to leave the job alone");
    assertTrue(
        drain.leaveJobToItsLock(4712L, "task", "someTask", null),
        "a failure without an exception is treated the same way");

    final var logged = output.getOut() + output.getErr();
    assertTrue(logged.contains("4711"), "the job key is named, so the operator can find it: "
        + logged);
    assertTrue(logged.contains("someTask"), "and the task with it: "
        + logged);
    assertTrue(logged.contains("retries intact"), "and what happens to it: "
        + logged);

  }

  @Test
  @DisplayName("What is still running when the grace passed is reported once per job")
  public void whatWasCutOffIsNamed(
      final CapturedOutput output) {

    drain.jobStarted(4711L, "task", "someTask", "TestProcess");
    drain.jobStarted(4712L, "task", "anotherTask", "TestProcess");

    drain.reportCutOff(Duration.ofSeconds(20));

    final var logged = output.getOut() + output.getErr();
    assertTrue(logged.contains("4711") && logged.contains("4712"), "every job is named: "
        + logged);
    assertTrue(logged.contains("someTask") && logged.contains("anotherTask"), "with its task: "
        + logged);
    assertTrue(
        logged.contains("vanillabp.adapters.c8.shutdown-grace"),
        "and the property to raise where a handler legitimately runs that long: "
            + logged);
    assertTrue(
        logged.contains("terminationGracePeriodSeconds"),
        "together with what else has to be raised then: "
            + logged);

    assertEquals(2, drain.getInFlight().size(), "reporting does not change what is in flight");
    assertTrue(
        drain
            .getInFlight()
            .stream()
            .allMatch(job -> job.since() != null),
        "every entry knows since when it runs");

  }

}
