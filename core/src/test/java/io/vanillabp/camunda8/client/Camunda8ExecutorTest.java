package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What both execution models promise, asserted against each of them: a bound which holds,
 * and a scheduled task which asks the cluster for work only while a slot is free.
 * <p>
 * The second one is the designed back pressure. Without it a worker whose slots are all
 * busy keeps activating jobs which then wait in front of the slots, spending the lock they
 * were handed out with; with it the poll waits instead, and the job stays at the cluster,
 * where another node can take it.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8ExecutorTest {

  /**
   * The two models, each as a way of building an executor of a given width.
   *
   * @return The models under test, named after the value which configures them
   */
  static Stream<Arguments> executionModels() {

    return Stream.of(
        Arguments.of(
            "worker-threads: <number>",
            (IntFunction<Camunda8Executor>) bound -> new Camunda8PlatformThreadExecutor("c8", bound)),
        Arguments.of(
            "worker-threads: virtual",
            (IntFunction<Camunda8Executor>) bound -> new Camunda8VirtualThreadExecutor("c8", bound)));

  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("executionModels")
  public void neverMoreHandlersAtOnceThanTheBound(
      final String model,
      final IntFunction<Camunda8Executor> executionModel) throws Exception {

    final var bound = 3;
    final var jobs = bound * 8;
    final var executor = executionModel.apply(bound);
    try {
      final var running = new AtomicInteger();
      final var peak = new AtomicInteger();
      final var release = new CountDownLatch(1);
      final var everySlotTaken = new CountDownLatch(bound);
      final var finished = new CountDownLatch(jobs);

      for (int job = 0; job < jobs; job++) {
        executor.execute(() -> {
          peak.accumulateAndGet(running.incrementAndGet(), Math::max);
          everySlotTaken.countDown();
          try {
            release.await(10, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            running.decrementAndGet();
            finished.countDown();
          }
        });
      }

      assertTrue(everySlotTaken.await(5, TimeUnit.SECONDS), "the bound is used");
      assertEquals(0, executor.getFreeSlots(), "no slot is free while the handlers block");
      assertTrue(waitingReaches(executor, jobs - bound), "the jobs which found no slot are counted");
      release.countDown();
      assertTrue(finished.await(10, TimeUnit.SECONDS), "every job ran");
      assertEquals(bound, peak.get(), "never more handlers at once than the bound allows");
    } finally {
      executor.shutdownNow();
    }

  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("executionModels")
  public void aScheduledPollWaitsUntilASlotIsFree(
      final String model,
      final IntFunction<Camunda8Executor> executionModel) throws Exception {

    final var bound = 2;
    final var executor = executionModel.apply(bound);
    try {
      final var release = new CountDownLatch(1);
      takeEverySlot(executor, bound, release);

      final var polled = new CountDownLatch(1);
      executor.schedule(polled::countDown, 1, TimeUnit.MILLISECONDS);

      assertFalse(
          polled.await(Camunda8Executor.LOOK_FOR_A_SLOT_AGAIN_MILLIS * 5, TimeUnit.MILLISECONDS),
          "nothing asks the cluster for work while there is nothing to run it on");
      assertEquals(0, executor.getFreeSlots(), "the handlers are still inside their slots");

      release.countDown();

      assertTrue(polled.await(5, TimeUnit.SECONDS), "and the poll happens as soon as a slot is free");
    } finally {
      executor.shutdownNow();
    }

  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("executionModels")
  public void cancellingAWaitingPollReachesTheAttemptItWaitsIn(
      final String model,
      final IntFunction<Camunda8Executor> executionModel) throws Exception {

    final var bound = 1;
    final var executor = executionModel.apply(bound);
    try {
      final var release = new CountDownLatch(1);
      takeEverySlot(executor, bound, release);

      final var polled = new CountDownLatch(1);
      final var poll = executor.schedule(polled::countDown, 1, TimeUnit.MILLISECONDS);

      // the poll is waiting for a slot now, which is the state a caller cancelling its
      // own scheduled task has to reach: the 8.10 client cancels the tasks around a job
      // stream that way, and it holds the attempt which happens to be pending
      Thread.sleep(Camunda8Executor.LOOK_FOR_A_SLOT_AGAIN_MILLIS * 2);
      assertTrue(poll.cancel(false), "the waiting poll is cancelled");
      assertTrue(poll.isCancelled());

      release.countDown();

      assertFalse(
          polled.await(Camunda8Executor.LOOK_FOR_A_SLOT_AGAIN_MILLIS * 5, TimeUnit.MILLISECONDS),
          "a cancelled poll does not run when a slot becomes free either");
    } finally {
      executor.shutdownNow();
    }

  }

  /**
   * Fills every execution slot with a handler which stays inside until the latch is
   * counted down.
   *
   * @param executor The executor under test
   * @param bound How many slots it has
   * @param release What ends the handlers
   */
  private static void takeEverySlot(
      final Camunda8Executor executor,
      final int bound,
      final CountDownLatch release) throws InterruptedException {

    final var everySlotTaken = new CountDownLatch(bound);
    for (int job = 0; job < bound; job++) {
      executor.execute(() -> {
        everySlotTaken.countDown();
        try {
          release.await(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }
    assertTrue(everySlotTaken.await(5, TimeUnit.SECONDS), "the handlers took every slot");

  }

  /**
   * Waits until as many handlers as expected wait for a slot - they are counted where they
   * wait, and a job submitted a moment ago may not have got there yet.
   *
   * @param executor The executor under test
   * @param expected How many are waiting once every submitted job arrived
   * @return Whether the number was reached within five seconds
   */
  private static boolean waitingReaches(
      final Camunda8Executor executor,
      final int expected) throws InterruptedException {

    final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (executor.getWaiting() == expected) {
        return true;
      }
      Thread.sleep(10);
    }
    return executor.getWaiting() == expected;

  }

}
