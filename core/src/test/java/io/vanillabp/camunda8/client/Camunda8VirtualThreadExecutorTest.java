package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The executor of the virtual-thread mode: what the client submits runs on a virtual
 * thread and never more of it at once than the bound allows, while what the client
 * SCHEDULES keeps running even when every slot is taken - which is the property the
 * 8.8 line is missing on its own.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8VirtualThreadExecutorTest {

  @Test
  @DisplayName("a submitted handler runs on a virtual thread")
  public void submittedWorkRunsOnAVirtualThread() throws Exception {

    final var executor = new Camunda8VirtualThreadExecutor("c8", 4);
    try {
      final var virtual = new AtomicBoolean();
      final var name = new java.util.concurrent.atomic.AtomicReference<String>();
      final var ran = new CountDownLatch(1);

      executor.execute(() -> {
        virtual.set(Thread.currentThread().isVirtual());
        name.set(Thread.currentThread().getName());
        ran.countDown();
      });

      assertTrue(ran.await(5, TimeUnit.SECONDS), "the handler ran");
      assertTrue(virtual.get(), "the handler runs on a virtual thread");
      assertTrue(name.get().startsWith("vanillabp-c8-handler-"),
          "the thread is named after the adapter, but was: "
              + name.get());
    } finally {
      executor.shutdownNow();
    }

  }

  @Test
  @DisplayName("the bound holds with more concurrent jobs than the bound")
  public void theBoundHoldsUnderMoreJobsThanSlots() throws Exception {

    final var bound = 3;
    final var jobs = 24;
    final var executor = new Camunda8VirtualThreadExecutor("c8", bound);
    try {
      final var running = new AtomicInteger();
      final var peak = new AtomicInteger();
      final var release = new CountDownLatch(1);
      final var started = new CountDownLatch(bound);
      final var finished = new CountDownLatch(jobs);

      for (int job = 0; job < jobs; job++) {
        executor.execute(() -> {
          peak.accumulateAndGet(running.incrementAndGet(), Math::max);
          started.countDown();
          try {
            release.await(5, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            running.decrementAndGet();
            finished.countDown();
          }
        });
      }

      assertTrue(started.await(5, TimeUnit.SECONDS), "the bound is used");
      assertEquals(0, executor.getFreeSlots(), "every slot is taken while the handlers block");
      release.countDown();
      assertTrue(finished.await(10, TimeUnit.SECONDS), "every job ran");
      assertEquals(bound, peak.get(), "never more handlers at once than the bound allows");
      assertEquals(bound, executor.getFreeSlots(), "the slots are given back");
    } finally {
      executor.shutdownNow();
    }

  }

  @Test
  @DisplayName("a scheduled poll runs on a platform thread while every slot is blocked")
  public void schedulingIsNotStarvedByBlockedHandlers() throws Exception {

    final var bound = 2;
    final var executor = new Camunda8VirtualThreadExecutor("c8", bound);
    try {
      final var release = new CountDownLatch(1);
      final var blocking = new CountDownLatch(bound);
      for (int job = 0; job < bound * 4; job++) {
        executor.execute(() -> {
          blocking.countDown();
          try {
            release.await(5, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
      }
      assertTrue(blocking.await(5, TimeUnit.SECONDS), "the handlers took every slot");

      final var polled = new CountDownLatch(1);
      final var pollThreadIsVirtual = new AtomicBoolean(true);
      executor.schedule(() -> {
        pollThreadIsVirtual.set(Thread.currentThread().isVirtual());
        polled.countDown();
      }, 10, TimeUnit.MILLISECONDS);

      assertTrue(polled.await(2, TimeUnit.SECONDS),
          "a scheduled poll runs although every execution slot is blocked");
      assertFalse(pollThreadIsVirtual.get(), "the timing runs on a platform thread");
      release.countDown();
    } finally {
      executor.shutdownNow();
    }

  }

  @Test
  @DisplayName("shutdown ends both halves")
  public void shutdownEndsBothHalves() throws Exception {

    final var executor = new Camunda8VirtualThreadExecutor("c8", 2);

    executor.shutdown();

    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "both halves terminate");
    assertTrue(executor.isShutdown());
    assertTrue(executor.isTerminated());

  }

}
