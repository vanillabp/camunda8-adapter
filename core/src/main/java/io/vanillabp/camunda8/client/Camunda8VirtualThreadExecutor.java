package io.vanillabp.camunda8.client;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The executor of an adapter running <code>worker-threads: virtual</code>. It is handed to
 * the client through {@code CamundaClientBuilder.jobWorkerExecutor(executor, true)}, so the
 * client closes it together with itself.
 * <p>
 * The 8.8 client gives ONE {@link ScheduledExecutorService} two jobs: it schedules the polls
 * of every worker and it runs every handler invocation. This executor separates them the way
 * the 8.9 client does on its own:
 * <ul>
 *   <li>everything the client SCHEDULES (the polls, the stream re-opening) runs on a small
 *       pool of platform threads, so a handler can never delay a poll;</li>
 *   <li>everything the client SUBMITS (the handler invocations) runs on a virtual thread of
 *       its own.</li>
 * </ul>
 * <p>
 * <b>The bound.</b> A virtual thread per submitted task means no limit, and the client's own
 * limit is per worker, so an adapter with fifteen workers would allow fifteen times
 * <code>max-jobs-active</code> transactions at the same time. The submitted task therefore
 * takes a permit of a semaphore before it runs and gives it back afterwards. The permit is
 * taken INSIDE the virtual thread rather than in {@link #execute(Runnable)}: the thread
 * calling that is the client's, and blocking it would stall the delivery of every other
 * worker, which is the defect this whole mode exists to avoid. A virtual thread parked on
 * the semaphore costs almost nothing.
 * <p>
 * <b>Streaming adds a second bound.</b> With <code>stream-enabled: true</code> the client
 * wraps whatever executor it was given in its own semaphore of <code>max-jobs-active</code>
 * permits whose acquire waits for the job timeout. That one is per worker and applies before
 * this one, so with streaming on the effective limit is the smaller of the two.
 */
public class Camunda8VirtualThreadExecutor implements ScheduledExecutorService {

  /**
   * How many platform threads do the timing. Two, because the polls of all workers share
   * them and a scheduled task should not wait for another one which is just being handed
   * over; nothing runs on them longer than that hand-over.
   */
  private static final int SCHEDULING_THREADS = 2;

  private final ScheduledExecutorService scheduling;

  private final ExecutorService handling;

  private final Semaphore slots;

  private final int bound;

  /**
   * @param adapterId The adapter id, used to name the threads
   * @param bound How many handlers may run at the same time
   */
  public Camunda8VirtualThreadExecutor(
      final String adapterId,
      final int bound) {

    this.bound = bound;
    this.slots = new Semaphore(bound);
    this.scheduling = Executors.newScheduledThreadPool(
        SCHEDULING_THREADS,
        runnable -> {
          final var thread = new Thread(runnable, "vanillabp-%s-scheduling".formatted(adapterId));
          thread.setDaemon(true);
          return thread;
        });
    this.handling = Executors.newThreadPerTaskExecutor(
        Thread
            .ofVirtual()
            .name("vanillabp-%s-handler-".formatted(adapterId), 0)
            .factory());

  }

  /**
   * @return How many handlers may run at the same time
   */
  public int getBound() {

    return bound;

  }

  /**
   * How many of the {@link #getBound() bound} are free right now - the number a metric or a
   * test asks for.
   *
   * @return The free execution slots
   */
  public int getFreeSlots() {

    return slots.availablePermits();

  }

  private Runnable bounded(
      final Runnable command) {

    return () -> {
      try {
        slots.acquire();
      } catch (final InterruptedException e) {
        // the client is shutting down: the job was never started, so its lock runs
        // out and the cluster hands it to the next node
        Thread.currentThread().interrupt();
        return;
      }
      try {
        command.run();
      } finally {
        slots.release();
      }
    };

  }

  private <T> Callable<T> bounded(
      final Callable<T> task) {

    return () -> {
      slots.acquire();
      try {
        return task.call();
      } finally {
        slots.release();
      }
    };

  }

  @Override
  public void execute(
      final Runnable command) {

    handling.execute(bounded(command));

  }

  @Override
  public <T> Future<T> submit(
      final Callable<T> task) {

    return handling.submit(bounded(task));

  }

  @Override
  public <T> Future<T> submit(
      final Runnable task,
      final T result) {

    return handling.submit(bounded(task), result);

  }

  @Override
  public Future<?> submit(
      final Runnable task) {

    return handling.submit(bounded(task));

  }

  @Override
  public <T> List<Future<T>> invokeAll(
      final Collection<? extends Callable<T>> tasks) throws InterruptedException {

    return handling.invokeAll(tasks.stream().map(this::bounded).toList());

  }

  @Override
  public <T> List<Future<T>> invokeAll(
      final Collection<? extends Callable<T>> tasks,
      final long timeout,
      final TimeUnit unit) throws InterruptedException {

    return handling.invokeAll(tasks.stream().map(this::bounded).toList(), timeout, unit);

  }

  @Override
  public <T> T invokeAny(
      final Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {

    return handling.invokeAny(tasks.stream().map(this::bounded).toList());

  }

  @Override
  public <T> T invokeAny(
      final Collection<? extends Callable<T>> tasks,
      final long timeout,
      final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

    return handling.invokeAny(tasks.stream().map(this::bounded).toList(), timeout, unit);

  }

  @Override
  public ScheduledFuture<?> schedule(
      final Runnable command,
      final long delay,
      final TimeUnit unit) {

    return scheduling.schedule(command, delay, unit);

  }

  @Override
  public <V> ScheduledFuture<V> schedule(
      final Callable<V> callable,
      final long delay,
      final TimeUnit unit) {

    return scheduling.schedule(callable, delay, unit);

  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      final Runnable command,
      final long initialDelay,
      final long period,
      final TimeUnit unit) {

    return scheduling.scheduleAtFixedRate(command, initialDelay, period, unit);

  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      final Runnable command,
      final long initialDelay,
      final long delay,
      final TimeUnit unit) {

    return scheduling.scheduleWithFixedDelay(command, initialDelay, delay, unit);

  }

  @Override
  public void shutdown() {

    scheduling.shutdown();
    handling.shutdown();

  }

  @Override
  public List<Runnable> shutdownNow() {

    final var pending = new java.util.ArrayList<>(scheduling.shutdownNow());
    pending.addAll(handling.shutdownNow());
    return pending;

  }

  @Override
  public boolean isShutdown() {

    return scheduling.isShutdown() && handling.isShutdown();

  }

  @Override
  public boolean isTerminated() {

    return scheduling.isTerminated() && handling.isTerminated();

  }

  @Override
  public boolean awaitTermination(
      final long timeout,
      final TimeUnit unit) throws InterruptedException {

    final var deadline = System.nanoTime() + unit.toNanos(timeout);
    final var schedulingTerminated = scheduling.awaitTermination(timeout, unit);
    final var remaining = Math.max(0, deadline - System.nanoTime());
    return handling.awaitTermination(remaining, TimeUnit.NANOSECONDS) && schedulingTerminated;

  }

}
