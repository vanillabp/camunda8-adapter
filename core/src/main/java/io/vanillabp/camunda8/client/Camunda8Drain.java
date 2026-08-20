package io.vanillabp.camunda8.client;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import lombok.extern.slf4j.Slf4j;

/**
 * What one workflow module of one adapter instance does with the work it has in flight
 * while the application is going down (story 90).
 * <p>
 * The Camunda client does not drain: {@code JobWorker#close()} returns without waiting
 * for the handlers of the jobs the worker already activated, and {@code
 * CamundaClient#close()} shuts its executor down with an interrupt, measured at two
 * milliseconds after the call. An interrupted handler throws, the adapter used to answer
 * every failure with {@code newFailCommand(retries - 1)}, and so an ordinary restart cost
 * one retry per job in flight - a rolling restart across replicas could walk a job into an
 * incident which has nothing to do with the business logic.
 * <p>
 * This object is the answer, and it does two things. It counts what is in flight: every
 * handler of the module registers the job it is running and deregisters it afterwards, so
 * the shutdown can wait for the handlers to come back ({@link #awaitDrained}) before the
 * client is closed under them, and what is still running when the grace period passed is
 * named per job so an operator knows what was cut off. And it carries the module's state:
 * while it is shutting down, a failing delivery is not the application's fault and is not
 * reported as one ({@link #leaveJobToItsLock}). Such a job keeps its lock, the cluster
 * hands it out again when the lock expires, its retries untouched, and VanillaBP's
 * delivery record decides whether the work has to run again.
 * <p>
 * <b>Why the drain waits for the handlers rather than for the workers.</b>
 * {@code JobWorker#isClosed()} is the client's own answer and it means three things at
 * once: the worker was closed, no activation request is in flight, and no activated job is
 * left. The middle one is the problem. A worker long-polls with the request timeout (ten
 * seconds by default), and closing it does not cancel the request in flight, so a worker
 * which runs nothing at all keeps reporting {@code false} for as long as that request
 * takes. Waiting for it would mean paying that on every shutdown, for nothing. What the
 * retries are burnt by is a handler which is INSIDE the application's code, so that is what
 * this waits for; the workers' own state is reported alongside it. Jobs activated but never
 * handed to a handler need no drain either - nothing failed them, so they simply run out of
 * their lock and come back.
 */
@Slf4j
public class Camunda8Drain {

  /**
   * How often the drain looks whether the handlers came back. Short enough not to add a
   * noticeable delay to a shutdown which has nothing to wait for.
   */
  static final long POLL_MILLIS = 50;

  private final String adapterId;

  private final String workflowModuleId;

  /**
   * The jobs whose handler is running right now, keyed by job key.
   */
  private final Map<Long, InFlightJob> inFlight = new ConcurrentHashMap<>();

  private volatile boolean shuttingDown;

  /**
   * @param adapterId The adapter instance this belongs to
   * @param workflowModuleId The workflow module whose workers are drained
   */
  public Camunda8Drain(
      final String adapterId,
      final String workflowModuleId) {

    this.adapterId = adapterId;
    this.workflowModuleId = workflowModuleId;

  }

  /**
   * A delivery whose handler is running right now.
   *
   * @param jobKey The job key the cluster handed out
   * @param kind What kind of worker delivered it (for the message)
   * @param name The task definition respectively the job type, as the application knows it
   * @param bpmnProcessId The BPMN process, as the application knows it
   * @param since When the handler entered
   */
  public record InFlightJob(
                            long jobKey,
                            String kind,
                            String name,
                            String bpmnProcessId,
                            Instant since) {
  }

  /**
   * Whether the workers of this workflow module are going down. Set before the workers are
   * closed, so a handler which is interrupted by the shutdown can tell the difference
   * between its own failure and the shutdown.
   *
   * @return Whether shutdown began
   */
  public boolean isShuttingDown() {

    return shuttingDown;

  }

  /**
   * Marks the module as shutting down. Called by {@code stopWorkflowProcessing} BEFORE the
   * workers are closed - a job failing between the flag and the close is already a job the
   * shutdown cut off.
   */
  public void beginShutdown() {

    shuttingDown = true;

  }

  /**
   * Registers the delivery a handler just entered.
   *
   * @param jobKey The job key
   * @param kind What kind of worker delivered it
   * @param name The task definition respectively the job type
   * @param bpmnProcessId The BPMN process
   */
  public void jobStarted(
      final long jobKey,
      final String kind,
      final String name,
      final String bpmnProcessId) {

    inFlight.put(jobKey, new InFlightJob(jobKey, kind, name, bpmnProcessId, Instant.now()));

  }

  /**
   * Deregisters a delivery whose handler returned - in a {@code finally}, so a handler
   * which threw is gone from the drain as well.
   *
   * @param jobKey The job key
   */
  public void jobFinished(
      final long jobKey) {

    inFlight.remove(jobKey);

  }

  /**
   * @return The deliveries whose handler is running right now
   */
  public Collection<InFlightJob> getInFlight() {

    return List.copyOf(inFlight.values());

  }

  /**
   * Waits until no handler of this workflow module is running any more, or until the grace
   * period passed.
   *
   * @param grace How long the shutdown waits for the handlers
   * @param workersClosed What the workers themselves report (see the class javadoc: it is
   *          logged, not waited for)
   * @return Whether everything came back within the grace period
   */
  public boolean awaitDrained(
      final Duration grace,
      final BooleanSupplier workersClosed) {

    final var startedAt = System.nanoTime();
    final var deadline = startedAt + Math.max(0, grace.toNanos());
    while (true) {
      if (inFlight.isEmpty()) {
        log.info(
            "Camunda8[{}]: workflow module '{}' drained after {} ms (workers closed: {})",
            adapterId,
            workflowModuleId,
            (System.nanoTime() - startedAt) / 1_000_000,
            workersClosed.getAsBoolean());
        return true;
      }
      if (System.nanoTime() >= deadline) {
        return false;
      }
      try {
        Thread.sleep(POLL_MILLIS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return inFlight.isEmpty();
      }
    }

  }

  /**
   * Names what is still running after the grace period passed, once per job: those handlers
   * are about to be interrupted by the closing client, and their jobs stay locked until the
   * cluster hands them out again.
   *
   * @param grace The grace period which passed
   */
  public void reportCutOff(
      final Duration grace) {

    getInFlight()
        .forEach(job -> log.warn(
            """
                Camunda8[{}]: the {} '{}' of BPMN process '{}' (job {}, workflow module '{}') was still \
                running after the shutdown waited {} for it and is being cut off. The job keeps its \
                retries and is redelivered once its lock expires. Raise '{}' where handlers legitimately \
                run that long - and raise the shutdown budget of the runtime with it \
                ('spring.lifecycle.timeout-per-shutdown-phase', Kubernetes' \
                'terminationGracePeriodSeconds'), which is what this grace stays below.""",
            adapterId,
            job.kind(),
            job.name(),
            job.bpmnProcessId(),
            job.jobKey(),
            workflowModuleId,
            grace,
            Camunda8AdapterConfiguration.propertyKey(adapterId, "shutdown-grace")));

  }

  /**
   * Whether a failed delivery is the shutdown rather than the application - the rule being
   * the adapter's STATE and not the kind of exception: a handler interrupted by the closing
   * client throws whatever the interrupt made it throw, and none of those exceptions is
   * distinguishable from a genuine failure which happened to occur at the same moment.
   * <p>
   * Where it is the shutdown, the job is deliberately left to its lock: no
   * {@code newFailCommand}, so the cluster redelivers it after the lock expires with its
   * retries intact. It is logged at INFO with the job key, because a job left behind by a
   * restart is a normal event but not an invisible one.
   *
   * @param jobKey The job key
   * @param kind What kind of worker delivered it
   * @param name The task definition respectively the job type
   * @param failure What the handler threw
   * @return Whether the caller has to leave the job alone
   */
  public boolean leaveJobToItsLock(
      final long jobKey,
      final String kind,
      final String name,
      final Throwable failure) {

    if (!shuttingDown) {
      return false;
    }
    log.info(
        "Camunda8[{}]: the {} '{}' (job {}, workflow module '{}') did not finish before the adapter shut "
            + "down. The job is left to its lock instead of being failed, so the cluster redelivers it "
            + "with its retries intact and VanillaBP answers the redelivery from its delivery record. "
            + "Reason: {}",
        adapterId,
        kind,
        name,
        jobKey,
        workflowModuleId,
        failure == null
            ? "none given"
            : failure.toString());
    return true;

  }

}
