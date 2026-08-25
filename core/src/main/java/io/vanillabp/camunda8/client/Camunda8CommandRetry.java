package io.vanillabp.camunda8.client;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import lombok.extern.slf4j.Slf4j;

/**
 * The bounded retry around the command a job handler sends BACK to the cluster - the
 * completion, the BPMN error, the failure and the lock renewal of an open asynchronous
 * task.
 * <p>
 * <b>Why it exists.</b> A command the cluster rejects because it is busy arrives as
 * <code>RESOURCE_EXHAUSTED</code> on gRPC and as HTTP 503 on REST, and nothing in the
 * Camunda client repeats either of them: the gRPC retry policy is switched off by default,
 * and even switched on it is a channel setting which does nothing for the REST transport
 * this adapter prefers. Measured against a single node, 19.433 of 20.000 gRPC commands came
 * back rejected. Phase-two commands survive that because the outbox repeats them; the
 * command inside a job handler had nobody to repeat it, so a rejected completion of work
 * which was already committed escaped into the client's fail path and cost the job one of
 * its retries. Under sustained load that walks a job into an incident although every
 * attempt of it succeeded.
 * <p>
 * <b>What bounds it.</b> Two things, and the tighter one wins:
 * <ul>
 * <li><b>the job's lock.</b> Retrying past it hands the job to somebody else, so the
 * remaining lock is the real deadline. It is read from the job's own deadline rather than
 * from the configured timeout, which says how long the lock was granted for and not how
 * much of it is left;</li>
 * <li><b>{@value #MAX_ATTEMPTS} attempts.</b> A handler waiting for a cluster to calm down
 * occupies an execution slot (see
 * {@link io.vanillabp.camunda8.client.Camunda8ExecutionModel}), and a slot which waits
 * delivers nothing. Five attempts spread over less than half a second are a hiccup; more
 * than that is a cluster problem an operator has to see rather than one a worker should
 * sit out.</li>
 * </ul>
 * The backoff is exponential and shaped like the client's own activation backoff
 * ({@code ExponentialBackoffBuilderImpl}): {@value #INITIAL_BACKOFF_MILLIS} ms initially,
 * factor {@value #BACKOFF_FACTOR}, a ceiling of {@value #MAX_BACKOFF_MILLIS} ms and
 * {@value #JITTER_FACTOR} jitter, so the documentation tells one story about backoff
 * instead of two. With five attempts the ceiling is never reached - the longest wait is
 * around 330 ms and the whole sequence stays below half a second, which is the point.
 * <p>
 * <b>What is not retried.</b> The classification is the one the outbox already uses
 * ({@link Camunda8Errors#repeatableJobCommandFailure}), so the adapter has one opinion
 * about what a repetition can change rather than two. A job which is gone and a request
 * the cluster rejects come back on the first attempt. And a shutdown ends the retry at
 * once: while the module is going down, the failure belongs to the shutdown and the job is
 * left to its lock instead of being failed - a retry loop must not hold the
 * drain, and it must not turn into a failure the shutdown would have avoided.
 * <p>
 * When the bound is reached the original failure is rethrown, so the behaviour after the
 * retries are used up is what it was before this class existed.
 * <p>
 * What may be repeated, what bounds the repetition, and why a shutdown ends it at once is decision
 * 9 in the repository's DECISIONS.md.
 */
@Slf4j
public final class Camunda8CommandRetry {

  private Camunda8CommandRetry() {
  }

  /**
   * How often a repeatable failure of an outcome command is tried at all, the first
   * attempt included.
   */
  public static final int MAX_ATTEMPTS = 5;

  /**
   * The first backoff in milliseconds - the client's own initial activation delay.
   */
  public static final long INITIAL_BACKOFF_MILLIS = 50;

  /**
   * What each backoff is multiplied by - the client's own factor.
   */
  public static final double BACKOFF_FACTOR = 1.6;

  /**
   * The longest backoff in milliseconds - the client's own ceiling. Deliberately never
   * reached within {@value #MAX_ATTEMPTS} attempts.
   */
  public static final long MAX_BACKOFF_MILLIS = 5_000;

  /**
   * How much a backoff is spread around its nominal value, so two handlers rejected in the
   * same moment do not come back in the same moment - the client's own jitter.
   */
  public static final double JITTER_FACTOR = 0.1;

  /**
   * Sends an outcome command, repeating it while the cluster's answer is worth repeating
   * and the job's lock still allows another attempt.
   *
   * @param adapterId The adapter instance, for the messages
   * @param command What is being sent, named the way the log should name it (e.g.
   *          <code>completion</code>)
   * @param jobKey The job the command belongs to
   * @param taskName The task definition respectively job type, as the application knows it
   * @param lockDeadline When the job's lock expires (epoch milliseconds, i.e.
   *          {@code ActivatedJob#getDeadline()})
   * @param shuttingDown Whether the workflow module is going down
   * @param send The command itself
   * @throws RuntimeException The original failure, once no further attempt is allowed
   */
  public static void send(
      final String adapterId,
      final String command,
      final long jobKey,
      final String taskName,
      final long lockDeadline,
      final BooleanSupplier shuttingDown,
      final Runnable send) {

    var attempt = 1;
    while (true) {
      try {
        send.run();
        if (attempt > 1) {
          log.info(
              "Camunda8[{}]: the {} of job {} ('{}') went through on attempt {}",
              adapterId,
              command,
              jobKey,
              taskName,
              attempt);
        }
        return;
      } catch (final RuntimeException e) {
        final var reason = whyToStop(e, attempt, lockDeadline, shuttingDown);
        if (reason != null) {
          if (reason.worthAWarning()) {
            log.warn(
                """
                    Camunda8[{}]: the {} of job {} ('{}') was rejected by the cluster and {}. The job is \
                    reported as failed although its work is done, which costs it one retry - a cluster \
                    answering like this under load needs more capacity or fewer workers pushing at it.""",
                adapterId,
                command,
                jobKey,
                taskName,
                reason.text(),
                e);
          }
          throw e;
        }
        log.debug(
            "Camunda8[{}]: the {} of job {} ('{}') was rejected on attempt {} - retrying in {} ms",
            adapterId,
            command,
            jobKey,
            taskName,
            attempt,
            backoffMillis(attempt),
            e);
        if (!sleep(nextBackoff(attempt))) {
          throw e;
        }
        ++attempt;
      }
    }

  }

  /**
   * Why no further attempt is made, or <code>null</code> while one is.
   *
   * @param text What the message says after "and"
   * @param worthAWarning Whether giving up is worth a WARN - a job which is gone and a
   *          command the cluster refuses are reported by the caller, not here
   */
  private record Stop(
                      String text,
                      boolean worthAWarning) {
  }

  private static Stop whyToStop(
      final RuntimeException failure,
      final int attempt,
      final long lockDeadline,
      final BooleanSupplier shuttingDown) {

    if (!Camunda8Errors.repeatableJobCommandFailure(failure)) {
      // the job is gone, or the cluster refuses the command itself: repeating it would
      // only produce the same answer, and both cases have a caller which knows what to
      // make of them
      return new Stop("repeating it cannot change the answer", false);
    }
    if (shuttingDown.getAsBoolean()) {
      // The adapter is going down, so this is the shutdown and not the cluster.
      // The caller leaves the job to its lock, which is better than any retry
      return new Stop("the workflow module is shutting down", false);
    }
    if (attempt >= MAX_ATTEMPTS) {
      return new Stop("all %d attempts were used up".formatted(MAX_ATTEMPTS), true);
    }
    final var backoff = backoffMillis(attempt);
    final var remainingLock = lockDeadline - System.currentTimeMillis();
    if (remainingLock <= backoff) {
      return new Stop(
          "its lock runs out in %d ms, which is less than the %d ms until the next attempt"
              .formatted(Math.max(0, remainingLock), backoff), true);
    }
    return null;

  }

  /**
   * The nominal backoff after the given attempt, without jitter - what the decision
   * whether the lock still allows another attempt is made on.
   *
   * @param attempt The attempt which just failed (one-based)
   * @return The backoff in milliseconds
   */
  static long backoffMillis(
      final int attempt) {

    final var nominal = INITIAL_BACKOFF_MILLIS * Math.pow(BACKOFF_FACTOR, attempt - 1d);
    return Math.min(MAX_BACKOFF_MILLIS, Math.round(nominal));

  }

  /**
   * The backoff actually waited: the nominal one spread by {@value #JITTER_FACTOR}, so
   * handlers rejected together do not come back together.
   *
   * @param attempt The attempt which just failed (one-based)
   * @return The backoff to wait
   */
  static Duration nextBackoff(
      final int attempt) {

    final var nominal = backoffMillis(attempt);
    final var spread = nominal * JITTER_FACTOR * ((java.util.concurrent.ThreadLocalRandom.current()
        .nextDouble() * 2) - 1);
    return Duration.ofMillis(Math.max(1, Math.round(nominal + spread)));

  }

  /**
   * @return Whether the wait completed - an interrupted handler stops retrying and lets
   *         its caller report the original failure
   */
  private static boolean sleep(
      final Duration backoff) {

    try {
      Thread.sleep(backoff.toMillis());
      return true;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }

  }

}
