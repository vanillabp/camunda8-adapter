package io.vanillabp.camunda8.client;

import java.util.Set;

import io.camunda.client.api.command.ClientHttpException;
import io.camunda.client.api.command.ClientStatusException;
import io.camunda.client.api.command.ProblemException;

/**
 * Shared classification of Camunda 8 client errors: whether a job-based command
 * failed because the job is GONE (already completed, canceled by a boundary event,
 * or the workflow moved on) - the at-least-once residual tolerated by completions
 * and mapped to UNKNOWN_TO_BPMS by awareness probes. Everything else is treated as
 * an infrastructure failure.
 */
public final class Camunda8Errors {

  private Camunda8Errors() {
  }

  /**
   * Whether the given failure means "this job does not exist (anymore)".
   *
   * @param throwable The failure of a job-based command
   * @return Whether the job is gone
   */
  public static boolean jobAlreadyGone(
      final Throwable throwable) {

    var current = throwable;
    while (current != null) {
      if (current instanceof ProblemException problem && (problem.details() != null) && (problem.details()
          .getStatus() == 404)) {
        return true;
      }
      final var message = current.getMessage();
      if ((message != null) && (message.contains("NOT_FOUND") || message.contains("was not found"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;

  }

  /**
   * HTTP statuses of the REST transport a repetition cannot change (story 73):
   * <ul>
   * <li><code>400</code> - the cluster rejected the request itself,</li>
   * <li><code>403</code> - the credentials or the tenant are wrong, not late,</li>
   * <li><code>405</code> and <code>501</code> - this cluster version has no such
   * endpoint.</li>
   * </ul>
   * Deliberately NOT in the list: <code>404</code> is the signature of eventual
   * consistency (and for job commands it never gets here, see
   * {@link #jobAlreadyGone(Throwable)}), <code>401</code> is usually an expired token
   * the client refreshes, and <code>409</code>, <code>429</code> and every
   * <code>5xx</code> are exactly what the outbox repeats for.
   */
  private static final Set<Integer> PERMANENT_HTTP_STATUS = Set.of(400, 403, 405, 501);

  /**
   * The gRPC equivalents of {@link #PERMANENT_HTTP_STATUS}, for the commands still
   * travelling that transport. <code>NOT_FOUND</code>, <code>UNAUTHENTICATED</code>,
   * <code>ABORTED</code>, <code>RESOURCE_EXHAUSTED</code> and
   * <code>UNAVAILABLE</code> stay repeatable for the reasons given there.
   */
  private static final Set<io.grpc.Status.Code> PERMANENT_GRPC_CODES = Set
      .of(
          io.grpc.Status.Code.INVALID_ARGUMENT,
          io.grpc.Status.Code.PERMISSION_DENIED,
          io.grpc.Status.Code.UNIMPLEMENTED);

  /**
   * Whether repeating a phase-two operation which failed like this cannot help
   * (story 73), so the outbox entry is blocked at once instead of being retried until
   * its attempts are used up.
   * <p>
   * The list is short on purpose: repeating is the safe answer and stays the default
   * for everything not named here. {@link ProblemException} needs no rule of its own,
   * it extends {@link ClientHttpException}.
   *
   * @param throwable What the phase-two command threw
   * @return Whether the cluster will answer the same way on every attempt
   */
  public static boolean permanentFailure(
      final Throwable throwable) {

    var current = throwable;
    while (current != null) {
      // the task or instance key of the outbox entry is not a number, and it will not
      // become one
      if (current instanceof NumberFormatException) {
        return true;
      }
      if ((current instanceof ClientHttpException http) && PERMANENT_HTTP_STATUS.contains(http.code())) {
        return true;
      }
      if ((current instanceof ClientStatusException status) && PERMANENT_GRPC_CODES
          .contains(status.getStatusCode())) {
        return true;
      }
      current = current.getCause() == current
          ? null
          : current.getCause();
    }
    return false;

  }

  /**
   * Whether repeating a command a JOB HANDLER sends back to the cluster - a completion, a
   * BPMN error, a failure, a lock renewal - can change its answer (story 91). It is
   * {@link #permanentFailure} plus the one case which is permanent for a job command and
   * not for an outbox entry: a job which is gone stays gone, and repeating a command
   * against it would turn the benign at-least-once residual into a retry storm.
   * <p>
   * There is deliberately no separate opinion about what backpressure looks like. The
   * cluster answers it with <code>RESOURCE_EXHAUSTED</code> on gRPC and HTTP 503 on REST,
   * neither of which is permanent, so the classification the outbox already uses covers it
   * - and one classification cannot drift apart from itself.
   *
   * @param throwable What the command threw
   * @return Whether another attempt is worth making
   */
  public static boolean repeatableJobCommandFailure(
      final Throwable throwable) {

    return !jobAlreadyGone(throwable) && !permanentFailure(throwable);

  }

  /**
   * What a failed job reports as its error message - the text an operator reads in the
   * incident, so it carries the exception's TYPE next to its message. Camunda's own advice
   * is that this message is what a human sees, and the plain message alone says
   * <code>null</code> for every failure which carries none, a
   * {@link NullPointerException} above all.
   *
   * @param throwable What the handler threw
   * @return The incident text, never <code>null</code>
   */
  public static String incidentMessage(
      final Throwable throwable) {

    if (throwable == null) {
      return "no exception given";
    }
    final var message = throwable.getMessage();
    return (message == null) || message.isBlank()
        ? throwable.getClass().getName()
        : "%s: %s".formatted(throwable.getClass().getName(), message);

  }

}
