package io.vanillabp.camunda8.client;

import java.util.Set;

import io.camunda.client.api.command.ClientHttpException;
import io.camunda.client.api.command.ClientStatusException;
import io.camunda.client.api.command.ProblemException;
import io.grpc.Status;

/**
 * Shared classification of Camunda 8 client errors: whether a job-based command
 * failed because the job is GONE (already completed, canceled by a boundary event,
 * or the workflow moved on) - the at-least-once residual tolerated by completions
 * and mapped to UNKNOWN_TO_BPMS by awareness probes. Everything else is treated as
 * an infrastructure failure.
 * <p>
 * Why a job which is gone is final even though the classification is otherwise generous is decision
 * 9 in the repository's DECISIONS.md.
 */
public final class Camunda8Errors {

  private Camunda8Errors() {
  }

  /**
   * Whether the given failure means "this job does not exist (anymore)".
   * <p>
   * Both transports say it with a code of their own: the REST gateway answers a job
   * command addressing a key it cannot find with HTTP <code>404</code>, the gRPC gateway
   * with the status <code>NOT_FOUND</code>. Nothing here reads the message text - the
   * words the cluster wraps around that code are the cluster's to change.
   *
   * @param throwable The failure of a job-based command
   * @return Whether the job is gone
   */
  public static boolean jobAlreadyGone(
      final Throwable throwable) {

    var current = throwable;
    while (current != null) {
      if ((current instanceof ClientHttpException http) && (http.code() == 404)) {
        return true;
      }
      if ((current instanceof ClientStatusException status) && (status
          .getStatusCode() == Status.Code.NOT_FOUND)) {
        return true;
      }
      current = current.getCause() == current
          ? null
          : current.getCause();
    }
    return false;

  }

  /**
   * Whether the cluster refused a publication because a message of the same id was
   * published before and still lives - the answer which makes an outbox entry done
   * rather than repeated, because a repetition would be refused again.
   * <p>
   * Both transports name the rejection with a code, and this adapter uses both: a
   * publication travels REST or gRPC depending on
   * <code>vanillabp.adapters.&lt;id&gt;.prefer-rest-over-grpc</code>. On gRPC the
   * rejection arrives as the status <code>ALREADY_EXISTS</code>, on REST as HTTP
   * <code>409</code> (whose problem detail carries the same word as its title). No other
   * conflict reaches a publication, so the code alone settles it, and the sentence the
   * cluster writes around it stays the cluster's to reword.
   *
   * @param throwable What the publish command threw
   * @return Whether the message was published before
   */
  public static boolean messageAlreadyPublished(
      final Throwable throwable) {

    var current = throwable;
    while (current != null) {
      if ((current instanceof ClientHttpException http) && (http.code() == 409)) {
        return true;
      }
      if ((current instanceof ClientStatusException status) && (status
          .getStatusCode() == Status.Code.ALREADY_EXISTS)) {
        return true;
      }
      current = current.getCause() == current
          ? null
          : current.getCause();
    }
    return false;

  }

  /**
   * Whether the cluster REFUSED a query-API request, which is what a cluster does that
   * cannot be searched at all.
   * <p>
   * The searches of this adapter travel REST only - the client offers no gRPC equivalent
   * for them - and the cluster refuses them with HTTP <code>403</code>. That code does
   * not say WHY, see {@link Camunda8QueryApi}, which is why only the probe asks this
   * question and everything else reads the remembered answer.
   *
   * @param throwable What a query-API request failed with
   * @return Whether the cluster refused to answer it
   */
  public static boolean queryApiRefused(
      final Throwable throwable) {

    var current = throwable;
    while (current != null) {
      if ((current instanceof ClientHttpException http) && (http.code() == 403)) {
        return true;
      }
      current = current.getCause() == current
          ? null
          : current.getCause();
    }
    return false;

  }

  /**
   * HTTP statuses of the REST transport a repetition cannot change:
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
  private static final Set<Status.Code> PERMANENT_GRPC_CODES = Set
      .of(
          Status.Code.INVALID_ARGUMENT,
          Status.Code.PERMISSION_DENIED,
          Status.Code.UNIMPLEMENTED);

  /**
   * Whether repeating a phase-two operation which failed like this cannot help,
   * so the outbox entry is blocked at once instead of being retried until
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
   * BPMN error, a failure, a lock renewal - can change its answer. It is
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
