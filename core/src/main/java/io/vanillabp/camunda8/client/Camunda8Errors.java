package io.vanillabp.camunda8.client;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Predicate;

import io.camunda.client.api.command.ClientHttpException;
import io.camunda.client.api.command.ClientStatusException;
import io.camunda.client.api.command.ProblemException;
import io.grpc.Status;

/**
 * Shared classification of Camunda 8 client errors. Two kinds of caller ask.
 * <p>
 * A caller which SENT something asks whether another attempt can change the answer:
 * whether a job-based command failed because the job is GONE (already completed, canceled
 * by a boundary event, or the workflow moved on) - the at-least-once residual tolerated by
 * completions and mapped to UNKNOWN_TO_BPMS by awareness probes - and whether a phase-two
 * operation is refused the same way every time. Everything else is treated as an
 * infrastructure failure.
 * <p>
 * A caller which READ something asks what the cluster's answer means about the state it
 * asked for: {@link #notFound(Throwable)} is the reading side of the same code, where it
 * is usually the exporter which has not caught up rather than a failure. That side is what
 * this class is public for.
 * <p>
 * Every question is answered from the codes of the two transports, down the failure's
 * cause chain, and never from the words the cluster wraps around them. Why a job which is
 * gone is final even though the classification is otherwise generous is decision 9 in the
 * repository's DECISIONS.md.
 */
public final class Camunda8Errors {

  private Camunda8Errors() {
  }

  /**
   * The first failure in the chain of causes which answers the question, or
   * <code>null</code> where none does.
   * <p>
   * Every classification here reads a cause chain, and a cause chain is not guaranteed to
   * end. A client which wraps a failure it already wrapped hands over a ring, and the walk
   * would then run forever inside a job handler or an outbox dispatch, which is worse than
   * any answer it could have given. Remembering the failures already seen, by identity
   * rather than by <code>equals</code>, ends the walk on the second sight of one - a self
   * reference and a longer ring alike.
   */
  private static Throwable firstCauseAnswering(
      final Throwable throwable,
      final Predicate<Throwable> question) {

    final var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
    var current = throwable;
    while ((current != null) && seen.add(current)) {
      if (question.test(current)) {
        return current;
      }
      current = current.getCause();
    }
    return null;

  }

  /**
   * Whether any failure in the chain of causes answers the question, bounded as
   * {@link #firstCauseAnswering(Throwable, Predicate)} describes.
   */
  private static boolean anyCauseAnswers(
      final Throwable throwable,
      final Predicate<Throwable> question) {

    return firstCauseAnswering(throwable, question) != null;

  }

  /**
   * Whether the cluster answered "I do not hold that" - the one answer which means the
   * addressed key is not there, whatever was addressed by it.
   * <p>
   * Both transports say it with a code of their own: the REST gateway answers with HTTP
   * <code>404</code> (a {@link ProblemException} carries it as well, it extends
   * {@link ClientHttpException}), the gRPC gateway with the status
   * <code>NOT_FOUND</code>. Nothing here reads the message text - the words the cluster
   * wraps around that code are the cluster's to change.
   * <p>
   * Public because the answer means different things to different callers and every one
   * of them has to recognise it first: for a job command it is a job which is gone (see
   * {@link #jobAlreadyGone(Throwable)}), for a read of something just written it is the
   * exporter which has not caught up yet, and reading either from ONE of the two codes
   * turns the other transport's answer into a hard failure.
   *
   * @param throwable What the command or the request failed with
   * @return Whether the cluster does not (or does not yet) hold what was addressed
   */
  public static boolean notFound(
      final Throwable throwable) {

    return anyCauseAnswers(
        throwable,
        cause -> ((cause instanceof ClientHttpException http) && (http
            .code() == 404)) || ((cause instanceof ClientStatusException status) && (status
                .getStatusCode() == Status.Code.NOT_FOUND)));

  }

  /**
   * Whether the given failure means "this job does not exist (anymore)".
   * <p>
   * A job command addresses one key and nothing else, so the cluster not holding that key
   * is the whole answer: {@link #notFound(Throwable)} is what it reads, and what it adds
   * is the meaning, which is that the job was completed, canceled or otherwise moved on.
   *
   * @param throwable The failure of a job-based command
   * @return Whether the job is gone
   */
  public static boolean jobAlreadyGone(
      final Throwable throwable) {

    return notFound(throwable);

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

    return anyCauseAnswers(
        throwable,
        cause -> ((cause instanceof ClientHttpException http) && (http
            .code() == 409)) || ((cause instanceof ClientStatusException status) && (status
                .getStatusCode() == Status.Code.ALREADY_EXISTS)));

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

    return anyCauseAnswers(
        throwable,
        cause -> (cause instanceof ClientHttpException http) && (http.code() == 403));

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

    return anyCauseAnswers(
        throwable,
        // the task or instance key of the outbox entry is not a number, and it will not
        // become one
        cause -> (cause instanceof NumberFormatException) || ((cause instanceof ClientHttpException http) && PERMANENT_HTTP_STATUS
            .contains(http.code())) || ((cause instanceof ClientStatusException status) && PERMANENT_GRPC_CODES
                .contains(status.getStatusCode())));

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
   * How the cluster named a rejection, in the few words a log line can carry: the code of
   * the transport it arrived on and the sentence the cluster wrote around it.
   * <p>
   * The classifications above read a code and nothing else, because a decision may not rest
   * on words the cluster is free to reword (see decision 16 in the repository's
   * DECISIONS.md). This one decides nothing. It is read by a reader, who wants both halves
   * of what came back and wants them without turning on a stack trace first.
   *
   * @param throwable What the command failed with
   * @return One phrase naming the rejection, never <code>null</code>
   */
  public static String rejection(
      final Throwable throwable) {

    final var rejected = firstCauseAnswering(
        throwable,
        cause -> (cause instanceof ClientHttpException) || (cause instanceof ClientStatusException));
    if (rejected instanceof ClientHttpException http) {
      return "HTTP %d, %s".formatted(Integer.valueOf(http.code()), inOneLine(http.reason(), http));
    }
    if (rejected instanceof ClientStatusException status) {
      return "gRPC %s, %s"
          .formatted(status.getStatusCode(), inOneLine(status.getStatus().getDescription(), status));
    }
    return incidentMessage(throwable);

  }

  /**
   * What the cluster wrote around a code, as one line: the reason the transport carries, and
   * the exception's message where it carries none. A problem detail arrives with line breaks
   * in it, and a log line which brings its own is a log line nothing greps.
   */
  private static String inOneLine(
      final String reason,
      final Throwable throwable) {

    final var words = (reason == null) || reason.isBlank()
        ? throwable.getMessage()
        : reason;
    return (words == null) || words.isBlank()
        ? "no reason given"
        : words.replaceAll("\\s+", " ").trim();

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
