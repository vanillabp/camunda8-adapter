package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.client.api.ProblemDetail;
import io.camunda.client.api.command.ClientException;
import io.camunda.client.api.command.ClientHttpException;
import io.camunda.client.api.command.ClientStatusException;
import io.camunda.client.api.command.ProblemException;
import io.grpc.Status;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Which phase-two failures of Camunda 8 are worth repeating. The classification
 * is a pure function of the failure, so the boundary cases belong here and not into a test
 * against a cluster.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8ErrorsTest {

  private static ProblemException problem(
      final int status) {

    return problem(status, "reason");
  }

  /**
   * A cluster's REST answer, as the client hands it on: the status twice (the HTTP code
   * and the problem detail's own field) and the rejection's name as the title.
   */
  private static ProblemException problem(
      final int status,
      final String title) {

    final var details = new ProblemDetail();
    details.setStatus(status);
    details.setTitle(title);
    return new ProblemException(status, "reason", details);
  }

  @Test
  @DisplayName("A request the cluster rejects is permanent")
  public void rejectedRequestsArePermanent() {

    assertTrue(Camunda8Errors.permanentFailure(problem(400)));
    assertTrue(Camunda8Errors.permanentFailure(problem(403)));
    assertTrue(Camunda8Errors.permanentFailure(problem(405)));
    assertTrue(Camunda8Errors.permanentFailure(problem(501)));
    // ProblemException needs no rule of its own - a plain HTTP failure counts as well
    assertTrue(Camunda8Errors.permanentFailure(new ClientHttpException(400, "Bad Request")));

  }

  @Test
  @DisplayName("The gRPC equivalents are permanent, too")
  public void grpcEquivalentsArePermanent() {

    assertTrue(
        Camunda8Errors.permanentFailure(
            new ClientStatusException(Status.INVALID_ARGUMENT, null)));
    assertTrue(
        Camunda8Errors.permanentFailure(
            new ClientStatusException(Status.PERMISSION_DENIED, null)));
    assertTrue(
        Camunda8Errors.permanentFailure(
            new ClientStatusException(Status.UNIMPLEMENTED, null)));

  }

  @Test
  @DisplayName("A task key which is not a number can never become one")
  public void malformedTaskKeyIsPermanent() {

    assertTrue(
        Camunda8Errors.permanentFailure(
            new IllegalStateException("dispatching", new NumberFormatException("not-a-key"))));

  }

  @Test
  @DisplayName("Eventual consistency, an expired token, conflicts and cluster trouble are repeated")
  public void everythingElseIsRepeatable() {

    // 404: the signature of eventual consistency (for job commands jobAlreadyGone
    // consumes it before the classification is asked at all)
    assertFalse(Camunda8Errors.permanentFailure(problem(404)));
    // 401: usually an expired token the client refreshes
    assertFalse(Camunda8Errors.permanentFailure(problem(401)));
    assertFalse(Camunda8Errors.permanentFailure(problem(409)));
    assertFalse(Camunda8Errors.permanentFailure(problem(429)));
    assertFalse(Camunda8Errors.permanentFailure(problem(500)));
    assertFalse(Camunda8Errors.permanentFailure(problem(503)));
    assertFalse(
        Camunda8Errors.permanentFailure(
            new ClientStatusException(Status.UNAVAILABLE, null)));
    assertFalse(Camunda8Errors.permanentFailure(new IOException("connection reset")));
    assertFalse(Camunda8Errors.permanentFailure(null));

  }

  @Test
  @DisplayName("A timeout is repeated, in every shape the client hands one over in")
  public void aTimeoutIsRepeatedInEveryShape() {

    // The transport decides how a request which ran out of time reaches the adapter, and
    // none of these shapes is a statement about what the cluster did - the answer may
    // have been given and only failed to arrive. So every one of them is repeated, and
    // none of them is mistaken for a job which is gone.
    for (final var timeout : timeoutsAsTheClientHandsThemOver()) {
      assertFalse(Camunda8Errors.permanentFailure(timeout), timeout.toString());
      assertFalse(Camunda8Errors.jobAlreadyGone(timeout), timeout.toString());
      assertTrue(Camunda8Errors.repeatableJobCommandFailure(timeout), timeout.toString());
      assertFalse(Camunda8Errors.messageAlreadyPublished(timeout), timeout.toString());
      assertFalse(Camunda8Errors.queryApiRefused(timeout), timeout.toString());
    }

  }

  /**
   * Every shape a request which ran out of time arrives in - read off the client: a REST
   * request times out in the socket below Apache HttpClient, a bounded wait on the future
   * ends in a {@link TimeoutException}, and gRPC answers a deadline of
   * its own with the status of that name. The client wraps whichever of them into a
   * {@link ClientException} respectively a
   * {@link CompletionException} on its way out.
   */
  private static List<Throwable> timeoutsAsTheClientHandsThemOver() {

    return List
        .of(
            new SocketTimeoutException("Read timed out"),
            new CompletionException(
                new ClientException(
                    "io error", new SocketTimeoutException("Read timed out"))),
            new CompletionException(new TimeoutException()),
            new ClientException(
                "timed out", new TimeoutException("waited 10s")),
            new ClientStatusException(Status.DEADLINE_EXCEEDED, null),
            new CompletionException(
                new ClientStatusException(Status.DEADLINE_EXCEEDED, null)));

  }

  @Test
  @DisplayName("The whole chain of causes is examined, and a self-referencing cause does not loop")
  public void theChainIsExaminedWithoutLooping() {

    assertTrue(
        Camunda8Errors.permanentFailure(
            new IllegalStateException("outer", new RuntimeException("inner", problem(400)))));

    final var selfReferencing = new RuntimeException("loops") {

      @Override
      public synchronized Throwable getCause() {
        return this;
      }

    };
    assertFalse(Camunda8Errors.permanentFailure(selfReferencing));

  }

  @Test
  @DisplayName("A job command has one more permanent case: the job itself is gone")
  public void aGoneJobIsPermanentForJobCommands() {

    // repeatable for an outbox entry (404 is the signature of eventual consistency) and
    // permanent for a command against THIS job - repeating it would turn the tolerated
    // at-least-once residual into a retry storm
    assertFalse(Camunda8Errors.permanentFailure(problem(404)));
    assertFalse(Camunda8Errors.repeatableJobCommandFailure(problem(404)));

  }

  @Test
  @DisplayName("Backpressure is repeatable on both transports")
  public void backpressureIsRepeatable() {

    // REST answers with 503 and the title the engine sends...
    assertTrue(Camunda8Errors.repeatableJobCommandFailure(problem(503)));
    // ...gRPC with the status of the same name, and neither of them is permanent
    assertTrue(
        Camunda8Errors.repeatableJobCommandFailure(
            new ClientStatusException(Status.RESOURCE_EXHAUSTED, null)));
    assertFalse(Camunda8Errors.repeatableJobCommandFailure(problem(400)));

  }

  @Test
  @DisplayName("A job which is gone is recognised on both transports, by their codes")
  public void aGoneJobIsRecognisedOnBothTransports() {

    // what the cluster answers a command addressing a job key it does not hold:
    // REST 404 with the title of the rejection, gRPC the status of the same name
    assertTrue(Camunda8Errors.jobAlreadyGone(problem(404, "NOT_FOUND")));
    assertTrue(
        Camunda8Errors.jobAlreadyGone(
            new ClientStatusException(Status.NOT_FOUND, null)));
    // the words around the code are the cluster's to reword, so they decide nothing
    assertFalse(Camunda8Errors.jobAlreadyGone(new IllegalStateException("no such job was NOT_FOUND")));
    assertFalse(Camunda8Errors.jobAlreadyGone(problem(409, "ALREADY_EXISTS")));

  }

  @Test
  @DisplayName("A message published twice is recognised on both transports, by their codes")
  public void aRepeatedPublicationIsRecognisedOnBothTransports() {

    // a publication carrying a message id the cluster still remembers is rejected as
    // ALREADY_EXISTS - which reaches the REST client as HTTP 409 and the gRPC client as
    // the status of that name. Both transports are in use: which one carries a command
    // is what 'prefer-rest-over-grpc' decides
    assertTrue(Camunda8Errors.messageAlreadyPublished(problem(409, "ALREADY_EXISTS")));
    assertTrue(
        Camunda8Errors.messageAlreadyPublished(
            new ClientStatusException(Status.ALREADY_EXISTS, null)));
    // and the words are not what says so: a reworded rejection keeps its code, while a
    // failure carrying the word and no code is not a rejection of the cluster at all
    assertFalse(
        Camunda8Errors.messageAlreadyPublished(
            new IllegalStateException("a message with that id has already been published")));
    assertFalse(Camunda8Errors.messageAlreadyPublished(problem(400, "INVALID_ARGUMENT")));
    assertFalse(Camunda8Errors.messageAlreadyPublished(null));

  }

  @Test
  @DisplayName("A refused query-API request is recognised by its status, not by its wording")
  public void aRefusedSearchIsRecognisedByItsStatus() {

    // every query endpoint of a cluster which cannot be searched answers 403, and the
    // problem detail says in prose only whether secondary storage is missing or the
    // credentials fall short - which is why the probe treats both the same
    assertTrue(Camunda8Errors.queryApiRefused(problem(403, "FORBIDDEN")));
    assertTrue(
        Camunda8Errors.queryApiRefused(
            new IllegalStateException("searching", problem(403, "FORBIDDEN"))));
    // a cluster which is merely unreachable says nothing about what it offers
    assertFalse(Camunda8Errors.queryApiRefused(new IOException("connection refused")));
    assertFalse(
        Camunda8Errors.queryApiRefused(
            new IllegalStateException("This endpoint requires a secondary storage, but none is set")));
    assertFalse(Camunda8Errors.queryApiRefused(problem(503)));

  }

  @Test
  @DisplayName("An incident names the exception's type where its message says nothing")
  public void anIncidentNamesTheType() {

    assertEquals(
        "java.lang.NullPointerException",
        Camunda8Errors.incidentMessage(new NullPointerException()));
    assertEquals(
        "java.lang.IllegalStateException: the connection pool is exhausted",
        Camunda8Errors.incidentMessage(new IllegalStateException("the connection pool is exhausted")));
    assertEquals("no exception given", Camunda8Errors.incidentMessage(null));

  }

}
