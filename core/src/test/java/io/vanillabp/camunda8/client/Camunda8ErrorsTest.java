package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.client.api.ProblemDetail;
import io.camunda.client.api.command.ClientHttpException;
import io.camunda.client.api.command.ClientStatusException;
import io.camunda.client.api.command.ProblemException;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Story 73: which phase-two failures of Camunda 8 are worth repeating. The classification
 * is a pure function of the failure, so the boundary cases belong here and not into a test
 * against a cluster.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8ErrorsTest {

  private static ProblemException problem(
      final int status) {

    final var details = new ProblemDetail();
    details.setStatus(status);
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
            new ClientStatusException(io.grpc.Status.INVALID_ARGUMENT, null)));
    assertTrue(
        Camunda8Errors.permanentFailure(
            new ClientStatusException(io.grpc.Status.PERMISSION_DENIED, null)));
    assertTrue(
        Camunda8Errors.permanentFailure(
            new ClientStatusException(io.grpc.Status.UNIMPLEMENTED, null)));

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
            new ClientStatusException(io.grpc.Status.UNAVAILABLE, null)));
    assertFalse(Camunda8Errors.permanentFailure(new java.io.IOException("connection reset")));
    assertFalse(Camunda8Errors.permanentFailure(null));

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

}
