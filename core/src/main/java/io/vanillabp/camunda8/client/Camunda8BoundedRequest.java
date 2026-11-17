package io.vanillabp.camunda8.client;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.camunda.client.api.command.FinalCommandStep;

/**
 * A request which gives up after a limit of its own. The client's timeout ends the request.
 * The limit on the future ends the waiting for it, so a request which the transport never
 * finishes cannot hold up the caller either.
 */
final class Camunda8BoundedRequest {

  private Camunda8BoundedRequest() {
  }

  /**
   * Sends the request and waits at most <code>limit</code> for the answer.
   *
   * @param request The request, ready to be sent
   * @param limit How long the request and the waiting for it may take
   * @return The answer of the cluster
   * @throws io.camunda.client.api.command.ClientException If the cluster did not answer in
   *           time, or answered with a failure
   */
  static <T> T answerWithin(
      final FinalCommandStep<T> request,
      final Duration limit) {

    return request
        .requestTimeout(limit)
        .send()
        .join(limit.toMillis(), TimeUnit.MILLISECONDS);

  }

}
