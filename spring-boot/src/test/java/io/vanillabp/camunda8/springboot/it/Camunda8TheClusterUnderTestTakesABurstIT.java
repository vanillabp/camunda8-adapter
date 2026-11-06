package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vanillabp.camunda8.test.ClusterUnderTest;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The cluster under test takes every request of a burst, on the active release line.
 * <p>
 * An application opens all its workers in the same moment. With the limit a broker puts on the
 * requests it works on at the same time, a part of them is answered with HTTP 503. So is a command
 * a test sends in that moment, and that test then fails for a reason it has nothing to do with.
 * {@link ClusterUnderTest} switches the limit off with a key of the broker configuration. A
 * cluster ignores a key it does not know, without a word. A key which stops working on a new line
 * would only show as a test which fails now and then, somewhere in the module. This test says it
 * directly.
 * <p>
 * It keeps a cluster of its own. The limit grows with every request which goes well, so a cluster
 * which has worked for half an hour could take a burst of this size even with the limit on.
 * <p>
 * The class is skipped when Docker is unavailable. See decision 96 in the repository's
 * DECISIONS.md.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
public class Camunda8TheClusterUnderTestTakesABurstIT {

  /**
   * Four times the limit a fresh cluster starts with. It is far above what a limit can reach
   * during the moments this test needs.
   */
  private static final int REQUESTS_IN_THE_BURST = 400;

  private static final Duration EVERY_ANSWER_WITHIN = Duration.ofMinutes(2);

  @Container
  private static final GenericContainer<?> CLUSTER = ClusterUnderTest.cluster("burst");

  @Test
  @DisplayName("A burst of job activations far above the broker's limit is answered without a refusal")
  public void aBurstIsNotRefused() {

    final var activation = URI.create(ClusterUnderTest.restAddress(CLUSTER)
        + "/v2/jobs/activation");
    // HTTP/1.1, so that every request of the burst has a connection of its own, as a worker has
    try (final var http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
      final List<CompletableFuture<HttpResponse<Void>>> burst = IntStream
          .range(0, REQUESTS_IN_THE_BURST)
          .mapToObj(number -> HttpRequest
              .newBuilder(activation)
              .timeout(EVERY_ANSWER_WITHIN)
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString("""
                  {"type":"nobody-serves-%d","worker":"the-burst","timeout":1000,\
                  "maxJobsToActivate":1,"requestTimeout":500}""".formatted(number)))
              .build())
          .map(request -> http.sendAsync(request, HttpResponse.BodyHandlers.discarding()))
          .toList();

      final Map<Integer, Long> answers = burst
          .stream()
          .map(CompletableFuture::join)
          .map(HttpResponse::statusCode)
          .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

      assertEquals(
          Map.of(200, (long) REQUESTS_IN_THE_BURST),
          answers,
          "the answers by status. A 503 means the limit is on, so the key in ClusterUnderTest no longer works on this line");
    }

  }

}
