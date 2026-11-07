package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
 * A busy cluster refuses the change of a job timeout, and never a deployment, on the active
 * release line.
 * <p>
 * The broker's flow control keeps a list of commands it always takes, and the creation of a
 * deployment is on it on 8.8, 8.9 and 8.10. So the deployment at boot needs no retry for a busy
 * cluster, and the adapter has none. A line which takes the command off that list turns this
 * test red, and then the deployment needs the retry which the pre-commit check has.
 * <p>
 * The cluster runs with the smallest limit the broker allows, so a burst is refused for sure.
 * That half of the test proves the limit is on. Without it, a deployment which goes through
 * would prove nothing. The requests go out over plain HTTP, because the client would hide one
 * refusal per request behind its own repetition.
 * <p>
 * The class is skipped when Docker is unavailable.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
public class Camunda8ABusyClusterTakesEveryDeploymentIT {

  private static final int REQUESTS_OF_EACH_KIND = 50;

  private static final Duration EVERY_ANSWER_WITHIN = Duration.ofMinutes(2);

  private static final String A_MODEL = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="Definitions_1" \
      targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:process id="%s" isExecutable="true">
          <bpmn:startEvent id="TheStart" />
        </bpmn:process>
      </bpmn:definitions>
      """;

  @Container
  private static final GenericContainer<?> CLUSTER = ClusterUnderTest
      .cluster("busy")
      // overrides what ClusterUnderTest sets, so this cluster refuses as soon as two requests
      // are in flight
      .withEnv("ZEEBE_BROKER_FLOWCONTROL_REQUEST_ENABLED", "true")
      .withEnv("ZEEBE_BROKER_FLOWCONTROL_REQUEST_ALGORITHM", "fixed")
      .withEnv("ZEEBE_BROKER_FLOWCONTROL_REQUEST_FIXED_LIMIT", "1");

  private static HttpRequest aDeployment(
      final URI address,
      final int number) {

    final var boundary = "the-boundary-%d".formatted(number);
    final var body = """
        --%s\r
        Content-Disposition: form-data; name="resources"; filename="busy-%d.bpmn"\r
        Content-Type: application/octet-stream\r
        \r
        %s\r
        --%s--\r
        """.formatted(boundary, number, A_MODEL.formatted("Busy"
        + UUID.randomUUID().toString().replace("-", "")),
        boundary);
    return HttpRequest
        .newBuilder(address.resolve("/v2/deployments"))
        .timeout(EVERY_ANSWER_WITHIN)
        .header("Content-Type", "multipart/form-data; boundary="
            + boundary)
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build();

  }

  private static HttpRequest aTimeoutChange(
      final URI address,
      final int number) {

    // a job which does not exist. The limit is checked before the job is looked up, so a
    // refusal says the limit was hit, and a 404 says the command was taken
    return HttpRequest
        .newBuilder(address.resolve("/v2/jobs/%d".formatted(2251799813685249L + number)))
        .timeout(EVERY_ANSWER_WITHIN)
        .header("Content-Type", "application/json")
        .method("PATCH", HttpRequest.BodyPublishers.ofString("""
            {"changeset":{"timeout":60000}}"""))
        .build();

  }

  private static Map<Integer, Long> statusesOf(
      final List<CompletableFuture<HttpResponse<Void>>> answers) {

    return answers
        .stream()
        .map(CompletableFuture::join)
        .map(HttpResponse::statusCode)
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

  }

  @Test
  @DisplayName("In a burst which the cluster partly refuses as busy, every deployment is taken")
  public void everyDeploymentIsTaken() {

    final var address = URI.create(ClusterUnderTest.restAddress(CLUSTER));
    // HTTP/1.1, so that every request of the burst has a connection of its own
    try (final var http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
      final var timeoutChanges = IntStream
          .range(0, REQUESTS_OF_EACH_KIND)
          .mapToObj(number -> http.sendAsync(aTimeoutChange(address, number), HttpResponse.BodyHandlers.discarding()))
          .toList();
      final var deployments = IntStream
          .range(0, REQUESTS_OF_EACH_KIND)
          .mapToObj(number -> http.sendAsync(aDeployment(address, number), HttpResponse.BodyHandlers.discarding()))
          .toList();

      final var timeoutChangeStatuses = statusesOf(timeoutChanges);
      final var deploymentStatuses = statusesOf(deployments);

      assertTrue(
          timeoutChangeStatuses.getOrDefault(503, 0L) > 0,
          "the answers to the timeout changes by status. No 503 means the limit was not on, so the "
              + "deployments below prove nothing: "
              + timeoutChangeStatuses);
      assertEquals(
          Map.of(200, (long) REQUESTS_OF_EACH_KIND),
          deploymentStatuses,
          "the answers to the deployments by status. A 503 means the broker no longer takes every "
              + "deployment, so the deployment at boot needs a retry for a busy cluster");
    }

  }

}
