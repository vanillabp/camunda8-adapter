package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.camunda.client.CamundaClient;
import io.camunda.client.CredentialsProvider;
import io.camunda.client.api.command.ClientStatusException;
import io.camunda.client.api.command.FinalCommandStep;
import io.camunda.client.api.response.ActivatedJob;
import io.grpc.Status;
import io.vanillabp.camunda8.test.ClusterUnderTest;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Which commands a busy cluster refuses, of those a job handler and a user task send back.
 * <p>
 * The broker's flow control keeps a list of commands it always takes
 * ({@code WhiteListedCommands} of {@code zeebe-logstreams}). The completion and the failure of
 * a job are on it on 8.8, 8.9 and 8.10. The BPMN error and the change of a job timeout are on
 * it on no line. The completion of a user task is on it on 8.8 and 8.9 and no longer on 8.10.
 * This test sends each command in a burst against a cluster with the smallest request limit
 * and counts the answers, over REST and over gRPC. A line which changes the list turns it red,
 * and then the reasons in decision 9 and on the wiki page {@code Configuration} need another
 * look.
 * <p>
 * A burst of a command the list does not hold proves that the limit is on. Without that, a
 * burst which is taken in full would prove nothing. Every command meets a job or a user task
 * which exists, so an accepted command really did its work. REST requests go out over plain
 * HTTP, because the Camunda client repeats a 503 once and would hide a refusal. The gRPC
 * requests go out with the client, which repeats nothing on that transport. A user task has
 * no gRPC command, so its completion is only sent over REST.
 * <p>
 * The class is skipped when Docker is unavailable.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
public class Camunda8WhatABusyClusterRefusesIT {

  private static final int REQUESTS_PER_BURST = 300;

  private static final Duration EVERY_ANSWER_WITHIN = Duration.ofMinutes(2);

  /**
   * Long enough that no job of a burst comes back to an activation of a later burst.
   */
  private static final Duration LOCKED_FOR = Duration.ofMinutes(30);

  private static final String JOBS = "BusyJobs";

  private static final String USER_TASKS = "BusyUserTasks";

  private static final String MODEL = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
          id="Definitions_Busy" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:process id="%1$s" isExecutable="true">
          <bpmn:startEvent id="JobsStart" />
          <bpmn:sequenceFlow id="JobsFlow1" sourceRef="JobsStart" targetRef="Work" />
          <bpmn:serviceTask id="Work">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="=jobType" />
            </bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>
          </bpmn:serviceTask>
          <bpmn:sequenceFlow id="JobsFlow2" sourceRef="Work" targetRef="JobsEnd" />
          <bpmn:endEvent id="JobsEnd" />
        </bpmn:process>
        <bpmn:process id="%2$s" isExecutable="true">
          <bpmn:startEvent id="TasksStart" />
          <bpmn:sequenceFlow id="TasksFlow1" sourceRef="TasksStart" targetRef="Decide" />
          <bpmn:userTask id="Decide">
            <bpmn:extensionElements>
              <zeebe:userTask />
            </bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>
          </bpmn:userTask>
          <bpmn:sequenceFlow id="TasksFlow2" sourceRef="Decide" targetRef="TasksEnd" />
          <bpmn:endEvent id="TasksEnd" />
        </bpmn:process>
      </bpmn:definitions>
      """.formatted(JOBS, USER_TASKS);

  @Container
  private static final GenericContainer<?> CLUSTER = ClusterUnderTest
      .withAuthentication()
      // overrides what ClusterUnderTest sets, so this cluster refuses as soon as two requests
      // are in flight. The authenticated cluster, because gRPC needs an identity
      .withEnv("ZEEBE_BROKER_FLOWCONTROL_REQUEST_ENABLED", "true")
      .withEnv("ZEEBE_BROKER_FLOWCONTROL_REQUEST_ALGORITHM", "fixed")
      .withEnv("ZEEBE_BROKER_FLOWCONTROL_REQUEST_FIXED_LIMIT", "1");

  /** What came back from one request, the way the counts name it. */
  private static final String ACCEPTED = "accepted";

  /** @see #ACCEPTED */
  private static final String REFUSED_AS_BUSY = "refused as busy";

  /** What a burst of one command is expected to give. */
  private enum Verdict {
    /** Every request was accepted. */
    ALL_TAKEN,
    /** At least one request was refused as busy, and none failed for another reason. */
    PARTLY_REFUSED,
    /** At least one request failed for a reason other than a busy cluster. */
    BROKEN
  }

  @Test
  @DisplayName("A busy cluster takes every completion and failure of a job, and refuses the other commands")
  public void whatABusyClusterRefuses() throws Exception {

    final var line = ClusterUnderTest.image().getVersionPart();
    final var counts = new LinkedHashMap<String, Map<String, Long>>();
    final var verdicts = new LinkedHashMap<String, Verdict>();

    try (final var client = grpcClient(); final var http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
        .build()) {

      awaitTaken(() -> client
          .newDeployResourceCommand()
          .addResourceStringUtf8(MODEL, "busy.bpmn"));

      final var rest = URI.create(ClusterUnderTest.restAddress(CLUSTER));

      record Kind(String name, Function<Long, HttpRequest.Builder> overRest,
                  Function<Long, FinalCommandStep<?>> overGrpc) {
      }
      final List<Kind> kinds = List
          .of(
              new Kind(
                  "job completion", key -> post(rest, "/v2/jobs/%d/completion".formatted(key), "{}"), key -> client
                      .newCompleteCommand(key)),
              new Kind(
                  "job failure", key -> post(rest, "/v2/jobs/%d/failure".formatted(key),
                      """
                          {"retries":1,"errorMessage":"busy","retryBackOff":%d}"""
                          .formatted(LOCKED_FOR.toMillis())), key -> client
                              .newFailCommand(key)
                              .retries(1)
                              .retryBackoff(LOCKED_FOR)
                              .errorMessage("busy")),
              new Kind(
                  "BPMN error", key -> post(rest, "/v2/jobs/%d/error".formatted(key), """
                      {"errorCode":"busy"}"""), key -> client.newThrowErrorCommand(key).errorCode("busy")),
              new Kind(
                  "job timeout change", key -> HttpRequest
                      .newBuilder(rest.resolve("/v2/jobs/%d".formatted(key)))
                      .header("Content-Type", "application/json")
                      .method("PATCH", HttpRequest.BodyPublishers.ofString("""
                          {"changeset":{"timeout":%d}}""".formatted(LOCKED_FOR.toMillis()))), key -> client
                          .newUpdateTimeoutCommand(key).timeout(LOCKED_FOR)));

      for (final var kind : kinds) {
        for (final var transport : List.of("REST", "gRPC")) {
          final var jobs = jobsFor(client, kind.name()
              + " "
              + transport);
          final List<CompletableFuture<String>> answers = transport.equals("REST")
              ? jobs
                  .stream()
                  .map(key -> sendOverRest(http, kind.overRest().apply(key)))
                  .toList()
              : jobs
                  .stream()
                  .map(key -> sendOverGrpc(kind.overGrpc().apply(key)))
                  .toList();
          count(counts, verdicts, "%s over %s".formatted(kind.name(), transport), answers);
        }
      }

      final var userTasks = userTasks(client);
      count(
          counts,
          verdicts,
          "user task completion over REST",
          userTasks
              .stream()
              .map(key -> sendOverRest(http, post(rest, "/v2/user-tasks/%d/completion".formatted(key), "{}")))
              .toList());
    }

    // printed for the record of a measurement. The extension shows it when the test fails
    counts.forEach((
        burst,
        byAnswer) -> System.out.printf("BURST line=%s %s: %s%n", line, burst, byAnswer));

    final var userTaskCompletion = line.startsWith("8.8.") || line.startsWith("8.9.")
        ? Verdict.ALL_TAKEN
        : Verdict.PARTLY_REFUSED;
    final var expected = new LinkedHashMap<String, Verdict>();
    expected.put("job completion over REST", Verdict.ALL_TAKEN);
    expected.put("job completion over gRPC", Verdict.ALL_TAKEN);
    expected.put("job failure over REST", Verdict.ALL_TAKEN);
    expected.put("job failure over gRPC", Verdict.ALL_TAKEN);
    expected.put("BPMN error over REST", Verdict.PARTLY_REFUSED);
    expected.put("BPMN error over gRPC", Verdict.PARTLY_REFUSED);
    expected.put("job timeout change over REST", Verdict.PARTLY_REFUSED);
    expected.put("job timeout change over gRPC", Verdict.PARTLY_REFUSED);
    expected.put("user task completion over REST", userTaskCompletion);
    assertEquals(
        expected,
        verdicts,
        "what a cluster of line %s with a request limit of one answered, burst by burst: %s"
            .formatted(line, counts));

  }

  private static void count(
      final Map<String, Map<String, Long>> counts,
      final Map<String, Verdict> verdicts,
      final String burst,
      final List<CompletableFuture<String>> answers) {

    final Map<String, Long> byAnswer = answers
        .stream()
        .map(CompletableFuture::join)
        .collect(Collectors.groupingBy(Function.identity(), TreeMap::new, Collectors.counting()));
    counts.put(burst, byAnswer);
    verdicts
        .put(
            burst,
            byAnswer.keySet().stream().anyMatch(answer -> !answer.equals(ACCEPTED) && !answer.equals(REFUSED_AS_BUSY))
                ? Verdict.BROKEN
                : byAnswer.containsKey(REFUSED_AS_BUSY)
                    ? Verdict.PARTLY_REFUSED
                    : Verdict.ALL_TAKEN);

  }

  private static CamundaClient grpcClient() {

    return CamundaClient
        .newClientBuilder()
        .grpcAddress(URI.create(ClusterUnderTest.grpcAddress(CLUSTER)))
        .restAddress(URI.create(ClusterUnderTest.restAddress(CLUSTER)))
        .preferRestOverGrpc(false)
        .credentialsProvider(
            CredentialsProvider
                .newBasicAuthCredentialsProviderBuilder()
                .username(ClusterUnderTest.USERNAME)
                .password(ClusterUnderTest.PASSWORD)
                .build())
        .defaultRequestTimeout(EVERY_ANSWER_WITHIN)
        .build();

  }

  private static HttpRequest.Builder post(
      final URI rest,
      final String path,
      final String body) {

    return HttpRequest
        .newBuilder(rest.resolve(path))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body));

  }

  private static CompletableFuture<String> sendOverRest(
      final HttpClient http,
      final HttpRequest.Builder request) {

    final var credentials = Base64
        .getEncoder()
        .encodeToString("%s:%s"
            .formatted(ClusterUnderTest.USERNAME, ClusterUnderTest.PASSWORD)
            .getBytes(StandardCharsets.UTF_8));
    return http
        .sendAsync(
            request
                .timeout(EVERY_ANSWER_WITHIN)
                .header("Authorization", "Basic "
                    + credentials)
                .build(),
            HttpResponse.BodyHandlers.ofString())
        .handle((
            response,
            failure) -> {
          if (failure != null) {
            return failure.toString();
          }
          final var status = response.statusCode();
          if (status >= 200 && status < 300) {
            return ACCEPTED;
          }
          return status == 503 && response.body().contains("RESOURCE_EXHAUSTED")
              ? REFUSED_AS_BUSY
              : "HTTP %d".formatted(status);
        });

  }

  private static CompletableFuture<String> sendOverGrpc(
      final FinalCommandStep<?> command) {

    return command
        .send()
        .toCompletableFuture()
        .handle((
            response,
            failure) -> {
          if (failure == null) {
            return ACCEPTED;
          }
          return busy(failure)
              ? REFUSED_AS_BUSY
              : unwrapped(failure).getClass().getSimpleName()
                  + ": "
                  + unwrapped(failure).getMessage();
        });

  }

  private static boolean busy(
      final Throwable failure) {

    final var cause = unwrapped(failure);
    // join() wraps the answer of the gateway into a ClientStatusException. The future of send()
    // hands over the StatusRuntimeException of gRPC itself
    return cause instanceof ClientStatusException status
        ? status.getStatusCode() == Status.Code.RESOURCE_EXHAUSTED
        : Status.fromThrowable(cause).getCode() == Status.Code.RESOURCE_EXHAUSTED;

  }

  private static Throwable unwrapped(
      final Throwable failure) {

    return failure instanceof CompletionException && failure.getCause() != null
        ? failure.getCause()
        : failure;

  }

  /**
   * Sends a command of the setup until the cluster takes it. Setup commands are not on the
   * list, and the limit of one refuses them now and then even without a burst.
   */
  private static <T> T awaitTaken(
      final Supplier<FinalCommandStep<T>> command) throws InterruptedException {

    while (true) {
      try {
        return command.get().send().join();
      } catch (final RuntimeException e) {
        if (!busy(e)) {
          throw e;
        }
        Thread.sleep(50);
      }
    }

  }

  private static List<Integer> items() {

    return IntStream.range(0, REQUESTS_PER_BURST).boxed().toList();

  }

  /**
   * Starts a workflow with a job for each request of a burst and activates all of them.
   */
  private static List<Long> jobsFor(
      final CamundaClient client,
      final String burst) throws InterruptedException {

    final var jobType = "busy-"
        + burst.replaceAll("[^A-Za-z]", "-");
    awaitTaken(() -> client
        .newCreateInstanceCommand()
        .bpmnProcessId(JOBS)
        .latestVersion()
        .variables(Map.of("jobType", jobType, "items", items())));
    final var keys = new ArrayList<Long>();
    final var deadline = System.currentTimeMillis() + EVERY_ANSWER_WITHIN.toMillis();
    while (keys.size() < REQUESTS_PER_BURST && System.currentTimeMillis() < deadline) {
      final var missing = REQUESTS_PER_BURST - keys.size();
      awaitTaken(() -> client
          .newActivateJobsCommand()
          .jobType(jobType)
          .maxJobsToActivate(missing)
          .timeout(LOCKED_FOR)
          .requestTimeout(Duration.ofSeconds(5)))
          .getJobs()
          .stream()
          .map(ActivatedJob::getKey)
          .forEach(keys::add);
    }
    assertEquals(REQUESTS_PER_BURST, keys.size(), "the jobs activated for the burst '%s'".formatted(burst));
    return keys;

  }

  /**
   * Starts a workflow with a user task for each request of a burst and waits until the
   * search finds all of them.
   */
  private static List<Long> userTasks(
      final CamundaClient client) throws InterruptedException {

    final var instance = awaitTaken(() -> client
        .newCreateInstanceCommand()
        .bpmnProcessId(USER_TASKS)
        .latestVersion()
        .variables(Map.of("items", items())))
        .getProcessInstanceKey();
    final var deadline = System.currentTimeMillis() + EVERY_ANSWER_WITHIN.toMillis();
    List<Long> keys = List.of();
    while (keys.size() < REQUESTS_PER_BURST && System.currentTimeMillis() < deadline) {
      Thread.sleep(500);
      keys = client
          .newUserTaskSearchRequest()
          .filter(filter -> filter.processInstanceKey(instance))
          .page(page -> page.limit(REQUESTS_PER_BURST))
          .send()
          .join()
          .items()
          .stream()
          .map(task -> task.getUserTaskKey())
          .toList();
    }
    assertEquals(REQUESTS_PER_BURST, keys.size(), "the user tasks found for the burst");
    return keys;

  }

}
