package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.camunda.client.CamundaClient;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * End-to-end test of pushing a changed aggregate (story 44) against a real Camunda 8.
 * <p>
 * Unlike the other Camunda 8 tests this cluster runs WITH secondary storage (an
 * Elasticsearch of its own): Camunda 8 has no business key and no command addressing
 * a workflow by one of its variables, so the query API is the only way from an
 * aggregate ID to the process-instance and element-instance keys
 * {@code SetVariables} needs. A cluster without it cannot serve this feature - the
 * adapter says so instead of pretending.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
public class Camunda8AggregateChangedIT {

  static final Network NETWORK = Network.newNetwork();

  @Container
  static final GenericContainer<?> ELASTICSEARCH = new GenericContainer<>(
      DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.17.0"))
      .withNetwork(NETWORK)
      .withNetworkAliases("elasticsearch")
      .withEnv("discovery.type", "single-node")
      .withEnv("xpack.security.enabled", "false")
      .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
      .withExposedPorts(9200)
      .waitingFor(Wait
          .forHttp("/_cluster/health")
          .forPort(9200)
          .forStatusCode(200)
          .withStartupTimeout(Duration.ofMinutes(3)));

  @Container
  static final GenericContainer<?> CAMUNDA = new GenericContainer<>(
      DockerImageName.parse("camunda/zeebe:8.8.31"))
      .withNetwork(NETWORK)
      .dependsOn(ELASTICSEARCH)
      .withExposedPorts(8080, 26500, 9600)
      .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "elasticsearch")
      .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_ELASTICSEARCH_URL", "http://elasticsearch:9200")
      .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
      .waitingFor(Wait
          .forHttp("/actuator/health/readiness")
          .forPort(9600)
          .forStatusCode(200)
          .withStartupTimeout(Duration.ofMinutes(3)));

  @DynamicPropertySource
  static void camunda8Properties(
      final DynamicPropertyRegistry registry) {

    registry
        .add(
            "vanillabp.adapters.c8.rest-address",
            () -> "http://"
                + CAMUNDA.getHost()
                + ":"
                + CAMUNDA.getMappedPort(8080));
    registry
        .add(
            "vanillabp.adapters.c8.grpc-address",
            () -> "http://"
                + CAMUNDA.getHost()
                + ":"
                + CAMUNDA.getMappedPort(26500));

  }

  @Autowired
  private PushDockerWorkflowService workflowService;

  @Autowired
  private PushDockerAggregateRepository repository;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry clientFactoryRegistry;

  private CamundaClient client() {

    return clientFactoryRegistry
        .getFactory("c8")
        .getClient();

  }

  private void awaitUntil(
      final Supplier<Boolean> condition,
      final String description) throws InterruptedException {

    // generous on purpose: this cluster exports to Elasticsearch before the query
    // API can answer, and that pipeline is the slowest part of the test
    final var deadline = System.currentTimeMillis() + 240_000;
    while (!Boolean.TRUE.equals(condition.get())) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("timed out waiting for: "
            + description);
      }
      Thread.sleep(500);
    }

  }

  /**
   * The variables named 'note' of the workflow of the given aggregate, as the query
   * API reports them (with the scope they belong to).
   */
  private List<io.camunda.client.api.search.response.Variable> notesOf(
      final Long processInstanceKey) {

    return client()
        .newVariableSearchRequest()
        .filter(filter -> filter
            .processInstanceKey(processInstanceKey)
            .name("note"))
        .send()
        .join()
        .items();

  }

  private Long processInstanceKeyOf(
      final Long aggregateId) {

    final var found = client()
        .newProcessInstanceSearchRequest()
        // variable values are stored as JSON: a String value is searched WITH its quotes
        .filter(filter -> filter.variables(java.util.Map.of("id", "\"%s\"".formatted(aggregateId))))
        .send()
        .join()
        .items();
    return found.isEmpty()
        ? null
        : found.getFirst().getProcessInstanceKey();

  }

  /**
   * The element instance of the task itself - the scope a push must NOT write into.
   */
  private Long elementInstanceKeyOfTask(
      final String taskId) {

    return client()
        .newJobSearchRequest()
        .filter(filter -> filter.jobKey(Long.parseLong(taskId)))
        .send()
        .join()
        .items()
        .getFirst()
        .getElementInstanceKey();

  }

  private String taskIdsOf(
      final Long aggregateId) {

    return transactionTemplate
        .execute(status -> repository.findById(aggregateId).map(PushDockerAggregate::getTaskIds).orElse(null));

  }

  @Test
  @DisplayName("a global push updates the workflow's own scope")
  public void aGlobalPushWritesTheWorkflowScope() throws Exception {

    final var aggregateId = transactionTemplate
        .execute(status -> workflowService.startWorkflow().getId());
    assertNotNull(aggregateId);

    awaitUntil(() -> taskIdsOf(aggregateId) != null, "the workflow to park at its asynchronous task");
    awaitUntil(() -> processInstanceKeyOf(aggregateId) != null, "the query API to know the instance");
    final var processInstanceKey = processInstanceKeyOf(aggregateId);

    transactionTemplate
        .executeWithoutResult(status -> workflowService.pushGlobally(aggregateId, "pushed-globally"));

    awaitUntil(
        () -> notesOf(processInstanceKey)
            .stream()
            .anyMatch(variable -> variable.getValue().contains("pushed-globally")),
        "the pushed value to arrive at the cluster");

    final var notes = notesOf(processInstanceKey);
    assertEquals(1, notes.size(), "a global push updates the existing variable instead of adding a scope");
    assertEquals(
        processInstanceKey,
        notes.getFirst().getScopeKey(),
        "the value has to live at the workflow's own scope");

  }

  @Test
  @DisplayName("a task-scoped push lands in the scope the task runs in, not at the workflow's")
  public void aTaskScopedPushReachesTheEnclosingScope() throws Exception {

    final var aggregateId = transactionTemplate
        .execute(status -> workflowService.saveAggregate().getId());
    assertNotNull(aggregateId);

    // the multi-instance process is started against the cluster: the injectable
    // process service starts the primary process only
    client()
        .newCreateInstanceCommand()
        .bpmnProcessId("test-app__AggregateChangedMultiInstanceProcess")
        .latestVersion()
        // one call: the client's variable() replaces what a previous call set
        .variables(java.util.Map.of("id", String.valueOf(aggregateId), "note", "before"))
        .send()
        .join();

    awaitUntil(
        () -> {
          final var taskIds = taskIdsOf(aggregateId);
          return (taskIds != null) && (taskIds.split(",").length == 2);
        },
        "both iterations of the multi-instance subprocess to park");

    final var taskIds = taskIdsOf(aggregateId).split(",");
    awaitUntil(() -> processInstanceKeyOf(aggregateId) != null, "the query API to know the instance");
    final var processInstanceKey = processInstanceKeyOf(aggregateId);

    transactionTemplate
        .executeWithoutResult(status -> workflowService.pushInto(aggregateId, "pushed-locally", taskIds[0]));

    awaitUntil(
        () -> notesOf(processInstanceKey)
            .stream()
            .anyMatch(variable -> variable.getValue().contains("pushed-locally")),
        "the pushed value to arrive at the cluster");

    final var notes = notesOf(processInstanceKey);
    final var local = notes
        .stream()
        .filter(variable -> variable.getValue().contains("pushed-locally"))
        .toList();
    assertEquals(1, local.size(), "exactly ONE scope may see the pushed value");
    assertNotEquals(
        processInstanceKey,
        local.getFirst().getScopeKey(),
        "a task-scoped push may not land at the workflow's scope");
    assertNotEquals(
        elementInstanceKeyOfTask(taskIds[0]),
        local.getFirst().getScopeKey(),
        "and not in the task's own element instance, which disappears with the task");
    assertTrue(
        notes
            .stream()
            .anyMatch(variable -> (variable.getScopeKey().equals(processInstanceKey)) && variable.getValue()
                .contains("before")),
        "the workflow's global value stays as it was - the honest consequence of scoping");

  }

}
