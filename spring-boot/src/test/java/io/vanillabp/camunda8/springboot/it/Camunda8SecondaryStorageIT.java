package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
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
 * What a cluster WITH secondary storage does to operations on running workflows
 * (story 52).
 * <p>
 * This is the cluster the other Camunda 8 tests deliberately do not run on: without
 * secondary storage the query API is unavailable, the adapter answers its awareness
 * probe optimistically, and the search path is never exercised. That is how a filter
 * which matched nothing survived - the probe answered "no BPMS knows this workflow"
 * for every workflow, and every operation electing its BPMS by probing failed on real
 * clusters while all tests were green.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
public class Camunda8SecondaryStorageIT {

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
  private SecondaryStorageDockerWorkflowService workflowService;

  @Autowired
  private SecondaryStorageDockerAggregateRepository repository;

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

    // generous: the cluster exports to Elasticsearch before the query API can answer,
    // and that pipeline is the slowest part of the test
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
   * Whether the query API knows the workflow of the aggregate - the very search the
   * adapter's awareness probe runs, with the value encoded the way the cluster stores
   * it (JSON, so a String carries its quotes).
   */
  private boolean queryApiKnows(
      final Long aggregateId) {

    return !client()
        .newProcessInstanceSearchRequest()
        .filter(filter -> filter.variables(java.util.Map.of("id", "\"%s\"".formatted(aggregateId))))
        .send()
        .join()
        .items()
        .isEmpty();

  }

  private Long startedWorkflow() throws Exception {

    final var aggregateId = transactionTemplate
        .execute(status -> workflowService.startWorkflow().getId());
    assertNotNull(aggregateId);
    awaitUntil(() -> queryApiKnows(aggregateId), "the query API to know the started workflow");
    return aggregateId;

  }

  @Test
  @DisplayName("a message reaches the workflow although the BPMS was found by probing")
  public void theProbeFindsTheWorkflow() throws Exception {

    final var aggregateId = startedWorkflow();

    // before the fix this threw WorkflowNotFoundException: the probe searched the
    // aggregate-ID variable without its JSON quotes, matched nothing, and reported
    // that no BPMS knows the workflow
    transactionTemplate.executeWithoutResult(status -> workflowService.correlate(aggregateId));

    awaitUntil(
        () -> "messageArrived".equals(
            repository
                .findById(aggregateId)
                .map(SecondaryStorageDockerAggregate::getProcessedBy)
                .orElse(null)),
        "the task behind the message catch event to run");

  }

  @Test
  @DisplayName("the viewer finds the workflow through the same search")
  public void theViewerFindsTheWorkflow() throws Exception {

    final var aggregateId = startedWorkflow();

    final var definitions = transactionTemplate
        .execute(status -> workflowService.definitionsOf(aggregateId));

    assertNotNull(definitions);
    assertFalse(
        definitions.isEmpty(),
        "the viewer locates the workflow by the same variable filter as the probe");

  }

}
