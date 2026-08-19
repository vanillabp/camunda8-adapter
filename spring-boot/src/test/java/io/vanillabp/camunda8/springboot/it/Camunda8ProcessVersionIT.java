package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import java.util.function.Supplier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
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
 * What <code>&#64;WorkflowTask(version = ...)</code> means on Camunda 8 (story 48), with
 * TWO deployed versions of one process: the workflow started while only version 1 exists
 * is served by the method specifying that version, and a workflow started after a second,
 * TAGGED version was deployed is served by the method naming that tag.
 * <p>
 * The cluster brings secondary storage, because a job carries the version NUMBER and
 * never the version tag: which version carries which tag is a query-API question. The
 * numeric half of the feature needs none of that and works on any cluster.
 * <p>
 * The second version is deployed while the application runs, the way another node of a
 * rolling deployment deploys it - so this also exercises the on-demand lookup of a version
 * this application never deployed itself. Its identifiers carry the workflow module's
 * prefix by hand: the test deploys the model directly through the client, bypassing
 * VanillaBP's pipeline (the adapter runs with name-clash-avoidance 'use-prefix' here).
 * <p>
 * An own in-memory database keeps this test apart from the other Camunda 8 ITs: Spring
 * caches test contexts, so several of them live in parallel - and an outbox they SHARE
 * would let a foreign context start this test's workflow on its own cluster.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = {
        "spring.config.name=camunda8-it", "spring.datasource.url=jdbc:h2:mem:c8-versions-it;DB_CLOSE_DELAY=-1"
    })
// closed when the class is done: every IT here has a context of its own (its own
// container), Spring would keep them all until the JVM exits, and a context outliving
// its cluster keeps its job workers polling an address nobody answers - which is what
// made the later classes of this module run into their timeouts
@DirtiesContext
public class Camunda8ProcessVersionIT {

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
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.withSecondaryStorage(NETWORK, ELASTICSEARCH);

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
  private VersionedDockerWorkflowService workflowService;

  @Autowired
  private VersionedDockerAggregateRepository repository;

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

  private void awaitServedBy(
      final Long aggregateId,
      final String expected,
      final String description) throws InterruptedException {

    awaitUntil(
        () -> expected.equals(
            repository
                .findById(aggregateId)
                .map(VersionedDockerAggregate::getServedBy)
                .orElse(null)),
        description);

  }

  @Test
  @DisplayName("the version of the deployed process definition decides which method serves the task")
  public void theVersionDecidesWhichMethodRuns() throws Exception {

    // the application deployed version 1 while booting - a version made of numbers is
    // compared to what the job carries, so nothing is asked of the cluster
    final var firstId = transactionTemplate.execute(status -> workflowService.startWorkflow().getId());
    assertNotNull(firstId);
    awaitServedBy(firstId, "firstVersion", "the method serving version 1 to run");

    client()
        .newDeployResourceCommand()
        .addResourceFromClasspath("versioned/versioned-process-v2.bpmn")
        .send()
        .join();

    // the query API is fed by an exporter, so the tagged version arrives there a moment
    // later - and a job of a version no method serves would burn its retries
    awaitUntil(
        () -> client()
            .newProcessDefinitionSearchRequest()
            .filter(filter -> filter.processDefinitionId("test-app__VersionedProcess"))
            .send()
            .join()
            .items()
            .stream()
            .anyMatch(definition -> "release-2".equals(definition.getVersionTag())),
        "the query API to know the tagged version");

    final var secondId = transactionTemplate.execute(status -> workflowService.startWorkflow().getId());
    assertNotNull(secondId);
    awaitServedBy(
        secondId,
        "taggedVersion",
        "the method naming the version tag of version 2 to run - the version this "
            + "application never deployed itself");

  }

}
