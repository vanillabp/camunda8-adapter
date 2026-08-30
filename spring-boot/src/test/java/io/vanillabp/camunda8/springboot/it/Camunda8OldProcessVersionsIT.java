package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The old-versions startup check against a REAL cluster: the application deploys version
 * 1 of a process and boots again with a model which dropped one of its tasks. Reading
 * the model of the older version and counting the workflows running on it are the two
 * things only Camunda 8 can answer here, and both need the query API - which is why
 * this cluster runs WITH secondary storage.
 * <p>
 * Every case is a full boot, because the question is what a START reports, and the
 * findings are read from the captured output: Spring Boot resets the logging context
 * while it starts, which takes a log appender attached beforehand with it.
 * <p>
 * The last case is the one an operator ends up in: it deletes the older version and
 * boots again. Camunda 8 keeps a deleted definition and marks it <code>DELETED</code>,
 * so without a state in the search the check would go on reporting it forever.
 */
@ExtendWith(SuppressOutputExtension.class)
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Camunda8OldProcessVersionsIT {

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

  @Test
  @Order(1)
  @DisplayName("Version 1 is deployed and a workflow is started on it")
  public void deployVersionOneAndStartAWorkflow() throws Exception {

    final var application = boot("v1");
    try {
      final var workflowService = application.getBean(OldProcessVersionsDockerWorkflowService.class);
      final var repository = application.getBean(OldProcessVersionsDockerAggregateRepository.class);
      final var aggregate = application
          .getBean(org.springframework.transaction.support.TransactionTemplate.class)
          .execute(status -> workflowService.startWorkflow());

      // the workflow of version 1 walks through both tasks - what matters for the
      // next boot is that the cluster holds the version and the exporter saw it
      // generous on purpose: in a full build this class needs some 80 seconds of cluster
      // time, and a deadline close to that fails on a loaded machine while nothing is wrong
      final var deadline = System.currentTimeMillis() + 180_000;
      while (repository.findById(aggregate.getId()).orElseThrow().getServedBy() == null) {
        if (System.currentTimeMillis() > deadline) {
          throw new AssertionError("the workflow of version 1 did not run");
        }
        Thread.sleep(200);
      }
    } finally {
      application.close();
    }

  }

  @Test
  @Order(2)
  @DisplayName("The task version 1 still has is read from the cluster and reported")
  public void theDroppedTaskOfVersionOneIsReported(
      final CapturedOutput output) {

    final var before = output.getAll().length();
    boot("v2").close();
    final var reported = output.getAll().substring(before);

    // version 1 is served by the method kept for it, so nothing is demanded ...
    assertTrue(
        !reported.contains("definition(s) 'droppedInVersionTwo'"),
        "the task served by the version-1 method is not reported");
    // ... which is only provable if the model of version 1 was read at all: without
    // the query API the adapter says so instead
    assertTrue(
        !reported.contains("cannot read the models"),
        "the cluster's query API answered, so the models were read");

  }

  @Test
  @Order(3)
  @DisplayName("Fading version 1 out makes the method kept for it dead, and says why")
  public void fadingVersionOneOutReportsTheMethodKeptForIt(
      final CapturedOutput output) throws Exception {

    // the reason is only written if the check LEARNS from the cluster that version 1 is
    // still there, and that answer comes out of the secondary storage. The exporter is
    // behind the deployment by an unknown amount, and a check running before it caught up
    // knows the version it just deployed and nothing else - which is why this waits
    awaitTheQueryApiKnowingBothVersions();

    final var before = output.getAll().length();
    boot("v2", "--vanillabp.workflow-modules.test-app.adapters.c8.outfaded-versions=<2").close();
    final var reported = output.getAll().substring(before);

    assertTrue(reported.contains("droppedInVersionTwo"), "the method serving the faded-out version is named");
    assertTrue(reported.contains("the method never runs"), "and what that means is said");
    assertTrue(reported.contains("faded out by"), "and why");

  }

  @Test
  @Order(4)
  @DisplayName("A version an operator deleted is not one the check works on any more")
  public void aDeletedVersionIsNoVersionTheClusterHolds(
      final CapturedOutput output) throws Exception {

    // the remedy every report above asks for, applied: the version nobody wants to hear
    // about is deleted. Camunda 8 does not remove it, it marks it DELETED and keeps
    // answering searches with it, so a check asking without naming a state would find
    // everything it found before and report it again
    deleteVersionOneFromTheCluster();

    final var before = output.getAll().length();
    boot("v2").close();
    final var reported = output.getAll().substring(before);

    // this is the boot of the second case, which reported nothing while version 1 was
    // there: what the check works on is now version 2 alone, and the method kept for
    // version 1 has become the dead one
    assertTrue(
        reported.contains("(held: 2)"),
        "the deleted version is gone from the versions the check asks the cluster for");
    assertTrue(
        reported.contains("droppedInVersionTwo"),
        "and the method serving it is named as the method which never runs");

  }

  /**
   * A client of the test's own: the application booted per case is gone between them, and
   * this question is asked while none is running.
   */
  private static io.camunda.client.CamundaClient testClient() {

    return io.camunda.client.CamundaClient
        .newClientBuilder()
        .preferRestOverGrpc(true)
        .restAddress(java.net.URI.create("http://%s:%d".formatted(CAMUNDA.getHost(), CAMUNDA.getMappedPort(8080))))
        .grpcAddress(java.net.URI.create("http://%s:%d".formatted(CAMUNDA.getHost(), CAMUNDA.getMappedPort(26500))))
        .build();

  }

  private static void awaitTheQueryApiKnowingBothVersions() throws Exception {

    try (var client = testClient()) {
      // generous for the same reason the other cases are: the export pipeline is the
      // slowest part of this class, and a loaded machine makes it slower still
      final var deadline = System.currentTimeMillis() + 240_000;
      while (versionsKnownToTheCluster(client) < 2) {
        if (System.currentTimeMillis() > deadline) {
          throw new AssertionError(
              "the query API did not learn both versions of 'OldProcessVersionsProcess'");
        }
        Thread.sleep(500);
      }
    }

  }

  /**
   * The versions of the test's process the cluster answers with - the process id carries
   * the workflow module as a prefix (name-clash avoidance), so the search matches on the
   * end of it.
   */
  private static long versionsKnownToTheCluster(
      final io.camunda.client.CamundaClient client) {

    return definitionsOfTheTestsProcess(client).count();

  }

  /**
   * Deletes the resource version 1 was deployed with and waits until the cluster stops
   * answering searches for ACTIVE definitions with it - a deletion reaches the query API
   * the same way a deployment does, so it is behind by an unknown amount as well.
   */
  private static void deleteVersionOneFromTheCluster() throws Exception {

    try (var client = testClient()) {
      final var versionOne = definitionsOfTheTestsProcess(client)
          .filter(definition -> definition.getVersion() == 1)
          .findFirst()
          .orElseThrow(() -> new AssertionError("version 1 is not known to the cluster"));
      client
          .newDeleteResourceCommand(versionOne.getProcessDefinitionKey())
          .send()
          .join();

      final var deadline = System.currentTimeMillis() + 240_000;
      while (activeDefinitionsOfTheTestsProcess(client) > 1) {
        if (System.currentTimeMillis() > deadline) {
          throw new AssertionError("the query API kept answering with the deleted version 1");
        }
        Thread.sleep(500);
      }
    }

  }

  private static java.util.stream.Stream<io.camunda.client.api.search.response.ProcessDefinition> definitionsOfTheTestsProcess(
      final io.camunda.client.CamundaClient client) {

    return client
        .newProcessDefinitionSearchRequest()
        .send()
        .join()
        .items()
        .stream()
        .filter(definition -> definition.getProcessDefinitionId().endsWith("OldProcessVersionsProcess"));

  }

  /**
   * What the cluster answers the way the adapter asks: definitions which were not
   * deleted.
   */
  private static long activeDefinitionsOfTheTestsProcess(
      final io.camunda.client.CamundaClient client) {

    return client
        .newProcessDefinitionSearchRequest()
        .filter(filter -> filter.state(io.camunda.client.api.search.enums.ProcessDefinitionState.ACTIVE))
        .send()
        .join()
        .items()
        .stream()
        .filter(definition -> definition.getProcessDefinitionId().endsWith("OldProcessVersionsProcess"))
        .count();

  }

  private static org.springframework.context.ConfigurableApplicationContext boot(
      final String version,
      final String... arguments) {

    final var boot = new java.util.ArrayList<String>();
    boot.add("--spring.config.name=camunda8-it");
    boot
        .add("--vanillabp.adapters.c8.rest-address=http://%s:%d".formatted(
            CAMUNDA.getHost(),
            CAMUNDA.getMappedPort(8080)));
    boot
        .add("--vanillabp.adapters.c8.grpc-address=http://%s:%d".formatted(
            CAMUNDA.getHost(),
            CAMUNDA.getMappedPort(26500)));
    boot.add("--vanillabp.adapters.c8.workflow-visibility-timeout=PT60S");
    boot
        .add("--vanillabp.workflow-modules.test-app.adapters.c8.resources-location=classpath*:old-process-versions/%s"
            .formatted(version));
    boot.addAll(java.util.List.of(arguments));
    return new SpringApplicationBuilder(DockerTestApplication.class).run(boot.toArray(String[]::new));

  }

}
