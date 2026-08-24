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
 * Both cases are full boots, because the question is what a START reports, and the
 * findings are read from the captured output: Spring Boot resets the logging context
 * while it starts, which takes a log appender attached beforehand with it.
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
      final CapturedOutput output) {

    final var before = output.getAll().length();
    boot("v2", "--vanillabp.workflow-modules.test-app.adapters.c8.outfaded-versions=<2").close();
    final var reported = output.getAll().substring(before);

    assertTrue(reported.contains("droppedInVersionTwo"), "the method serving the faded-out version is named");
    assertTrue(reported.contains("the method never runs"), "and what that means is said");
    assertTrue(reported.contains("faded out by"), "and why");

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
