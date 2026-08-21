package io.vanillabp.camunda8.springboot.election;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.vanillabp.camunda8.springboot.it.ClusterUnderTest;
import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The acceptance test of story 103: two <code>camunda8</code> adapter ids on ONE cluster
 * are told apart by the scope they deployed under, not by the first entry of
 * <code>prioritized-adapters</code>.
 * <p>
 * The scenario is the migration the adapter documents as supported. A workflow is started
 * while only <code>c8-plain</code> exists and waits for a message. The application is then
 * restarted with a second adapter id <code>c8-prefix</code> in FIRST priority, which
 * deploys the same process under a prefixed identifier onto the same cluster. Correlating
 * the message must reach the workflow of the OLD deployment.
 * <p>
 * <b>What fails without the fix.</b> Both adapters find the instance, because the search
 * behind <code>awarenessOfWorkflow</code> filtered by the aggregate-ID variable alone, and
 * the walk stops at the first <code>ACTIVE</code>. The message is then published as
 * <code>test-app__ElectionMessage</code>, which nobody subscribes to, and the cluster
 * buffers it until its time to live passes. Nothing fails, the workflow simply never
 * continues, which is why the assertion here is a task that ran.
 * <p>
 * The tenant is deliberately not part of this test. Two ids differing in
 * <code>use-prefix</code> versus <code>none</code> live in the default tenant and are told
 * apart by the process definition id alone, which needs no authenticated cluster; the
 * tenant is one more comparison in the same scope key and is covered by unit tests.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
public class Camunda8SharedClusterElectionIT {

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

  /**
   * The election probes ask the query API, so this cluster exports to Elasticsearch.
   * Without it an application configuring two ids on one cluster does not boot at all,
   * which is the other half of what story 103 decided.
   */
  @Container
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.withSecondaryStorage(NETWORK, ELASTICSEARCH);

  private ConfigurableApplicationContext application;

  private static String restAddress() {

    return "http://"
        + CAMUNDA.getHost()
        + ":"
        + CAMUNDA.getMappedPort(8080);

  }

  private static String grpcAddress() {

    return "http://"
        + CAMUNDA.getHost()
        + ":"
        + CAMUNDA.getMappedPort(26500);

  }

  /**
   * @param withPrefixAdapter Whether the second adapter id is configured, in first
   *          priority - which is what the restart of the migration does
   */
  private ConfigurableApplicationContext boot(
      final boolean withPrefixAdapter) {

    final var arguments = new java.util.LinkedList<String>();
    arguments.add("--spring.config.name=camunda8-election-it");
    arguments.add("--spring.main.web-application-type=none");
    arguments.add("--vanillabp.adapters.c8-plain.rest-address="
        + restAddress());
    arguments.add("--vanillabp.adapters.c8-plain.grpc-address="
        + grpcAddress());
    // the correlation follows the start closely, and the exporter of a cold cluster
    // needs longer than the production default to make the workflow findable
    arguments.add("--vanillabp.adapters.c8-plain.workflow-visibility-timeout=PT60S");
    if (withPrefixAdapter) {
      arguments.add("--vanillabp.adapters.c8-prefix.type=camunda8");
      arguments.add("--vanillabp.adapters.c8-prefix.name-clash-avoidance=use-prefix");
      arguments.add("--vanillabp.adapters.c8-prefix.rest-address="
          + restAddress());
      arguments.add("--vanillabp.adapters.c8-prefix.grpc-address="
          + grpcAddress());
      arguments.add("--vanillabp.adapters.c8-prefix.workflow-visibility-timeout=PT60S");
      arguments.add(
          "--vanillabp.workflow-modules.test-app.adapters.c8-prefix.resources-location=classpath:it-election");
      // the new deployment takes over new workflows, which is what makes the old ones
      // the ones the election has to find
      arguments.add("--vanillabp.prioritized-adapters[0]=c8-prefix");
      arguments.add("--vanillabp.prioritized-adapters[1]=c8-plain");
    }
    return new SpringApplicationBuilder(ElectionTestApplication.class)
        .run(arguments.toArray(new String[0]));

  }

  @AfterEach
  public void closeWhatIsLeft() {

    if ((application != null) && application.isActive()) {
      application.close();
    }
    ElectionWorkflowService.reset();

  }

  private <T> T bean(
      final Class<T> type) {

    return application.getBean(type);

  }

  @Test
  @DisplayName("A message reaches the workflow of the adapter which started it, not the one in first priority")
  public void theMessageReachesTheAdapterHoldingTheWorkflow(
      final CapturedOutput output) throws Exception {

    // the application as it was before the migration: one adapter, no prefixes
    ElectionWorkflowService.reset();
    application = boot(false);
    final var aggregateId = bean(TransactionTemplate.class)
        .execute(status -> bean(ElectionWorkflowService.class)
            .startWorkflow()
            .getId());
    assertNotNull(aggregateId, "the workflow was started by 'c8-plain'");
    application.close();

    // and after it: the same cluster, a second adapter id deploying the same process
    // under a prefixed identifier, first in the prioritized list
    application = boot(true);
    final var aggregate = bean(ElectionAggregateRepository.class)
        .findById(aggregateId)
        .orElseThrow();
    bean(TransactionTemplate.class)
        .executeWithoutResult(
            status -> bean(ElectionWorkflowService.class).theMessageArrived(aggregate));

    assertTrue(
        ElectionWorkflowService.SERVED.await(2, TimeUnit.MINUTES),
        "the message reached the workflow started before the migration - without the scope check it is "
            + "published under the prefixed name into a cluster where nobody subscribes to it");

    final var logged = output.getOut() + output.getErr();
    assertTrue(
        logged.contains("Camunda8[c8-plain]: published message"),
        "and the adapter holding the workflow is the one which published it: "
            + logged);
    assertFalse(
        logged.contains("Camunda8[c8-prefix]: published message"),
        "the adapter of the new deployment must not answer for a workflow of the old one: "
            + logged);

  }

}
