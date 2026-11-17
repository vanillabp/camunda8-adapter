package io.vanillabp.camunda8.springboot.foreigncluster;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vanillabp.camunda8.test.ClusterUnderTest;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * A start against a cluster which is not the one the delivery log was written with. A new
 * cluster hands out the job keys of the old one again, so a record of the old cluster would
 * answer a delivery of the new one. The start ends instead, before the first task is
 * delivered.
 * <p>
 * Two clusters of its own, one after the other, and one database which outlives both: the
 * first boot runs a workflow on the first cluster and leaves a record. The second boot meets
 * the same cluster and starts as usual. The third boot meets a new cluster and ends. The
 * identity is a cluster variable from 8.9 on and the name of a marker deployment on 8.8, so
 * this class is run on both lines.
 */
@ExtendWith(SuppressOutputExtension.class)
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Camunda8ForeignClusterAtStartupIT {

  private static final long THE_METHOD_RUNS_WITHIN = 60_000;

  private static GenericContainer<?> cluster;

  @AfterAll
  public static void stopTheCluster() {

    if (cluster != null) {
      cluster.stop();
    }

  }

  @Test
  @Order(1)
  @DisplayName("The first boot runs a workflow and leaves a record")
  public void theFirstBootLeavesARecord() throws Exception {

    cluster = ClusterUnderTest.cluster("first-cluster");
    cluster.start();
    try (var application = boot()) {
      final var transactions = application.getBean(TransactionTemplate.class);
      final var workflowService = application.getBean(ForeignClusterWorkflowService.class);
      transactions.executeWithoutResult(status -> workflowService.startWorkflow(aggregate("first")));
      waitFor(() -> ForeignClusterWorkflowService.SERVED_AGGREGATE_IDS.contains("first"), "the method ran");
    }

  }

  @Test
  @Order(2)
  @DisplayName("The same cluster after a restart starts as usual")
  public void theSameClusterStartsAsUsual() {

    try (var application = boot()) {
      assertTrue(application.isRunning());
    }

  }

  @Test
  @Order(3)
  @DisplayName("A new cluster with the same delivery log ends the start")
  public void aNewClusterEndsTheStart() {

    cluster.stop();
    cluster = ClusterUnderTest.cluster("second-cluster");
    cluster.start();

    final var failure = assertThrows(RuntimeException.class, Camunda8ForeignClusterAtStartupIT::boot);

    final var message = messagesOf(failure);
    assertTrue(
        message.contains("The BPMS behind adapter 'c8' is not the one this application met before"),
        () -> "the start names the changed cluster: "
            + message);
    assertTrue(
        message.contains("To go on, choose one of these"),
        () -> "the start guides the developer to a way out: "
            + message);
    assertTrue(
        message.contains("'vanillabp.adapters.c8.*'"),
        () -> "the first way is correcting the address of a wrong cluster: "
            + message);
    assertTrue(
        message.contains("ADAPTER_ID 'c8'"),
        () -> "the second way says which records to clean up: "
            + message);

  }

  private static String messagesOf(
      final Throwable failure) {

    final var messages = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      messages
          .append(cause.getMessage())
          .append('\n');
    }
    return messages.toString();

  }

  private static ForeignClusterAggregate aggregate(
      final String id) {

    final var aggregate = new ForeignClusterAggregate();
    aggregate.setId(id);
    return aggregate;

  }

  private static void waitFor(
      final java.util.function.BooleanSupplier condition,
      final String what) throws InterruptedException {

    final var deadline = System.currentTimeMillis() + THE_METHOD_RUNS_WITHIN;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("timed out waiting until "
            + what);
      }
      Thread.sleep(250);
    }

  }

  private static ConfigurableApplicationContext boot() {

    final var arguments = List
        .of(
            "--spring.config.name=camunda8-foreign-cluster-it",
            "--vanillabp.adapters.c8.rest-address="
                + ClusterUnderTest.restAddress(cluster),
            "--vanillabp.adapters.c8.grpc-address="
                + ClusterUnderTest.grpcAddress(cluster));
    return new SpringApplicationBuilder(ForeignClusterTestApplication.class).run(arguments.toArray(String[]::new));

  }

}
