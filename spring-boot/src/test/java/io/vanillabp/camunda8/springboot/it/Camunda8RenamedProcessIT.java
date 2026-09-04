package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;

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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Renaming a BPMN process, against a real cluster and across two generations of one
 * application: the first deploys the process under its old id and starts a workflow which
 * then waits for a message, the second deploys the same model under the NEW id and
 * declares the old one as a secondary process. The workflow started before the rename has
 * to run to its end through the methods of that second application - which is the whole
 * promise of the declaration.
 * <p>
 * Two things make this a test only a cluster can answer. The job workers of the second
 * application subscribe to the task types of the model they deployed, and the jobs they
 * are handed belong to a process id that application does not deploy any more; and the
 * message correlated for the aggregate has to reach a subscription created by the first
 * application under the old id. Both are cases where a wrong translation between the id
 * the cluster knows and the id the core is keyed by would show up, and neither can be
 * faked.
 * <p>
 * The cluster runs WITHOUT secondary storage on purpose: what is asked here is whether the
 * workflows keep running, not what the startup check reports about their versions - that
 * report needs the query API and is held by {@code Camunda8OldProcessVersionsIT} and by
 * the platform's own {@code RenamedBpmnProcessTest}.
 * <p>
 * The workflow module of this scenario deliberately scopes nothing
 * ('name-clash-avoidance: none'), unlike every other integration test of this module: with
 * 'use-prefix' a task definition carries the BPMN process id it was deployed with, so the
 * jobs of the workflows under the old id are named after the OLD id and the workers of the
 * renamed application never ask for them. The adapter reports that while starting, and it
 * is the one thing a rename has to know about prefixed identifiers.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Camunda8RenamedProcessIT {

  @Container
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.standaloneBroker();

  /**
   * The workflow started by the first application, read by the second one from the same
   * database - a workflow which outlives an upgrade is the point of this test.
   */
  static Long orderId;

  @Test
  @Order(1)
  @DisplayName("A workflow is started under the old process id and waits")
  public void aWorkflowIsStartedUnderTheOldId() throws Exception {

    final var application = boot("rename-before", "v1");
    try {
      final var workflowService = application.getBean(RenamedBeforeDockerWorkflowService.class);
      final var repository = application.getBean(RenamedDockerAggregateRepository.class);
      final var aggregate = application
          .getBean(TransactionTemplate.class)
          .execute(status -> workflowService.startWorkflow());
      orderId = aggregate.getOrderId();

      // the first task ran, so the workflow is on its way and reached the message it
      // waits for - which is where it stands while the application is upgraded
      awaitUntil(
          () -> repository.findById(orderId).orElseThrow().getStartedBy() != null,
          "the workflow of the old process id did not reach its first task");
      assertEquals(
          "before-the-rename",
          repository.findById(orderId).orElseThrow().getStartedBy(),
          "the application before the rename served the first task");
    } finally {
      application.close();
    }

  }

  @Test
  @Order(2)
  @DisplayName("The renamed application finishes the workflow which runs under the old id")
  public void theWorkflowOfTheOldIdIsFinishedAfterTheRename() throws Exception {

    assertNotNull(orderId, "the workflow of the first case has to exist");

    final var application = boot("rename-after", "v2");
    try {
      final var workflowService = application.getBean(RenamedAfterDockerWorkflowService.class);
      final var repository = application.getBean(RenamedDockerAggregateRepository.class);

      // the message reaches a subscription the FIRST application created, under the id
      // this application does not deploy any more
      application
          .getBean(TransactionTemplate.class)
          .executeWithoutResult(status -> workflowService.continueWorkflow(orderId));

      awaitUntil(
          () -> repository.findById(orderId).orElseThrow().getFinishedBy() != null,
          "the workflow started under the old process id did not reach its last task");
      assertEquals(
          "after-the-rename",
          repository.findById(orderId).orElseThrow().getFinishedBy(),
          "the methods of the renamed application served the workflow of the old id");
    } finally {
      application.close();
    }

  }

  /**
   * Waits for something the cluster and the job workers have to bring about. Generous on
   * purpose: in a full build this class shares its machine with the other clusters of this
   * module, and a deadline close to what a quiet machine needs fails while nothing is
   * wrong.
   */
  private static void awaitUntil(
      final java.util.function.BooleanSupplier condition,
      final String whatDidNotHappen) throws Exception {

    final var deadline = System.currentTimeMillis() + 180_000;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError(whatDidNotHappen);
      }
      Thread.sleep(200);
    }

  }

  /**
   * One generation of the application: the workflow service of that generation (a Spring
   * profile decides which one) and the BPMN it deploys.
   * <p>
   * Both boots share ONE in-memory database, kept alive across them by
   * {@code DB_CLOSE_DELAY=-1}: the workflow aggregate written by the first application is
   * what the second one loads when the cluster delivers the last task of that workflow.
   */
  private static ConfigurableApplicationContext boot(
      final String profile,
      final String bpmnVersion) {

    final var boot = new ArrayList<String>();
    boot.add("--spring.config.name=camunda8-it");
    boot.add("--spring.profiles.active="
        + profile);
    boot.add("--spring.datasource.url=jdbc:h2:mem:c8-renamed-process;DB_CLOSE_DELAY=-1");
    boot.add("--spring.datasource.generate-unique-name=false");
    boot.add("--spring.jpa.hibernate.ddl-auto=update");
    boot
        .add("--vanillabp.adapters.c8.rest-address=http://%s:%d".formatted(
            CAMUNDA.getHost(),
            CAMUNDA.getMappedPort(8080)));
    boot
        .add("--vanillabp.adapters.c8.grpc-address=http://%s:%d".formatted(
            CAMUNDA.getHost(),
            CAMUNDA.getMappedPort(26500)));
    boot.add("--vanillabp.adapters.c8.workflow-visibility-timeout=PT60S");
    // this scenario does not prefix its identifiers, and that is not a detail: under
    // 'use-prefix' a task definition carries the BPMN process id, so the jobs of the
    // workflows running under the OLD id are named after that id and no worker of the
    // renamed application asks for them. The adapter says so while starting, and the
    // wiki page about renaming a process is where the ways out are written down
    boot.add("--vanillabp.workflow-modules.test-app.adapters.c8.name-clash-avoidance=none");
    boot
        .add("--vanillabp.workflow-modules.test-app.adapters.c8.resources-location=classpath*:renamed-process/%s"
            .formatted(bpmnVersion));
    return new SpringApplicationBuilder(DockerTestApplication.class).run(boot.toArray(String[]::new));

  }

}
