package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.test.ClusterUnderTest;
import io.vanillabp.camunda8.wiring.Camunda8CancelListeners;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;
import io.vanillabp.spi.service.WorkflowEnd;

/**
 * An instance canceled through the API, against a real cluster.
 * <p>
 * This is the test which measures the claim the unit tests can only pin down halfway: they
 * hold what the adapter writes into a model and what it makes of a job, while only a cluster
 * of the line really runs the listener when somebody terminates an instance. The two are
 * worth telling apart, because a line which stopped running the listener would otherwise show
 * up as a workflow which is canceled without a word and nothing here would turn red.
 * <p>
 * It runs on the 8.10 line and nowhere else: the <code>cancel</code> execution listener of
 * the process element arrived there, and the class is skipped rather than failed on the older
 * lines, so their run costs no container for it. The pull-request checks build the GA lines,
 * so what proves this is the nightly matrix.
 * <p>
 * The instance waits at a SERVICE task. An instance holding a Camunda-managed user task
 * cannot be canceled on 8.10.0-alpha5 at all: the <code>canceling</code> task-listener job is
 * created and never handed out, so the instance stays ACTIVE and the process listener never
 * fires, because the cluster runs it only after every child element has terminated. That is
 * camunda/camunda#58193, and widening this test to a user task is what to do once it is
 * fixed.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@EnabledIf("theLineReportsACancelation")
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
@DirtiesContext
public class Camunda8WorkflowCanceledIT {

  /**
   * Whether the release line this build belongs to reports the cancelation of an instance at
   * all. Read before the container of this class is started, so a line without the construct
   * pays nothing for a test which could not pass.
   *
   * @return Whether to run this class
   */
  static boolean theLineReportsACancelation() {

    return Camunda8CancelListeners.theProcessCanReportItsCancellation();

  }

  @Container
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.cluster();

  @DynamicPropertySource
  static void camunda8Properties(
      final DynamicPropertyRegistry registry) {

    registry.add("vanillabp.adapters.c8.rest-address",
        () -> "http://"
            + CAMUNDA.getHost()
            + ":"
            + CAMUNDA.getMappedPort(8080));
    registry.add("vanillabp.adapters.c8.grpc-address",
        () -> "http://"
            + CAMUNDA.getHost()
            + ":"
            + CAMUNDA.getMappedPort(26500));

  }

  @Autowired
  private CanceledDockerWorkflowService workflowService;

  @Autowired
  private CanceledDockerAggregateRepository repository;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private Camunda8ClientFactoryRegistry clientFactoryRegistry;

  @Test
  @DisplayName("An instance canceled through the API reports its end as TERMINATED")
  public void aCanceledInstanceReportsItsEnd() throws Exception {

    final var aggregateId = transactionTemplate
        .execute(status -> workflowService.startWorkflow(new CanceledDockerAggregate()).getId());
    CanceledDockerWorkflowService.ENDED_AS.remove(String.valueOf(aggregateId));

    awaitUntil(
        () -> openTaskOf(aggregateId) != null,
        60000,
        "the instance to reach its asynchronous task");

    final var instanceKey = theInstanceOf(aggregateId);
    clientFactoryRegistry
        .getFactory("c8")
        .getClient()
        .newCancelInstanceCommand(instanceKey)
        .send()
        .join();

    awaitUntil(
        () -> CanceledDockerWorkflowService.ENDED_AS.get(String.valueOf(aggregateId)) != null,
        60000,
        "the application to be told that the workflow ended");

    assertEquals(
        WorkflowEnd.Kind.TERMINATED.name(),
        CanceledDockerWorkflowService.ENDED_AS.get(String.valueOf(aggregateId)),
        "a cancelation is reported as one, not as a completion");
    assertEquals(
        WorkflowEnd.Kind.TERMINATED.name(),
        transactionTemplate.execute(status -> repository.findById(aggregateId).orElseThrow().getEndedAs()),
        "and the handler ran in a transaction of the application, so the aggregate holds it too");

  }

  /**
   * The job key the asynchronous task handler wrote, or <code>null</code> while the
   * instance has not reached the task yet.
   */
  private String openTaskOf(
      final Long aggregateId) {

    return transactionTemplate
        .execute(status -> repository.findById(aggregateId).map(CanceledDockerAggregate::getOpenTaskId).orElse(null));

  }

  /**
   * The process instance key of the workflow of that aggregate, read from the job the
   * application is holding open.
   * <p>
   * The read goes through the search, which an exporter feeds asynchronously, so it waits
   * for the job to turn up rather than reading once.
   */
  private long theInstanceOf(
      final Long aggregateId) throws Exception {

    final var instanceKey = new AtomicLong(0L);
    awaitUntil(
        () -> {
          final var jobs = clientFactoryRegistry
              .getFactory("c8")
              .getClient()
              .newJobSearchRequest()
              .filter(filter -> filter.jobKey(Long.parseLong(openTaskOf(aggregateId))))
              .send()
              .join()
              .items();
          if (jobs.isEmpty()) {
            return Boolean.FALSE;
          }
          instanceKey.set(jobs.getFirst().getProcessInstanceKey());
          return Boolean.TRUE;
        },
        60000,
        "the search to report the job the application holds open");
    return instanceKey.get();

  }

  private void awaitUntil(
      final Supplier<Boolean> condition,
      final long timeoutMillis,
      final String description) throws InterruptedException {

    final var deadline = System.currentTimeMillis() + timeoutMillis;
    while (!Boolean.TRUE.equals(condition.get())) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("timed out waiting for: "
            + description);
      }
      Thread.sleep(200);
    }

  }

}
