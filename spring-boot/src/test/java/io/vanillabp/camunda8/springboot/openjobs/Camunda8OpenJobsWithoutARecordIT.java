package io.vanillabp.camunda8.springboot.openjobs;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.transaction.support.TransactionTemplate;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.enums.JobState;
import io.vanillabp.camunda8.springboot.TestOnTheSharedCluster;
import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What the startup finding about open tasks without a delivery record counts on Camunda 8.
 * <p>
 * The finding is about a delivery which could run a <code>&#64;WorkflowTask</code> method a
 * second time. So it may count only jobs which are still open, and only jobs this adapter
 * delivers. The index of the cluster keeps a job after it is over. A count which did not leave
 * those out never went down, and every start with an empty delivery log reported tasks which
 * did not exist.
 * <p>
 * Three boots share one database, and the delivery log is emptied before the second and the
 * third. The first boot runs one workflow to its end. Before the second boot a workflow is
 * started while no application runs, so its job is open and nobody has a record of it. That
 * is the one task the second boot has to report. The job which the first boot completed is not.
 * After the second boot delivered the open job, the third boot reports nothing.
 */
@ExtendWith(SuppressOutputExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestOnTheSharedCluster.ItsTestsAreOneScenario
public class Camunda8OpenJobsWithoutARecordIT extends TestOnTheSharedCluster {

  private static final String THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT = "test-app__OpenJobsProcess";

  private static final String THE_FINDING = "open task(s) of this BPMN process which VanillaBP has no record of";

  private static final long THE_EXPORT_PIPELINE_CATCHES_UP_WITHIN = 60_000;

  @Test
  @Order(1)
  @DisplayName("A workflow runs through its service task while the application runs")
  public void aWorkflowRunsToItsEnd() throws Exception {

    try (var application = boot()) {
      final var transactions = application.getBean(TransactionTemplate.class);
      final var repository = application.getBean(OpenJobsAggregateRepository.class);
      final var workflowService = application.getBean(OpenJobsWorkflowService.class);
      transactions.executeWithoutResult(status -> {
        repository.save(aggregate("open-1"));
        workflowService.startWorkflow(aggregate("done-1"));
      });
      waitFor(() -> OpenJobsWorkflowService.SERVED_AGGREGATE_IDS.contains("done-1"), "the method ran");
    }
    try (var client = testClient()) {
      waitFor(() -> jobs(client, JobState.COMPLETED) == 1, "the index shows the job completed");
    }

  }

  @Test
  @Order(2)
  @DisplayName("A job which was opened before the application ran is reported, the completed one is not")
  public void onlyTheOpenJobIsReported(
      final CapturedOutput output) throws Exception {

    try (var client = testClient()) {
      client
          .newCreateInstanceCommand()
          .bpmnProcessId(THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT)
          .latestVersion()
          .variables(Map.of("id", "open-1"))
          .send()
          .join();
      // the start event carries the listener which VanillaBP writes for a start the BPMS decides
      // on, and no application answers it now. The test answers it, the way the cleanup of the
      // shared cluster does, so the workflow reaches its service task
      waitFor(() -> listenerJobsOfTheStart(client) > 0, "the index shows the listener job of the start event");
      final var startListenerType = client
          .newJobSearchRequest()
          .filter(filter -> filter
              .processDefinitionId(THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT)
              .elementId("OJ_Start")
              .state(JobState.CREATED))
          .send()
          .join()
          .items()
          .getFirst()
          .getType();
      client
          .newActivateJobsCommand()
          .jobType(startListenerType)
          .maxJobsToActivate(1)
          .send()
          .join()
          .getJobs()
          .forEach(job -> client.newCompleteCommand(job.getKey()).send().join());
      waitFor(() -> jobs(client, JobState.CREATED) == 1, "the index shows the open job");
    }
    emptyTheDeliveryLog();
    final var before = output.getAll().length();
    try (var application = boot()) {
      final var said = output.getAll().substring(before);
      assertTrue(
          said.contains("This adapter holds 1 "
              + THE_FINDING),
          () -> "the open job, and not the completed one: "
              + said);
      waitFor(() -> OpenJobsWorkflowService.SERVED_AGGREGATE_IDS.contains("open-1"), "the open job was delivered");
    }
    try (var client = testClient()) {
      waitFor(() -> jobs(client, JobState.COMPLETED) == 2, "the index shows both jobs completed");
    }

  }

  @Test
  @Order(3)
  @DisplayName("Jobs which are over are not reported, however empty the delivery log is")
  public void jobsWhichAreOverAreNotReported(
      final CapturedOutput output) throws Exception {

    emptyTheDeliveryLog();
    final var before = output.getAll().length();
    try (var application = boot()) {
      final var said = output.getAll().substring(before);
      assertFalse(
          said.contains(THE_FINDING),
          () -> "both jobs are completed, so no delivery can run the method again: "
              + said);
    }

  }

  private static OpenJobsAggregate aggregate(
      final String id) {

    final var aggregate = new OpenJobsAggregate();
    aggregate.setId(id);
    return aggregate;

  }

  /**
   * Removes every record of a delivery, the way a database which was set up anew would have
   * none. The database is an in-memory one which lives as long as the test JVM, so the test
   * reaches it directly.
   */
  private static void emptyTheDeliveryLog() throws java.sql.SQLException {

    try (var connection = java.sql.DriverManager.getConnection("jdbc:h2:mem:c8-open-jobs;DB_CLOSE_DELAY=-1", "sa",
        ""); var statement = connection.createStatement()) {
      statement.executeUpdate("DELETE FROM VANILLABP_TASK_DELIVERY");
    }

  }

  private static long listenerJobsOfTheStart(
      final CamundaClient client) {

    return client
        .newJobSearchRequest()
        .filter(filter -> filter
            .processDefinitionId(THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT)
            .elementId("OJ_Start")
            .state(JobState.CREATED))
        .send()
        .join()
        .items()
        .size();

  }

  private static long jobs(
      final CamundaClient client,
      final JobState state) {

    final var total = client
        .newJobSearchRequest()
        .filter(filter -> filter
            .processDefinitionId(THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT)
            // the service task only. The start event and the end of the process have listener jobs
            // of their own
            .elementId("OJ_DoTheWork")
            .state(state))
        .page(page -> page.limit(1))
        .send()
        .join()
        .page()
        .totalItems();
    return total == null
        ? 0L
        : total;

  }

  private static void waitFor(
      final java.util.function.BooleanSupplier condition,
      final String what) throws InterruptedException {

    final var deadline = System.currentTimeMillis() + THE_EXPORT_PIPELINE_CATCHES_UP_WITHIN;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("timed out waiting until "
            + what);
      }
      Thread.sleep(250);
    }

  }

  private static CamundaClient testClient() {

    return CamundaClient
        .newClientBuilder()
        .preferRestOverGrpc(true)
        .restAddress(URI.create(restAddress()))
        .grpcAddress(URI.create(grpcAddress()))
        .build();

  }

  private static ArrayList<String> bootArguments() {

    final var boot = new ArrayList<String>();
    boot.add("--spring.config.name=camunda8-open-jobs-it");
    boot
        .add("--vanillabp.adapters.c8.rest-address="
            + restAddress());
    boot
        .add("--vanillabp.adapters.c8.grpc-address="
            + grpcAddress());
    return boot;

  }

  private static ConfigurableApplicationContext boot() {

    return new SpringApplicationBuilder(OpenJobsTestApplication.class).run(bootArguments().toArray(String[]::new));

  }

}
