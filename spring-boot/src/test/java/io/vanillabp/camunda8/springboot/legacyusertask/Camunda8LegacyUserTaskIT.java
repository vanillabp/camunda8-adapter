package io.vanillabp.camunda8.springboot.legacyusertask;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import io.camunda.client.CamundaClient;
import io.vanillabp.camunda8.springboot.TestOnTheSharedCluster;
import io.vanillabp.camunda8.wiring.Camunda8TaskWiring;
import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What an application is told about a user task which an older Camunda Modeler drew. Such a
 * user task has no {@code zeebe:userTask}, so a job worker serves it. This adapter does not
 * take that shape.
 * <p>
 * Two situations are read against a real cluster. In the first, the shape is in the model the
 * application deploys now. In the second, the cluster holds the shape in an older version
 * only, and a workflow still stands at it. The application deploys a re-modelled version
 * next to it.
 * <p>
 * The test is a reproduction. It writes every message it reads to
 * {@code target/legacy-user-task-messages.txt} and to the test report, because the output of
 * a test which passes is suppressed. It asserts only the outcome of each boot.
 */
@ExtendWith(SuppressOutputExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestOnTheSharedCluster.ItsTestsAreOneScenario
public class Camunda8LegacyUserTaskIT extends TestOnTheSharedCluster {

  /**
   * The process under the id by which the CLUSTER knows it. This module runs with
   * {@code use-prefix}.
   */
  private static final String THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT = "test-app__LegacyUserTaskProcess";

  private static final Path THE_RECORD = Path.of("target", "legacy-user-task-messages.txt");

  private static final long THE_EXPORT_PIPELINE_CATCHES_UP_WITHIN = 60_000;

  @Test
  @Order(1)
  @DisplayName("An earlier deployment left the old shape in the cluster, and a workflow stands at it")
  public void theClusterHoldsTheOldShape() throws Exception {

    Files.deleteIfExists(THE_RECORD);
    final var model = Files
        .readString(
            Path.of("src/test/resources/legacy-user-task/stephans-tag/legacy-user-task.bpmn"),
            StandardCharsets.UTF_8)
        .replace("\"LegacyUserTaskProcess\"", "\""
            + THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT
            + "\"");
    try (var client = testClient()) {
      final var deployed = client
          .newDeployResourceCommand()
          .addResourceStringUtf8(model, "legacy-user-task.bpmn")
          .send()
          .join();
      final var version = deployed.getProcesses().getFirst().getVersion();
      final var instance = client
          .newCreateInstanceCommand()
          .bpmnProcessId(THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT)
          .version(version)
          .variables(Map.of("id", "legacy-1"))
          .send()
          .join();
      record("the earlier deployment: version %d, workflow %d".formatted(version, instance.getProcessInstanceKey()));

      final var deadline = System.currentTimeMillis() + THE_EXPORT_PIPELINE_CATCHES_UP_WITHIN;
      while (jobsOfTheOldShape(client) == 0) {
        if (System.currentTimeMillis() > deadline) {
          throw new AssertionError("the job of the old shape never reached the search");
        }
        Thread.sleep(500);
      }
    }

  }

  @Test
  @Order(2)
  @DisplayName("(a) Stephan's tag as the version the application deploys now")
  public void theOldTagInTheModelOfThisBoot(
      final TestReporter reporter) throws IOException {

    final var failure = assertThrows(Throwable.class, () -> boot("stephans-tag").close());
    record(reporter, "(a) the tag of an older modeller, no job type", causes(failure));

  }

  @Test
  @Order(3)
  @DisplayName("(a) What a current modeller writes, as the version the application deploys now")
  public void theModellerDefaultInTheModelOfThisBoot(
      final TestReporter reporter) throws IOException {

    final var failure = assertThrows(Throwable.class, () -> boot("modeller-default").close());
    record(reporter, "(a) <bpmn:userTask id=\"Activity_0vefbga\">", causes(failure));

  }

  @Test
  @Order(4)
  @DisplayName("(b) The old shape in an older version only, next to a re-modelled one")
  public void theOldTagInAnOlderVersion(
      final CapturedOutput output,
      final TestReporter reporter) throws Exception {

    final var before = output.getAll().length();
    final var application = assertDoesNotThrow(() -> boot("current"));
    try {
      // whatever the running application says about the standing workflow
      Thread.sleep(10_000);
    } finally {
      application.close();
    }
    record(reporter, "(b) older version with the old tag, current version re-modelled", output
        .getAll()
        .substring(before));

  }

  @Test
  @Order(5)
  @DisplayName("(a) Stephan's tag with a job type, as the version the application deploys now")
  public void theOldTagWithAJobTypeInTheModelOfThisBoot(
      final CapturedOutput output,
      final TestReporter reporter) throws IOException {

    final var before = output.getAll().length();
    try {
      boot("stephans-tag-with-a-job-type").close();
      record(reporter, "(a) the tag of an older modeller with a job type: the boot went on", output
          .getAll()
          .substring(before));
    } catch (final Throwable failure) {
      record(reporter, "(a) the tag of an older modeller with a job type: the boot ended", causes(failure));
    }

  }

  private static long jobsOfTheOldShape(
      final CamundaClient client) {

    return client
        .newJobSearchRequest()
        .filter(filter -> filter
            .processDefinitionId(THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT)
            .type(Camunda8TaskWiring.TASKDEFINITION_USERTASK_WORKER_V1))
        .send()
        .join()
        .items()
        .size();

  }

  private static String causes(
      final Throwable failure) {

    final var messages = new ArrayList<String>();
    for (var cause = failure; cause != null; cause = cause.getCause()) {
      messages
          .add(cause.getClass().getName()
              + ": "
              + cause.getMessage());
    }
    return String.join("\n  caused by ", messages);

  }

  private static void record(
      final String text) throws IOException {

    Files.createDirectories(THE_RECORD.getParent());
    Files
        .writeString(THE_RECORD, text
            + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

  }

  private static void record(
      final TestReporter reporter,
      final String situation,
      final String whatWasSaid) throws IOException {

    reporter.publishEntry(situation, whatWasSaid);
    record("===== "
        + situation
        + "\n"
        + whatWasSaid
        + "\n");

  }

  private static CamundaClient testClient() {

    return CamundaClient
        .newClientBuilder()
        .preferRestOverGrpc(true)
        .restAddress(URI.create(restAddress()))
        .grpcAddress(URI.create(grpcAddress()))
        .build();

  }

  private static ConfigurableApplicationContext boot(
      final String modelDirectory) {

    final var boot = new ArrayList<String>();
    boot.add("--spring.config.name=camunda8-legacy-user-task-it");
    boot
        .add("--vanillabp.adapters.c8.rest-address="
            + restAddress());
    boot
        .add("--vanillabp.adapters.c8.grpc-address="
            + grpcAddress());
    boot
        .add("--vanillabp.workflow-modules.test-app.adapters.c8.resources-location=classpath*:legacy-user-task/%s"
            .formatted(modelDirectory));
    return new SpringApplicationBuilder(LegacyUserTaskTestApplication.class).run(boot.toArray(String[]::new));

  }

}
