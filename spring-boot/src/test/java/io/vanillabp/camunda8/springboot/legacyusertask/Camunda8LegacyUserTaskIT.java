package io.vanillabp.camunda8.springboot.legacyusertask;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import io.camunda.client.api.search.enums.ProcessInstanceState;
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
 * next to it. The second situation is read twice: once for the tag without a job type, and
 * at the end once more for the tag with a {@code zeebe:taskDefinition} of its own. Both are
 * job-worker user tasks, so both get the same warning.
 * <p>
 * The test asserts what each boot says. It also writes every message it reads to
 * {@code target/legacy-user-task-messages.txt} and to the test report, because the output of
 * a test which passes is suppressed, and the wiki quotes those messages.
 * <p>
 * Two more things are measured here, because the messages rely on them. A user task without
 * {@code zeebe:userTask} gets a job of type {@code io.camunda.zeebe:userTask} even where the
 * model names a job type of its own. And a workflow standing at such a user task reaches
 * VanillaBP only in two steps: a process instance migration to a version with a Camunda user
 * task, which creates that user task without its {@code creating} listeners, and a process
 * instance modification which cancels the token in the user task and adds a new one.
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

  /** The workflow which stands at the job of the old shape. */
  private static long workflowOnTheOldShape;

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
      workflowOnTheOldShape = instance.getProcessInstanceKey();
      record("the earlier deployment: version %d, workflow %d".formatted(version, instance.getProcessInstanceKey()));

      final var deadline = System.currentTimeMillis() + THE_EXPORT_PIPELINE_CATCHES_UP_WITHIN;
      while (jobsOfTheOldShape(client) == 0) {
        if (System.currentTimeMillis() > deadline) {
          throw new AssertionError("the job of the old shape never reached the search");
        }
        Thread.sleep(500);
      }

      // the same tag with a job type of its own, under a process id of its own: which job type
      // does the cluster create?
      final var withAJobType = Files
          .readString(
              Path.of("src/test/resources/legacy-user-task/stephans-tag-with-a-job-type/legacy-user-task.bpmn"),
              StandardCharsets.UTF_8)
          .replace("\"LegacyUserTaskProcess\"", "\"LegacyUserTaskWithAJobTypeProbe\"");
      client
          .newDeployResourceCommand()
          .addResourceStringUtf8(withAJobType, "legacy-user-task-with-a-job-type.bpmn")
          .send()
          .join();
      final var probe = client
          .newCreateInstanceCommand()
          .bpmnProcessId("LegacyUserTaskWithAJobTypeProbe")
          .latestVersion()
          .variables(Map.of("id", "probe-1"))
          .send()
          .join();
      final var probeDeadline = System.currentTimeMillis() + THE_EXPORT_PIPELINE_CATCHES_UP_WITHIN;
      var probeJobs = java.util.List.<String>of();
      while (probeJobs.isEmpty() && (System.currentTimeMillis() < probeDeadline)) {
        Thread.sleep(500);
        probeJobs = client
            .newJobSearchRequest()
            .filter(filter -> filter.processInstanceKey(probe.getProcessInstanceKey()))
            .send()
            .join()
            .items()
            .stream()
            .map(job -> job.getType()
                + "/"
                + job.getKind()
                + "/"
                + job.getState())
            .toList();
      }
      record("a user task with zeebe:taskDefinition type 'manualMatching' gets the job(s): "
          + probeJobs);
      assertEquals(
          java.util.List.of(Camunda8TaskWiring.TASKDEFINITION_USERTASK_WORKER_V1
              + "/BPMN_ELEMENT/CREATED"),
          probeJobs,
          "the cluster ignores the job type of a user task and hands out its own user-task job");
      client.newCancelInstanceCommand(probe.getProcessInstanceKey()).send().join();
    }

  }

  @Test
  @Order(2)
  @DisplayName("(a) Stephan's tag as the version the application deploys now")
  public void theOldTagInTheModelOfThisBoot(
      final TestReporter reporter) throws IOException {

    final var failure = assertThrows(Throwable.class, () -> boot("stephans-tag").close());
    record(reporter, "(a) the tag of an older modeller, no job type", causes(failure));
    final var refused = causes(failure);
    assertTrue(
        refused.contains("does not deploy BPMN process 'LegacyUserTaskProcess' of workflow module 'test-app': "
            + "user task 'manualMatching' has no 'zeebe:userTask'."),
        () -> "process, module and element come first: "
            + refused);
    assertTrue(
        refused.contains("choose \"Camunda user task\" as its implementation (this writes '<zeebe:userTask />') "
            + "and set 'External form reference' to 'manualMatching', the task definition of the "
            + "@WorkflowTask method which serves it."),
        () -> "the fix in the words of the Modeler, with the task definition of the method: "
            + refused);
    assertTrue(
        refused.contains("Older versions of this process which the cluster already holds may keep this shape; "
            + "they are not refused."),
        () -> "the version the cluster holds is not the problem: "
            + refused);
    assertTrue(
        refused.contains("Open right now: 1 job(s) of type 'io.camunda.zeebe:userTask'"),
        () -> "the workflow which stands at the old version is counted: "
            + refused);
    assertFalse(
        refused.contains("formKey"),
        () -> "the element carries no formKey, so nothing is said about a search for one: "
            + refused);

  }

  @Test
  @Order(3)
  @DisplayName("(a) What a current modeller writes, as the version the application deploys now")
  public void theModellerDefaultInTheModelOfThisBoot(
      final TestReporter reporter) throws IOException {

    final var failure = assertThrows(Throwable.class, () -> boot("modeller-default").close());
    record(reporter, "(a) <bpmn:userTask id=\"Activity_0vefbga\">", causes(failure));
    final var refused = causes(failure);
    assertTrue(
        refused.contains("user task 'Activity_0vefbga' has no 'zeebe:userTask'.") && refused
            .contains("set 'External form reference' to the task definition of the @WorkflowTask "
                + "method which serves it."),
        () -> "no method names the element id, so no task definition is guessed: "
            + refused);

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
    final var said = output.getAll().substring(before);
    record(reporter, "(b) older version with the old tag, current version re-modelled", said);
    final var box = said.substring(said.indexOf("VanillaBP 2.0 looked at this application"));
    final var theBox = box.substring(0, box.indexOf("\n-----"));
    assertTrue(
        theBox.contains("Version 1 of this BPMN process, which the cluster still holds, has user task(s) "
            + "without 'zeebe:userTask': 'manualMatching'. A job worker serves such a user task, and "
            + "VanillaBP does not. 1 workflow(s) still run on version 1"),
        () -> "the version and its user task are named in the block of the start: "
            + theBox);
    assertTrue(
        theBox.contains("Migrate those process instances in Camunda Operate to a newer version of this "
            + "process which already uses Camunda user tasks ('zeebe:userTask'), such as version"),
        () -> "and the way out: "
            + theBox);
    assertTrue(
        theBox.contains("cancel the token in the user task and add a new one at the same user task after "
            + "the migration, in Operate with \"Modify instance\""),
        () -> "and the second step, which the migration below proves to be needed: "
            + theBox);
    assertTrue(
        theBox.contains("but a workflow on version 1 which reaches a user task without 'zeebe:userTask' is "
            + "not served at all"),
        () -> "the finding about older versions no longer says only that they keep being served: "
            + theBox);
    assertFalse(
        theBox.contains("open task(s) of this BPMN process which VanillaBP has no record of"),
        () -> "the job of the old user task is never delivered here, so it is no task whose delivery "
            + "could run a method twice: "
            + theBox);
    assertTrue(
        said.contains("wired 0 task(s) and 1 user task(s) of BPMN process 'LegacyUserTaskProcess'"),
        () -> "the wiring counts the user task: "
            + said);

  }

  @Test
  @Order(5)
  @DisplayName("(a) Stephan's tag with a job type, as the version the application deploys now")
  public void theOldTagWithAJobTypeInTheModelOfThisBoot(
      final TestReporter reporter) throws IOException {

    final var failure = assertThrows(Throwable.class, () -> boot("stephans-tag-with-a-job-type").close());
    record(reporter, "(a') the tag of an older modeller with a job type", causes(failure));
    final var refused = causes(failure);
    assertTrue(
        refused.contains("does not deploy BPMN process 'LegacyUserTaskProcess' of workflow module 'test-app': "
            + "user task 'manualMatching' has no 'zeebe:userTask'."),
        () -> "a job type of its own does not make it something else than a job-worker user task: "
            + refused);
    assertTrue(
        refused.contains("The job type in 'zeebe:taskDefinition' ('manualMatching' for 'manualMatching') does "
            + "not change that: the cluster ignores a job type on a user task and hands out a job of type "
            + "'io.camunda.zeebe:userTask'"),
        () -> "the message says why the job type does not help, as measured in the first step: "
            + refused);
    assertTrue(
        refused.contains("set 'External form reference' to 'manualMatching', the task definition of the "
            + "@WorkflowTask method which serves it."),
        () -> "the fix with the task definition of the method: "
            + refused);
    assertFalse(
        refused.contains("matching no task"),
        () -> "and not that the method matches no task: "
            + refused);

  }

  @Test
  @Order(6)
  @DisplayName("Migrating the workflow to the re-modelled version, the way Operate does it")
  public void migratingTheWorkflowToTheRemodelledVersion(
      final TestReporter reporter) throws Exception {

    final var application = assertDoesNotThrow(() -> boot("current"));
    final var aggregate = new LegacyUserTaskAggregate();
    aggregate.setId("legacy-1");
    application.getBean(LegacyUserTaskAggregateRepository.class).save(aggregate);
    final var lines = new ArrayList<String>();
    try (var client = testClient()) {
      // the version this boot deployed: the newest one which carries the listeners VanillaBP
      // writes into a Camunda user task
      io.camunda.client.api.search.response.ProcessDefinition found = null;
      final var targetDeadline = System.currentTimeMillis() + THE_EXPORT_PIPELINE_CATCHES_UP_WITHIN;
      while ((found == null) && (System.currentTimeMillis() < targetDeadline)) {
        Thread.sleep(500);
        for (final var definition : client
            .newProcessDefinitionSearchRequest()
            .filter(filter -> filter.processDefinitionId(THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT))
            .sort(sort -> sort.version().desc())
            .send()
            .join()
            .items()) {
          final var xml = client.newProcessDefinitionGetXmlRequest(definition.getProcessDefinitionKey()).send().join();
          if (xml.contains("zeebe:userTask") && xml.contains("io.vanillabp.userTask:")) {
            found = definition;
            break;
          }
        }
      }
      final var target = java.util.Objects.requireNonNull(found, "the boot deployed no Camunda user task");
      lines.add("target: version %d, key %d".formatted(target.getVersion(), target.getProcessDefinitionKey()));
      final var deliveredBefore = LegacyUserTaskWorkflowService.DELIVERED_TASK_IDS.size();
      try {
        client
            .newMigrateProcessInstanceCommand(workflowOnTheOldShape)
            .migrationPlan(target.getProcessDefinitionKey())
            .addMappingInstruction("manualMatching", "manualMatching")
            .send()
            .join();
        lines.add("migration accepted");
      } catch (final Exception e) {
        lines.add("migration refused: "
            + causes(e));
      }
      // a delivery takes well under a second once the job exists. Twenty seconds without one
      // is the answer
      final var deadline = System.currentTimeMillis() + 20_000;
      while ((LegacyUserTaskWorkflowService.DELIVERED_TASK_IDS.size() == deliveredBefore) && (System
          .currentTimeMillis() < deadline)) {
        Thread.sleep(500);
      }
      lines.add("after migration, deliveries: "
          + LegacyUserTaskWorkflowService.DELIVERED_TASK_IDS);
      lines.add(whatTheClusterShows(client));
      assertTrue(lines.contains("migration accepted"), () -> String.join("\n", lines));
      assertEquals(
          deliveredBefore,
          LegacyUserTaskWorkflowService.DELIVERED_TASK_IDS.size(),
          () -> "the migration creates the Camunda user task without its creating listener: "
              + String.join("\n", lines));
      assertTrue(
          lines.get(lines.size() - 1).contains("/manualMatching/CREATED/"),
          () -> "the user task exists all the same: "
              + String.join("\n", lines));
      {
        final var elementInstance = client
            .newElementInstanceSearchRequest()
            .filter(filter -> filter.processInstanceKey(workflowOnTheOldShape).elementId("manualMatching"))
            .send()
            .join()
            .items();
        lines.add("element instances: "
            + elementInstance.stream()
                .map(item -> item.getElementInstanceKey()
                    + "/"
                    + item.getState()
                    + "/"
                    + item.getProcessDefinitionKey())
                .toList());
        final var active = elementInstance
            .stream()
            .filter(item -> "ACTIVE".equals(String.valueOf(item.getState())))
            .findFirst()
            .orElseThrow();
        client
            .newModifyProcessInstanceCommand(workflowOnTheOldShape)
            .terminateElement(active.getElementInstanceKey())
            .and()
            .activateElement("manualMatching")
            .send()
            .join();
        lines.add("modification accepted");
        final var secondDeadline = System.currentTimeMillis() + THE_EXPORT_PIPELINE_CATCHES_UP_WITHIN;
        while ((LegacyUserTaskWorkflowService.DELIVERED_TASK_IDS.size() == deliveredBefore) && (System
            .currentTimeMillis() < secondDeadline)) {
          Thread.sleep(500);
        }
        lines.add("after modification, deliveries: "
            + LegacyUserTaskWorkflowService.DELIVERED_TASK_IDS);
        lines.add(whatTheClusterShows(client));
        assertEquals(
            deliveredBefore + 1,
            LegacyUserTaskWorkflowService.DELIVERED_TASK_IDS.size(),
            () -> "the token added by the modification creates the user task with its listeners: "
                + String.join("\n", lines));
      }
    } finally {
      application.close();
    }
    record(reporter, "migration to the re-modelled version", String.join("\n", lines));

  }

  @Test
  @Order(7)
  @DisplayName("(b) The old shape with a job type in an older version only, next to a re-modelled one")
  public void theOldTagWithAJobTypeInAnOlderVersion(
      final CapturedOutput output,
      final TestReporter reporter) throws Exception {

    final int heldVersion;
    try (var client = testClient()) {
      final var model = Files
          .readString(
              Path.of("src/test/resources/legacy-user-task/stephans-tag-with-a-job-type/legacy-user-task.bpmn"),
              StandardCharsets.UTF_8)
          .replace("\"LegacyUserTaskProcess\"", "\""
              + THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT
              + "\"");
      // deployed past the adapter, as an earlier release of the application did. The adapter
      // refuses this shape in the model it deploys itself, see the step before
      final var deployed = client
          .newDeployResourceCommand()
          .addResourceStringUtf8(model, "legacy-user-task.bpmn")
          .send()
          .join();
      heldVersion = deployed.getProcesses().getFirst().getVersion();
      final var instance = client
          .newCreateInstanceCommand()
          .bpmnProcessId(THE_PROCESS_AS_THE_CLUSTER_KNOWS_IT)
          .version(heldVersion)
          .variables(Map.of("id", "legacy-2"))
          .send()
          .join();
      record("the earlier deployment with a job type: version %d, workflow %d"
          .formatted(heldVersion, instance.getProcessInstanceKey()));
      // the warning counts the ACTIVE process instances of the held version through the
      // search, so the workflow has to be in that index before the application starts. The
      // job index is a different one, and it can be ahead of the index of process instances
      final var definitionKey = deployed.getProcesses().getFirst().getProcessDefinitionKey();
      final var deadline = System.currentTimeMillis() + THE_EXPORT_PIPELINE_CATCHES_UP_WITHIN;
      while ((client
          .newProcessInstanceSearchRequest()
          .filter(filter -> filter
              .processInstanceKey(instance.getProcessInstanceKey())
              .processDefinitionKey(definitionKey)
              .state(ProcessInstanceState.ACTIVE))
          .send()
          .join()
          .items()
          .isEmpty()) || client
              .newJobSearchRequest()
              .filter(filter -> filter.processInstanceKey(instance.getProcessInstanceKey()))
              .send()
              .join()
              .items()
              .isEmpty()) {
        if (System.currentTimeMillis() > deadline) {
          throw new AssertionError("the workflow on the old shape with a job type, or its job, never reached the "
              + "search");
        }
        Thread.sleep(500);
      }
    }

    final var before = output.getAll().length();
    final var application = assertDoesNotThrow(() -> boot("current"));
    application.close();
    final var said = output.getAll().substring(before);
    record(reporter, "(b') older version with the old tag and a job type, current version re-modelled", said);
    final var box = said.substring(said.indexOf("VanillaBP 2.0 looked at this application"));
    final var theBox = box.substring(0, box.indexOf("\n-----"));
    assertTrue(
        theBox.contains("Version %d of this BPMN process, which the cluster still holds, has user task(s) "
            .formatted(heldVersion)
            + "without 'zeebe:userTask': 'manualMatching'. A job worker serves such a user task, and "
            + "VanillaBP does not. 1 workflow(s) still run on version %d".formatted(heldVersion)),
        () -> "a job type of its own does not keep the held version out of the warning: "
            + theBox);
    assertTrue(
        theBox.contains("Migrate those process instances in Camunda Operate to a newer version of this "
            + "process which already uses Camunda user tasks ('zeebe:userTask'), such as version"),
        () -> "and the way out: "
            + theBox);
    assertTrue(
        theBox.contains("cancel the token in the user task and add a new one at the same user task after "
            + "the migration, in Operate with \"Modify instance\""),
        () -> "and the second step: "
            + theBox);

  }

  private static String whatTheClusterShows(
      final CamundaClient client) throws InterruptedException {

    Thread.sleep(3_000);
    final var userTasks = client
        .newUserTaskSearchRequest()
        .filter(filter -> filter.processInstanceKey(workflowOnTheOldShape))
        .send()
        .join()
        .items()
        .stream()
        .map(task -> task.getUserTaskKey()
            + "/"
            + task.getElementId()
            + "/"
            + task.getState()
            + "/def "
            + task.getProcessDefinitionKey())
        .toList();
    final var jobs = client
        .newJobSearchRequest()
        .filter(filter -> filter.processInstanceKey(workflowOnTheOldShape))
        .send()
        .join()
        .items()
        .stream()
        .map(job -> job.getJobKey()
            + "/"
            + job.getType()
            + "/"
            + job.getState()
            + "/"
            + job.getKind()
            + "/def "
            + job.getProcessDefinitionKey())
        .toList();
    return "user tasks: %s\njobs: %s".formatted(userTasks, jobs);

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
