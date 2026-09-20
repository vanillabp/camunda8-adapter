package io.vanillabp.camunda8.wiring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.CamundaFuture;
import io.camunda.client.api.ProblemDetail;
import io.camunda.client.api.command.ClientHttpException;
import io.camunda.client.api.command.ClientStatusException;
import io.camunda.client.api.command.ProblemException;
import io.camunda.client.api.command.UpdateTimeoutJobCommandStep1;
import io.camunda.client.api.response.UpdateTimeoutJobResponse;
import io.grpc.Status;
import io.vanillabp.integration.adapter.spi.workflowtask.OpenTaskProbe;
import io.vanillabp.integration.adapter.spi.workflowtask.TaskExistence;
import io.vanillabp.integration.adapter.spi.workflowtask.TaskInvocationContext;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskInvoker;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The one question this adapter answers for the core's check of the other open tasks of a
 * workflow: does the cluster still have this task.
 * <p>
 * Three answers and not two. Only "gone" produces a cancelation, so an answer which cannot
 * tell a refusal from an outage has to say so - reading a hiccup as "gone" would report every
 * open task of a workflow as canceled whenever the cluster stumbles.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8OpenTaskProbeTest {

  private static final String TASK_ID = "2251799813685249";

  private static final String WORKFLOW_ID = "2251799813685240";

  private static final String USER_TASK = "the-form-of-a-user-task";

  private static final String SERVICE_TASK = "the-service-task";

  private final CamundaClient client = mock(CamundaClient.class);

  @Test
  @DisplayName("A job the cluster takes the command for is still there")
  public void aJobTheClusterHasIsStillThere() {

    theClusterTakesTheJobTimeoutUpdate();

    assertEquals(TaskExistence.STILL_THERE, probeOfAModuleWithoutUserTasks().stillExists(TASK_ID, false));

  }

  @Test
  @DisplayName("A job the cluster does not hold is gone, on both transports")
  public void aJobTheClusterDoesNotHoldIsGone() {

    theClusterRefusesTheJobTimeoutUpdate(new ClientHttpException("Failed with code 404", 404, "job not found"));
    assertEquals(TaskExistence.GONE, probeOfAModuleWithoutUserTasks().stillExists(TASK_ID, false));

    theClusterRefusesTheJobTimeoutUpdate(new ClientStatusException(Status.NOT_FOUND, null));
    assertEquals(TaskExistence.GONE, probeOfAModuleWithoutUserTasks().stillExists(TASK_ID, false));

  }

  @Test
  @DisplayName("A job waiting in the queue is still there, not gone")
  public void aJobInTheQueueIsStillThere() {

    // the cluster HAS the job and no worker has it activated right now, which is what an
    // asynchronous task whose lock ran out looks like
    theClusterRefusesTheJobTimeoutUpdate(problem(400, "INVALID_ARGUMENT", "but it is not active"));

    assertEquals(TaskExistence.STILL_THERE, probeOfAModuleWithoutUserTasks().stillExists(TASK_ID, false));

  }

  @Test
  @DisplayName("A cluster which did not answer is 'cannot say' and cancels nothing")
  public void aClusterWhichDidNotAnswerCancelsNothing() {

    theClusterRefusesTheJobTimeoutUpdate(new IllegalStateException("connection reset"));

    assertEquals(TaskExistence.CANNOT_SAY, probeOfAModuleWithoutUserTasks().stillExists(TASK_ID, false));

  }

  @Test
  @DisplayName("A task id which is no key of this cluster is 'cannot say'")
  public void aTaskIdOfAnotherBpmsIsCannotSay() {

    theClusterTakesTheJobTimeoutUpdate();

    assertEquals(TaskExistence.CANNOT_SAY, probeOfAModuleWithoutUserTasks().stillExists("not-a-key", false));
    assertEquals(TaskExistence.CANNOT_SAY, probeOfAModuleWithoutUserTasks().stillExists(null, false));
    verify(client, never()).newUpdateTimeoutCommand(Mockito.anyLong());

  }

  @Test
  @DisplayName("In a process with user tasks a 404 is 'cannot say', not 'gone'")
  public void aProcessWithUserTasksCancelsNothing() {

    // the record of a user-task delivery keeps the USER-TASK key, and a job command
    // answers NOT_FOUND for one of those as long as the task is open. Nothing in the
    // question tells the two apart, so this adapter says so rather than canceling a task
    // the cluster is holding out to somebody
    theClusterRefusesTheJobTimeoutUpdate(new ClientHttpException("Failed with code 404", 404, "job not found"));

    assertEquals(
        TaskExistence.CANNOT_SAY,
        probeOfAModuleWithoutUserTasks().stillExists(TASK_ID, true));

  }

  @Test
  @DisplayName("Nothing is asked about a user task, on any path")
  public void noUserTaskCommandIsEverSent() {

    theClusterRefusesTheJobTimeoutUpdate(new ClientHttpException("Failed with code 404", 404, "job not found"));

    probeOfAModuleWithoutUserTasks().stillExists(TASK_ID, false);
    probeOfAModuleWithoutUserTasks().stillExists(TASK_ID, true);

    verify(client, never()).newUpdateUserTaskCommand(Mockito.anyLong());

  }

  @Test
  @DisplayName("The core is handed the wake-up and the probe, and a failure stays here")
  public void theCoreIsHandedTheWakeUp() {

    final var asked = new ArrayList<OpenTaskProbe>();
    final var invoker = anInvokerWhich(asked, false);
    final var wakeUp = mock(TaskInvocationContext.class);

    probeOf(invoker, false).reportWhatTheClusterNoLongerHas("TestProcess", wakeUp);

    assertEquals(1, asked.size(), "the core is asked to look at the other open tasks, once");

  }

  @Test
  @DisplayName("A core which throws never fails the job the check rode in on")
  public void aCoreWhichThrowsIsSwallowed() {

    final var invoker = anInvokerWhich(new ArrayList<>(), true);
    final var wakeUp = mock(TaskInvocationContext.class);

    org.junit.jupiter.api.Assertions
        .assertDoesNotThrow(() -> probeOf(invoker, false).reportWhatTheClusterNoLongerHas("TestProcess", wakeUp));

  }

  @Test
  @DisplayName("Without a core entry point or a wake-up nothing is asked")
  public void withoutTheHalvesNothingIsAsked() {

    final var asked = new ArrayList<OpenTaskProbe>();
    final var invoker = anInvokerWhich(asked, false);

    probeOf(invoker, false).reportWhatTheClusterNoLongerHas("TestProcess", null);
    probeOf(null, false).reportWhatTheClusterNoLongerHas("TestProcess", mock(TaskInvocationContext.class));

    assertTrue(asked.isEmpty());

  }

  @Test
  @DisplayName("One user task in a model no longer costs its service tasks their cancelation")
  public void theRefusalIsPerRecordAndNotPerProcess() {

    // the cluster answers NOT_FOUND for both of them: for the service task because its job
    // is gone, for the user task because a job command never finds a USER-TASK key
    theClusterRefusesTheJobTimeoutUpdate(new ClientHttpException("Failed with code 404", 404, "job not found"));
    final var asked = new ArrayList<OpenTaskProbe>();
    final var probe = probeOf(anInvokerWhich(asked, false), (
        bpmnProcessId,
        taskDefinition) -> USER_TASK.equals(taskDefinition));

    probe.reportWhatTheClusterNoLongerHas("TestProcess", mock(TaskInvocationContext.class));

    final var handedToTheCore = asked.get(0);
    // both halves in ONE test, so nobody can pass it by weakening the model
    assertEquals(
        TaskExistence.GONE,
        handedToTheCore.stillExists(WORKFLOW_ID, TASK_ID, SERVICE_TASK),
        "the service task of a process which also holds a user task is derived as canceled");
    assertEquals(
        TaskExistence.CANNOT_SAY,
        handedToTheCore.stillExists(WORKFLOW_ID, TASK_ID, USER_TASK),
        "and the user task of the same process is not");

  }

  @Test
  @DisplayName("A record which kept no task definition is answered by what its process holds")
  public void aRecordWithoutATaskDefinition() {

    theClusterRefusesTheJobTimeoutUpdate(new ClientHttpException("Failed with code 404", 404, "job not found"));
    final var asked = new ArrayList<OpenTaskProbe>();

    probeOf(anInvokerWhich(asked, false), true)
        .reportWhatTheClusterNoLongerHas("TestProcess", mock(TaskInvocationContext.class));
    assertEquals(
        TaskExistence.CANNOT_SAY,
        asked.get(0).stillExists(WORKFLOW_ID, TASK_ID),
        "nothing tells the two kinds of task apart here, so nothing is reported");

    asked.clear();
    probeOf(anInvokerWhich(asked, false), false)
        .reportWhatTheClusterNoLongerHas("TestProcess", mock(TaskInvocationContext.class));
    assertEquals(
        TaskExistence.GONE,
        asked.get(0).stillExists(WORKFLOW_ID, TASK_ID),
        "and where the process holds no such user task the answer is the whole answer");

  }

  private Camunda8OpenTaskProbe probeOfAModuleWithoutUserTasks() {

    return probeOf(mock(WorkflowTaskInvoker.class), false);

  }

  private Camunda8OpenTaskProbe probeOf(
      final WorkflowTaskInvoker invoker,
      final boolean everyRecordMayBeAUserTask) {

    return probeOf(invoker, (
        bpmnProcessId,
        taskDefinition) -> everyRecordMayBeAUserTask);

  }

  private Camunda8OpenTaskProbe probeOf(
      final WorkflowTaskInvoker invoker,
      final BiPredicate<String, String> theRecordMayNameACamundaManagedUserTask) {

    return new Camunda8OpenTaskProbe(
        "c8", "test-module", invoker, () -> client, Duration
            .ofHours(1), theRecordMayNameACamundaManagedUserTask);

  }

  /**
   * A core which records the probe it was handed, and optionally throws while doing it.
   */
  private static WorkflowTaskInvoker anInvokerWhich(
      final List<OpenTaskProbe> asked,
      final boolean throwsUp) {

    final var invoker = mock(WorkflowTaskInvoker.class);
    Mockito
        .lenient()
        .doAnswer(invocation -> {
          if (throwsUp) {
            throw new IllegalStateException("the delivery log did not answer");
          }
          asked.add(invocation.getArgument(3));
          return null;
        })
        .when(invoker)
        .reportTasksTheBpmsNoLongerHas(
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.any(),
            Mockito.any());
    return invoker;

  }

  private void theClusterTakesTheJobTimeoutUpdate() {

    final var command = mock(
        UpdateTimeoutJobCommandStep1.UpdateTimeoutJobCommandStep2.class,
        RETURNS_SELF);
    @SuppressWarnings("unchecked")
    final CamundaFuture<UpdateTimeoutJobResponse> answer = mock(CamundaFuture.class);
    Mockito.lenient().when(answer.join()).thenReturn(null);
    Mockito.lenient().when(command.send()).thenReturn(answer);
    theClusterAnswersTheJobTimeoutUpdateWith(command);

  }

  private void theClusterRefusesTheJobTimeoutUpdate(
      final RuntimeException rejection) {

    final var command = mock(
        UpdateTimeoutJobCommandStep1.UpdateTimeoutJobCommandStep2.class,
        RETURNS_SELF);
    Mockito.lenient().when(command.send()).thenThrow(rejection);
    theClusterAnswersTheJobTimeoutUpdateWith(command);

  }

  private void theClusterAnswersTheJobTimeoutUpdateWith(
      final UpdateTimeoutJobCommandStep1.UpdateTimeoutJobCommandStep2 command) {

    final var step1 = mock(UpdateTimeoutJobCommandStep1.class, RETURNS_SELF);
    Mockito.lenient().when(step1.timeout(Mockito.any(Duration.class))).thenReturn(command);
    Mockito.lenient().when(client.newUpdateTimeoutCommand(Mockito.anyLong())).thenReturn(step1);

  }

  private static ProblemException problem(
      final int status,
      final String title,
      final String reason) {

    final var details = new ProblemDetail();
    details.setStatus(status);
    details.setTitle(title);
    return new ProblemException(status, reason, details);

  }

}
