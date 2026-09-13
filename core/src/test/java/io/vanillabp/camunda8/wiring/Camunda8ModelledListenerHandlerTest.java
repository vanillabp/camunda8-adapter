package io.vanillabp.camunda8.wiring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.worker.JobClient;
import io.vanillabp.camunda8.TestScoping;
import io.vanillabp.camunda8.client.Camunda8Drain;
import io.vanillabp.integration.adapter.spi.NameClashAvoidance;
import io.vanillabp.integration.adapter.spi.workflowtask.TaskInvocationContext;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskInvoker;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskOutcome;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;
import io.vanillabp.spi.service.TaskEvent;

/**
 * What the job of a listener somebody modelled tells the application's method, and what it
 * sends back to the cluster.
 * <p>
 * The two things worth pinning are the ones version 1 got wrong. The completion carries no
 * variables, because the cluster discards them for a listener, and the event the method sees
 * is the only value which reaches a method at all: a method without a
 * <code>@TaskEvent</code> parameter subscribes to CREATED alone.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8ModelledListenerHandlerTest {

  private final JobClient jobClient = mock(JobClient.class, RETURNS_DEEP_STUBS);

  private final Camunda8Drain drain = new Camunda8Drain("c8", "test-module");

  private static ActivatedJob listenerJob(
      final String jobType) {

    final var job = mock(ActivatedJob.class);
    when(job.getKey()).thenReturn(4711L);
    when(job.getElementInstanceKey()).thenReturn(100L);
    when(job.getRetries()).thenReturn(3);
    when(job.getProcessDefinitionVersion()).thenReturn(7);
    when(job.getBpmnProcessId()).thenReturn("TestProcess");
    when(job.getElementId()).thenReturn("Event_Done");
    when(job.getType()).thenReturn(jobType);
    when(job.getVariablesAsMap()).thenReturn(Map.of("id", "42", "amount", 120));
    return job;

  }

  private TaskInvocationContext deliver(
      final ActivatedJob job,
      final NameClashAvoidance mode,
      final WorkflowTaskOutcome outcome) {

    final var invoker = mock(WorkflowTaskInvoker.class);
    when(invoker.resolveWorkflowAggregateIdName(anyString(), anyString())).thenReturn("id");
    when(invoker.invokeWorkflowTask(anyString(), anyString(), any())).thenReturn(outcome);
    Camunda8ModelledListenerHandler
        .builder()
        .adapterId("c8")
        .workflowModuleId("test-module")
        .workflowTaskInvoker(invoker)
        .scoping(TestScoping.of(mode))
        .drain(drain)
        .build()
        .handle(jobClient, job);
    final var context = ArgumentCaptor.forClass(TaskInvocationContext.class);
    Mockito
        .verify(invoker)
        .invokeWorkflowTask(anyString(), anyString(), context.capture());
    return context.getValue();

  }

  @Test
  @DisplayName("The job type is the task definition, and the method is told CREATED")
  public void theJobTypeIsTheTaskDefinition() {

    final var context = deliver(
        listenerJob("archiveTheOrder"),
        NameClashAvoidance.NONE,
        WorkflowTaskOutcome.completed());

    assertEquals("archiveTheOrder", context.getTaskDefinition(), "what a @WorkflowTask method names");
    assertEquals(
        TaskEvent.Event.CREATED,
        context.getTaskEvent(),
        "the only value a method without a @TaskEvent parameter is called for, and the event itself "
            + "is in the wiring: one method serves one event of one element");
    assertEquals("42", context.getWorkflowAggregateId());
    assertEquals("7", context.getProcessVersion(), "the cluster ships it with the job");
    assertEquals("4711", context.getDeliveryId(), "one listener event is one job");
    assertEquals("100", context.getActivationId(), "two listeners of one element share the activation");
    assertEquals(120, context.getTaskParameter("amount"));

  }

  @Test
  @DisplayName("Under use-prefix the method is told the task definition it wrote, not the scoped one")
  public void underPrefixingTheMethodSeesThePlainTaskDefinition() {

    final var context = deliver(
        listenerJob("test-module__TestProcess__archiveTheOrder"),
        NameClashAvoidance.USE_PREFIX,
        WorkflowTaskOutcome.completed());

    assertEquals(
        "archiveTheOrder",
        context.getTaskDefinition(),
        "the core is keyed by the plain names, whatever the cluster knows");

  }

  @Test
  @DisplayName("The completion carries no variables, because the cluster would discard them")
  public void theCompletionCarriesNoVariables() {

    deliver(listenerJob("archiveTheOrder"), NameClashAvoidance.NONE, WorkflowTaskOutcome.completed());

    verify(jobClient).newCompleteCommand(4711L);
    verify(jobClient.newCompleteCommand(4711L), never()).variables(any(Map.class));

  }

  @Test
  @DisplayName("A listener method raising a BPMN error fails the job and says why it cannot")
  public void aBpmnErrorFailsTheJob() {

    final var invoker = mock(WorkflowTaskInvoker.class);
    when(invoker.resolveWorkflowAggregateIdName(anyString(), anyString())).thenReturn("id");
    when(invoker.invokeWorkflowTask(anyString(), anyString(), any()))
        .thenReturn(WorkflowTaskOutcome.bpmnError("SOME_ERROR", null));

    Camunda8ModelledListenerHandler
        .builder()
        .adapterId("c8")
        .workflowModuleId("test-module")
        .workflowTaskInvoker(invoker)
        .drain(drain)
        .build()
        .handle(jobClient, listenerJob("archiveTheOrder"));

    final var message = ArgumentCaptor.forClass(String.class);
    verify(jobClient.newFailCommand(4711L).retries(2).retryBackoff(any(java.time.Duration.class)))
        .errorMessage(message.capture());
    assertTrue(
        message.getValue().contains("cannot raise a BPMN error"),
        () -> "an incident naming the cause beats one naming a transition the cluster was in: "
            + message.getValue());

  }

  @Test
  @DisplayName("A job without the aggregate's variable says which key would fetch it")
  public void aJobWithoutTheAggregateIdSaysWhatToConfigure() {

    final var job = listenerJob("archiveTheOrder");
    when(job.getVariablesAsMap()).thenReturn(Map.of());
    final var invoker = mock(WorkflowTaskInvoker.class);
    when(invoker.resolveWorkflowAggregateIdName(anyString(), anyString())).thenReturn("id");

    Camunda8ModelledListenerHandler
        .builder()
        .adapterId("c8")
        .workflowModuleId("test-module")
        .workflowTaskInvoker(invoker)
        .drain(drain)
        .build()
        .handle(jobClient, job);

    verify(invoker, never()).invokeWorkflowTask(anyString(), anyString(), any());
    final var message = ArgumentCaptor.forClass(String.class);
    verify(jobClient.newFailCommand(4711L).retries(2).retryBackoff(any(java.time.Duration.class)))
        .errorMessage(message.capture());
    assertTrue(
        message.getValue().contains("The listener job"),
        () -> "the message names what kind of job it was: "
            + message.getValue());

  }

}
