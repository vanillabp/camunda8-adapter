package io.vanillabp.camunda8.wiring;


import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.worker.JobClient;
import io.camunda.client.api.worker.JobHandler;
import io.vanillabp.integration.adapter.spi.workflowtask.TaskInvocationContext;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskInvoker;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskOutcome;
import io.vanillabp.spi.service.TaskEvent;
import lombok.extern.slf4j.Slf4j;

/**
 * Consumes USER-task lifecycle listener jobs (story 24): the V1-compatible
 * <code>zeebe:taskListener</code>s added at deployment deliver <code>creating</code>
 * (→ {@link TaskEvent.Event#CREATED}) and <code>canceling</code>
 * (→ {@link TaskEvent.Event#CANCELED}) as NORMAL JOBS consumed by this handler.
 * The notified <code>&#64;WorkflowTask</code> method is OPTIONAL (a user task
 * without one is simply processed through forms/task lists) and never completes
 * the user task on return - completion arrives via
 * <code>ProcessService#completeUserTask</code> with the USER-TASK KEY reported as
 * <code>&#64;TaskId</code>.
 * <p>
 * Listener jobs GATE the task lifecycle and are therefore ALWAYS completed -
 * including deliveries without a handler. A failing notification fails the
 * listener job; with the V1-compatible <code>retries="0"</code> this raises an
 * incident for the operator (notification defects must not be silently lost).
 */
@Slf4j
public class Camunda8UserTaskListenerHandler implements JobHandler {

  private final String adapterId;

  private final String workflowModuleId;

  private final WorkflowTaskInvoker workflowTaskInvoker;

  public Camunda8UserTaskListenerHandler(
      final String adapterId,
      final String workflowModuleId,
      final WorkflowTaskInvoker workflowTaskInvoker) {

    this.adapterId = adapterId;
    this.workflowModuleId = workflowModuleId;
    this.workflowTaskInvoker = workflowTaskInvoker;

  }

  @Override
  public void handle(
      final JobClient client,
      final ActivatedJob job) {

    final var bpmnProcessId = job.getBpmnProcessId();
    final var event = job.getListenerEventType() == io.camunda.client.api.search.enums.ListenerEventType.CANCELING
        ? TaskEvent.Event.CANCELED
        : TaskEvent.Event.CREATED;
    // the USER-TASK KEY is the @TaskId - it completes the task later via
    // ProcessService#completeUserTask (V1-compatible decimal representation)
    final var userTaskKey = job.getUserTask() != null
        ? String.valueOf(job.getUserTask().getUserTaskKey())
        : String.valueOf(job.getKey());
    final var taskDefinition = job
        .getType()
        .substring(Camunda8TaskWiring.TASKDEFINITION_USERTASK_ZEEBE.length());

    try {
      if (workflowTaskInvoker.workflowTaskHandlerExists(workflowModuleId, bpmnProcessId, taskDefinition)) {
        final var aggregateIdName = workflowTaskInvoker.resolveWorkflowAggregateIdName(
            workflowModuleId, bpmnProcessId);
        final var aggregateId = job.getVariablesAsMap().get(aggregateIdName);
        if (aggregateId == null) {
          throw new IllegalStateException(
              ("The user-task listener job '%s' (type '%s') of BPMN process '%s' carries no "
                  + "variable '%s' holding the workflow aggregate's ID! Workflows processed by "
                  + "VanillaBP have to be started through VanillaBP.")
                  .formatted(job.getKey(), job.getType(), bpmnProcessId, aggregateIdName));
        }
        final var outcome = workflowTaskInvoker.invokeWorkflowTask(
            workflowModuleId,
            bpmnProcessId,
            new Camunda8UserTaskInvocationContext(
                taskDefinition, String.valueOf(aggregateId), userTaskKey, event, job));
        if (outcome.kind() == WorkflowTaskOutcome.Kind.BPMN_ERROR) {
          throw new IllegalStateException(
              ("The @WorkflowTask method notified about the %s event of user task '%s' (BPMN "
                  + "process '%s' of workflow module '%s') threw a TaskException! User-task "
                  + "notification handlers must not raise BPMN errors - route errors via "
                  + "ProcessService#cancelUserTask instead.")
                  .formatted(event, taskDefinition, bpmnProcessId, workflowModuleId));
        }
      } else {
        log.trace(
            "Camunda8[{}]: no @WorkflowTask handler for user task '{}' of BPMN process '{}' - "
                + "completing the {} listener job without a notification",
            adapterId,
            taskDefinition,
            bpmnProcessId,
            event);
      }

      // listener jobs gate the task lifecycle - ALWAYS complete them; the
      // user-task result keeps the lifecycle moving (denying is not VanillaBP's
      // business)
      client
          .newCompleteCommand(job.getKey())
          .send()
          .join();
    } catch (final Exception e) {
      log.warn(
          "Camunda8[{}]: processing user-task listener job '{}' (type '{}', event {}) failed - "
              + "failing the job (retries are 0: an incident is raised for the operator)",
          adapterId,
          job.getKey(),
          job.getType(),
          event,
          e);
      client
          .newFailCommand(job.getKey())
          .retries(0)
          .errorMessage(String.valueOf(e.getMessage()))
          .send()
          .join();
    }

  }

  /**
   * The neutral invocation context built from a user-task lifecycle listener job.
   */
  static class Camunda8UserTaskInvocationContext implements TaskInvocationContext {

    private final String taskDefinition;

    private final String workflowAggregateId;

    private final String userTaskKey;

    private final TaskEvent.Event event;

    private final ActivatedJob job;

    Camunda8UserTaskInvocationContext(
        final String taskDefinition,
        final String workflowAggregateId,
        final String userTaskKey,
        final TaskEvent.Event event,
        final ActivatedJob job) {

      this.taskDefinition = taskDefinition;
      this.workflowAggregateId = workflowAggregateId;
      this.userTaskKey = userTaskKey;
      this.event = event;
      this.job = job;

    }

    @Override
    public String getTaskDefinition() {

      return taskDefinition;

    }

    @Override
    public String getWorkflowAggregateId() {

      return workflowAggregateId;

    }

    @Override
    public String getTaskId() {

      return userTaskKey;

    }

    @Override
    public TaskEvent.Event getTaskEvent() {

      return event;

    }

    @Override
    public Object getTaskParameter(
        final String name) {

      return job.getVariablesAsMap().get(name);

    }

  }

}
