package io.vanillabp.camunda8.springboot.it;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.TaskException;
import io.vanillabp.spi.service.TaskId;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * Workflow service of the task-processing integration test (story 21c): one
 * {@code @WorkflowTask} method per outcome variation, serving three BPMN processes
 * of one aggregate. Handlers record invocation counts so the test can prove
 * at-least-once redelivery convergence and async-task dormancy.
 */
@Service
@WorkflowService(
    workflowAggregateClass = TaskDockerAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "TaskProcess"),
    secondaryBpmnProcesses = {
        @BpmnProcess(bpmnProcessId = "FailProcess"), @BpmnProcess(bpmnProcessId = "AsyncProcess"), @BpmnProcess(
            bpmnProcessId = "RetryProcess"), @BpmnProcess(bpmnProcessId = "AsyncCancelProcess"), @BpmnProcess(
                bpmnProcessId = "UserTaskProcess"), @BpmnProcess(bpmnProcessId = "SilentUserTaskProcess"), @BpmnProcess(
                    bpmnProcessId = "MessageProcess"), @BpmnProcess(
                        bpmnProcessId = "MessageStartProcess"), @BpmnProcess(bpmnProcessId = "SyncProcess")
    })
public class TaskDockerWorkflowService {

  /**
   * Invocation counters per (task definition + aggregate ID) - inspected by the
   * integration test.
   */
  public static final Map<String, AtomicInteger> INVOCATIONS = new ConcurrentHashMap<>();

  private final ProcessService<TaskDockerAggregate> processService;

  public TaskDockerWorkflowService(
      final ProcessService<TaskDockerAggregate> processService) {

    this.processService = processService;

  }

  public TaskDockerAggregate startWorkflow(
      final TaskDockerAggregate aggregate) {

    return processService.startWorkflow(aggregate);

  }

  private static int countInvocation(
      final String taskDefinition,
      final TaskDockerAggregate aggregate) {

    return INVOCATIONS
        .computeIfAbsent(
            taskDefinition
                + ":"
                + aggregate.getId(),
            key -> new AtomicInteger())
        .incrementAndGet();

  }

  @WorkflowTask
  public void happyTask(
      final TaskDockerAggregate aggregate) {

    countInvocation("happyTask", aggregate);
    // idempotent: keyed on aggregate state, not on call count
    if ((aggregate.getResults() == null) || !aggregate.getResults().contains("happy")) {
      aggregate.appendResult("happy");
    }

  }

  @WorkflowTask
  public void retryTask(
      final TaskDockerAggregate aggregate) {

    // the FIRST delivery fails technically (local transaction rolled back, job
    // failed with decremented retries) - Camunda 8 REDELIVERS the same task and
    // the second delivery converges idempotently: at-least-once proven
    if (countInvocation("retryTask", aggregate) == 1) {
      throw new IllegalStateException("first delivery fails - forcing a redelivery");
    }
    if ((aggregate.getResults() == null) || !aggregate.getResults().contains("retried")) {
      aggregate.appendResult("retried");
    }

  }

  @WorkflowTask
  public void errorTask(
      final TaskDockerAggregate aggregate) {

    countInvocation("errorTask", aggregate);
    // the mutation has to be COMMITTED although the handler throws - the V1
    // TaskException contract (BPMN error, no rollback)
    aggregate.appendResult("error-raised");
    throw new TaskException("PaymentFailed", "PAYMENT_FAILED");

  }

  @WorkflowTask
  public void errorHandled(
      final TaskDockerAggregate aggregate) {

    countInvocation("errorHandled", aggregate);
    aggregate.appendResult("handled");

  }

  @WorkflowTask
  public void alwaysFails(
      final TaskDockerAggregate aggregate) {

    countInvocation("alwaysFails", aggregate);
    // must NEVER become visible: a technical exception rolls back the local
    // transaction; the job is failed with decremented retries
    aggregate.appendResult("must-never-be-visible");
    throw new IllegalStateException("boom-c8");

  }

  public TaskDockerAggregate completeAsyncTask(
      final TaskDockerAggregate aggregate,
      final String taskId) {

    return processService.completeTask(aggregate, taskId);

  }

  public TaskDockerAggregate cancelAsyncTask(
      final TaskDockerAggregate aggregate,
      final String taskId,
      final String bpmnErrorCode) {

    return processService.cancelTask(aggregate, taskId, bpmnErrorCode);

  }

  @WorkflowTask
  public void awaitCancelTask(
      final TaskDockerAggregate aggregate,
      @TaskId final String taskId) {

    countInvocation("awaitCancelTask", aggregate);
    aggregate.setTaskId(taskId);
    aggregate.appendResult("await-cancel");

  }

  @WorkflowTask
  public void cancelHandled(
      final TaskDockerAggregate aggregate) {

    countInvocation("cancelHandled", aggregate);
    aggregate.appendResult("cancel-handled");

  }

  public TaskDockerAggregate completeUserTask(
      final TaskDockerAggregate aggregate,
      final String taskId) {

    return processService.completeUserTask(aggregate, taskId);

  }

  public TaskDockerAggregate cancelUserTask(
      final TaskDockerAggregate aggregate,
      final String taskId,
      final String bpmnErrorCode) {

    return processService.cancelUserTask(aggregate, taskId, bpmnErrorCode);

  }

  @WorkflowTask(taskDefinition = "approveUser")
  public void approveUserNotification(
      final TaskDockerAggregate aggregate,
      @TaskId final String taskId,
      @io.vanillabp.spi.service.TaskEvent final io.vanillabp.spi.service.TaskEvent.Event event) {

    countInvocation("approveUser", aggregate);
    if (event == io.vanillabp.spi.service.TaskEvent.Event.CREATED) {
      aggregate.setTaskId(taskId);
      aggregate.appendResult("usertask-created");
    } else {
      aggregate.appendResult("usertask-"
          + event.name().toLowerCase());
    }

  }

  public TaskDockerAggregate correlate(
      final TaskDockerAggregate aggregate,
      final String messageName) {

    return processService.correlateMessage(aggregate, messageName);

  }

  public TaskDockerAggregate correlate(
      final TaskDockerAggregate aggregate,
      final String messageName,
      final String correlationId) {

    return processService.correlateMessage(aggregate, messageName, correlationId);

  }

  public TaskDockerAggregate startByMessage(
      final TaskDockerAggregate aggregate,
      final String messageName) {

    return processService.startWorkflowByMessage(aggregate, messageName);

  }

  @WorkflowTask
  public void c8MessageArrived(
      final TaskDockerAggregate aggregate) {

    countInvocation("c8MessageArrived", aggregate);
    aggregate.appendResult("message-arrived");

  }

  @WorkflowTask
  public void c8OrderPlaced(
      final TaskDockerAggregate aggregate) {

    countInvocation("c8OrderPlaced", aggregate);
    aggregate.appendResult("order-placed");

  }

  /**
   * The variables the cluster delivered to {@code syncApproved} - i.e. what the
   * completion of {@code syncTask} pushed (story 28b).
   */
  public static final Map<String, Object> OBSERVED_VARIABLES = new ConcurrentHashMap<>();

  @WorkflowTask
  public void syncTask(
      final TaskDockerAggregate aggregate) {

    countInvocation("syncTask", aggregate);
    // both changes happen AFTER the workflow was started, so the cluster can only
    // know them if the job completion pushes the aggregate state
    aggregate.setApproved(true);
    aggregate.setSecret("s3cr3t");
    aggregate.appendResult("sync-task");

  }

  @WorkflowTask
  public void syncApproved(
      final TaskDockerAggregate aggregate,
      @io.vanillabp.spi.service.TaskParam("approved") final Object approved,
      @io.vanillabp.spi.service.TaskParam("results") final Object results,
      @io.vanillabp.spi.service.TaskParam("secret") final Object secret) {

    countInvocation("syncApproved", aggregate);
    // @TaskParam reads the variables of the delivered job: the cluster's view
    OBSERVED_VARIABLES.put("approved", String.valueOf(approved));
    OBSERVED_VARIABLES.put("results", String.valueOf(results));
    OBSERVED_VARIABLES.put("secret", String.valueOf(secret));
    aggregate.appendResult("sync-approved");

  }

  @WorkflowTask
  public void syncRejected(
      final TaskDockerAggregate aggregate) {

    countInvocation("syncRejected", aggregate);
    // reached only if the gateway evaluated STALE data (the defect story 28b fixes)
    aggregate.appendResult("sync-rejected");

  }

  @WorkflowTask
  public void asyncTask(
      final TaskDockerAggregate aggregate,
      @TaskId final String taskId) {

    countInvocation("asyncTask", aggregate);
    aggregate.setTaskId(taskId);
    aggregate.appendResult("async-open");

  }

}
