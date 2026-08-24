package io.vanillabp.camunda8.springboot.it;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.TaskId;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * The workflow service of the aggregateChanged integration test: both
 * processes park in an asynchronous task, so the test can push into the workflow's
 * scope and into the scope of ONE instance of a multi-instance activity.
 */
@Service
@WorkflowService(
    workflowAggregateClass = PushDockerAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "AggregateChangedProcess"),
    secondaryBpmnProcesses = @BpmnProcess(bpmnProcessId = "AggregateChangedMultiInstanceProcess"))
public class PushDockerWorkflowService {

  private final ProcessService<PushDockerAggregate> processService;

  private final PushDockerAggregateRepository repository;

  public PushDockerWorkflowService(
      final ProcessService<PushDockerAggregate> processService,
      final PushDockerAggregateRepository repository) {

    this.processService = processService;
    this.repository = repository;

  }

  public PushDockerAggregate startWorkflow() {

    final var aggregate = new PushDockerAggregate();
    aggregate.setNote("before");
    return processService.startWorkflow(aggregate);

  }

  /**
   * Saves an aggregate without starting a workflow - the multi-instance process is
   * started against the cluster by the test (the injectable process service starts
   * the primary process only).
   *
   * @return The saved aggregate
   */
  public PushDockerAggregate saveAggregate() {

    final var aggregate = new PushDockerAggregate();
    aggregate.setNote("before");
    return repository.save(aggregate);

  }

  /**
   * Changes the aggregate and pushes it at the workflow's global scope.
   *
   * @param aggregateId The aggregate's id
   * @param note The new note
   */
  public void pushGlobally(
      final Long aggregateId,
      final String note) {

    final var aggregate = repository.findById(aggregateId).orElseThrow();
    aggregate.setNote(note);
    processService.aggregateChanged(aggregate);

  }

  /**
   * Changes the aggregate and pushes it into the scope of ONE task instance.
   *
   * @param aggregateId The aggregate's id
   * @param note The new note
   * @param taskId The task whose scope receives the values
   */
  public void pushInto(
      final Long aggregateId,
      final String note,
      final String taskId) {

    final var aggregate = repository.findById(aggregateId).orElseThrow();
    aggregate.setNote(note);
    processService.aggregateChanged(aggregate, taskId);

  }

  @WorkflowTask(taskDefinition = "awaitPush")
  public void awaitPush(
      final PushDockerAggregate aggregate,
      @TaskId final String taskId) {

    // parks the workflow: the instance stays in the cluster
    aggregate.setTaskIds(taskId);

  }

  @WorkflowTask(taskDefinition = "awaitPushPerInstance")
  public void awaitPushPerInstance(
      final PushDockerAggregate aggregate,
      @TaskId final String taskId) {

    aggregate
        .setTaskIds(
            aggregate.getTaskIds() == null
                ? taskId
                : aggregate.getTaskIds()
                    + ","
                    + taskId);

  }

}
