package io.vanillabp.camunda8.springboot.legacyusertask;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.TaskId;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * Claims the process of {@link Camunda8LegacyUserTaskIT} and serves its user task the usual
 * way: a {@code @WorkflowTask} method named after the task definition.
 */
@Service
@WorkflowService(
    workflowAggregateClass = LegacyUserTaskAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "LegacyUserTaskProcess"))
public class LegacyUserTaskWorkflowService {

  @WorkflowTask(taskDefinition = "manualMatching")
  public void manualMatching(
      final LegacyUserTaskAggregate aggregate,
      @TaskId final String taskId) {

    // the test only reads what the boot says. Nobody works on the task.

  }

}
