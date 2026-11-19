package io.vanillabp.camunda8.springboot.legacyusertask;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.TaskEvent;
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

  /**
   * The task ids this method was called with for a created task, across every boot of the
   * test. A static list, because each boot builds a new application context.
   */
  public static final List<String> DELIVERED_TASK_IDS = new CopyOnWriteArrayList<>();

  /**
   * The task ids this method was called with for a canceled task, across every boot of the
   * test.
   */
  public static final List<String> CANCELED_TASK_IDS = new CopyOnWriteArrayList<>();

  @WorkflowTask(taskDefinition = "manualMatching")
  public void manualMatching(
      final LegacyUserTaskAggregate aggregate,
      @TaskId final String taskId,
      @TaskEvent final TaskEvent.Event event) {

    // the test only notes the call. Nobody works on the task
    if (event == TaskEvent.Event.CANCELED) {
      CANCELED_TASK_IDS.add(taskId);
    } else {
      DELIVERED_TASK_IDS.add(taskId);
    }

  }

}
