package io.vanillabp.camunda8.springboot.processtest.it;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.TaskEvent;
import io.vanillabp.spi.service.TaskId;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * The workflow service of the review. The adapter delivers its user task through a user-task
 * listener, and the method at the end of the workflow through a listener the model declares.
 */
@Service
@WorkflowService(workflowAggregateClass = Review.class, bpmnProcess = @BpmnProcess(bpmnProcessId = "ReviewProcess"))
public class ReviewWorkflow {

  /**
   * Notes the id of the user task while it is open, so the test can complete it.
   *
   * @param review The review of the workflow
   * @param taskId The id of the user task
   * @param event Whether the task was created or ended
   */
  @WorkflowTask(taskDefinition = "reviewOrder")
  public void reviewOrder(
      final Review review,
      @TaskId final String taskId,
      @TaskEvent final TaskEvent.Event event) {

    review.setTaskId(event == TaskEvent.Event.CREATED
        ? taskId
        : null);

  }

  /**
   * The end listener of the end event. It marks the review as archived.
   *
   * @param review The review of the workflow
   */
  @WorkflowTask(taskDefinition = "archiveTheReview")
  public void archiveTheReview(
      final Review review) {

    review.setArchived(true);

  }

}
