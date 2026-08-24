package io.vanillabp.camunda8.quarkus.test;

import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowEnd;
import io.vanillabp.spi.service.WorkflowEnded;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * The workflow service of the timer-started workflow. It has NO method
 * starting anything on purpose: the aggregate of a workflow the cluster starts comes
 * into existence without any application code, and the task following the start event
 * has to find it through the aggregate-id variable the start listener wrote.
 */
@ApplicationScoped
@WorkflowService(
    workflowAggregateClass = C8TimerAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "TimerStartProcess"))
public class C8TimerWorkflowService {

  /**
   * The workflow started by the timer also reports its end.
   *
   * @param aggregate The workflow aggregate
   * @param end How the workflow ended
   */
  @WorkflowEnded
  public void workflowEnded(
      final C8TimerAggregate aggregate,
      final WorkflowEnd end) {

    aggregate.setEndedAs(String.valueOf(end.kind()));

  }

  @WorkflowTask(taskDefinition = "recordTimerStart")
  public void recordTimerStart(
      final C8TimerAggregate aggregate) {

    aggregate.setProcessedBy("recordTimerStart");

  }

}
