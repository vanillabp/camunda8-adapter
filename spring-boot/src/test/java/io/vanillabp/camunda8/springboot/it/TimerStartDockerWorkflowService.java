package io.vanillabp.camunda8.springboot.it;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowEnd;
import io.vanillabp.spi.service.WorkflowEnded;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * The workflow service of the timer-start integration test. It has NO
 * <code>@WorkflowStartedByBpms</code> method on purpose: the aggregate of a workflow
 * the cluster starts comes into existence without application code, and the task
 * following the start event has to find it through the aggregate-ID variable the
 * start listener wrote.
 */
@Service
@WorkflowService(
    workflowAggregateClass = TimerStartDockerAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "TimerStartProcess"))
public class TimerStartDockerWorkflowService {

  /**
   * The workflow reports its end as well.
   */
  @WorkflowEnded
  public void workflowEnded(
      final TimerStartDockerAggregate aggregate,
      final WorkflowEnd end) {

    aggregate.setEndedAs(String.valueOf(end.kind()));

  }

  @WorkflowTask(taskDefinition = "recordTimerStart")
  public void recordTimerStart(
      final TimerStartDockerAggregate aggregate) {

    aggregate.setProcessedBy("recordTimerStart");

  }

}
