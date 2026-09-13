package io.vanillabp.camunda8.springboot.listeners;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * The workflow service of the modelled-listener integration test: one method for the task and
 * one per listener, each named after the job type the model carries. Under
 * {@code name-clash-avoidance: use-prefix} the cluster knows those job types prefixed, and that
 * the methods below are reached at all is what proves the prefixing and its way back.
 */
@Service
@WorkflowService(
    workflowAggregateClass = ListenerDockerAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "ListenerProcess"))
public class ListenerDockerWorkflowService {

  @Autowired
  private ProcessService<ListenerDockerAggregate> processService;

  public ListenerDockerAggregate startWorkflow() {

    return processService.startWorkflow(new ListenerDockerAggregate());

  }

  @WorkflowTask(taskDefinition = "doTheWork")
  public void doTheWork(
      final ListenerDockerAggregate aggregate) {

    aggregate.setTheWorkWasDone(true);

  }

  /**
   * The <code>end</code> execution listener of the service task.
   */
  @WorkflowTask(taskDefinition = "auditTheWork")
  public void auditTheWork(
      final ListenerDockerAggregate aggregate) {

    aggregate.setTheWorkWasAudited(true);

  }

  /**
   * The <code>end</code> execution listener of the end event, which is the placement version 1
   * served on Camunda 7 and never on Camunda 8.
   */
  @WorkflowTask(taskDefinition = "archiveTheOrder")
  public void archiveTheOrder(
      final ListenerDockerAggregate aggregate) {

    aggregate.setTheOrderWasArchived(true);

  }

}
