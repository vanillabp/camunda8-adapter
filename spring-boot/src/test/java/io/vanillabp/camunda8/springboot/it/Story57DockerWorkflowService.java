package io.vanillabp.camunda8.springboot.it;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * The workflow service of the old-process-versions test (story 57): the task which
 * survives into version 2 is served for every version, the task which was dropped is
 * served for version 1 only - and whether the cluster still holds a version needing it
 * is what the startup check answers.
 */
@Service
@WorkflowService(
    workflowAggregateClass = Story57DockerAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "Story57Process"))
public class Story57DockerWorkflowService {

  private final ProcessService<Story57DockerAggregate> processService;

  public Story57DockerWorkflowService(
      final ProcessService<Story57DockerAggregate> processService) {

    this.processService = processService;

  }

  public Story57DockerAggregate startWorkflow() {

    return processService.startWorkflow(new Story57DockerAggregate());

  }

  @WorkflowTask(taskDefinition = "story57Kept")
  public void story57Kept(
      final Story57DockerAggregate aggregate) {

    aggregate.setServedBy("kept");

  }

  @WorkflowTask(taskDefinition = "story57Gone", version = "1")
  public void story57Gone(
      final Story57DockerAggregate aggregate) {

    aggregate.setServedBy("gone");

  }

}
