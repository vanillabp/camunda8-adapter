package io.vanillabp.camunda8.springboot.it;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowService;

/**
 * Workflow service bound to the BPMN process {@code TestProcess} deployed to the Camunda 8
 * cluster. It only exposes the {@link ProcessService} used by the test to start workflows;
 * the BPMN service task is handled by a raw Camunda 8 job worker in the test (adapter task
 * wiring is a later story).
 */
@Service
@WorkflowService(
    bpmnProcess = @BpmnProcess(bpmnProcessId = "TestProcess"),
    workflowAggregateClass = DockerAggregate.class)
public class DockerWorkflowService {

  private final ProcessService<DockerAggregate> processService;

  public DockerWorkflowService(
      final ProcessService<DockerAggregate> processService) {

    this.processService = processService;

  }

  public DockerAggregate startWorkflow(
      final DockerAggregate aggregate) {

    return processService.startWorkflow(aggregate);

  }

}
