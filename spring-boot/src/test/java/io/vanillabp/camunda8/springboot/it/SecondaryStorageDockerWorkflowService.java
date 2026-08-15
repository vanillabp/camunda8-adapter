package io.vanillabp.camunda8.springboot.it;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * The workflow service of the secondary-storage integration test (story 52): the
 * workflow waits for a message, and correlating it needs the probe which locates the
 * BPMS holding the workflow.
 */
@Service
@WorkflowService(
    workflowAggregateClass = SecondaryStorageDockerAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "SecondaryStorageProcess"))
public class SecondaryStorageDockerWorkflowService {

  private final ProcessService<SecondaryStorageDockerAggregate> processService;

  private final SecondaryStorageDockerAggregateRepository repository;

  public SecondaryStorageDockerWorkflowService(
      final ProcessService<SecondaryStorageDockerAggregate> processService,
      final SecondaryStorageDockerAggregateRepository repository) {

    this.processService = processService;
    this.repository = repository;

  }

  public SecondaryStorageDockerAggregate startWorkflow() {

    return processService.startWorkflow(new SecondaryStorageDockerAggregate());

  }

  public void correlate(
      final Long aggregateId) {

    processService.correlateMessage(repository.findById(aggregateId).orElseThrow(), "C8SecondaryStorage");

  }

  public java.util.List<io.vanillabp.spi.process.ProcessDefinition> definitionsOf(
      final Long aggregateId) {

    return processService.getProcessDefinitions(repository.findById(aggregateId).orElseThrow(), null);

  }

  public io.vanillabp.spi.process.WorkflowHistory historyOf(
      final Long aggregateId) {

    return processService.getWorkflowHistory(repository.findById(aggregateId).orElseThrow(), null);

  }

  @WorkflowTask(taskDefinition = "secondaryStorageMessageArrived")
  public void messageArrived(
      final SecondaryStorageDockerAggregate aggregate) {

    aggregate.setProcessedBy("messageArrived");

  }

}
