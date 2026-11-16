package io.vanillabp.camunda8.springboot.foreigncluster;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * Serves the one service task of the process of {@link Camunda8ForeignClusterAtStartupIT}. Its
 * delivery leaves the record which a new cluster could meet.
 */
@Service
@WorkflowService(
    workflowAggregateClass = ForeignClusterAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "ForeignClusterProcess"))
public class ForeignClusterWorkflowService {

  /**
   * The aggregate ids the method was called with, across every boot of the test. A static
   * list, because each boot builds a new application context.
   */
  public static final List<String> SERVED_AGGREGATE_IDS = new CopyOnWriteArrayList<>();

  private final ProcessService<ForeignClusterAggregate> processService;

  public ForeignClusterWorkflowService(
      final ProcessService<ForeignClusterAggregate> processService) {

    this.processService = processService;

  }

  public ForeignClusterAggregate startWorkflow(
      final ForeignClusterAggregate aggregate) {

    return processService.startWorkflow(aggregate);

  }

  @WorkflowTask(taskDefinition = "recordSomething")
  public void recordSomething(
      final ForeignClusterAggregate aggregate) {

    SERVED_AGGREGATE_IDS.add(aggregate.getId());

  }

}
