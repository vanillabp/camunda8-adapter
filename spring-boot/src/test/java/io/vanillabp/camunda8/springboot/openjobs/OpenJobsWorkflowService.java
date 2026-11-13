package io.vanillabp.camunda8.springboot.openjobs;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * Serves the one service task of the process of {@link Camunda8OpenJobsWithoutARecordIT}.
 */
@Service
@WorkflowService(
    workflowAggregateClass = OpenJobsAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "OpenJobsProcess"))
public class OpenJobsWorkflowService {

  /**
   * The aggregate ids the method was called with, across every boot of the test. A static
   * list, because each boot builds a new application context.
   */
  public static final List<String> SERVED_AGGREGATE_IDS = new CopyOnWriteArrayList<>();

  private final ProcessService<OpenJobsAggregate> processService;

  public OpenJobsWorkflowService(
      final ProcessService<OpenJobsAggregate> processService) {

    this.processService = processService;

  }

  public OpenJobsAggregate startWorkflow(
      final OpenJobsAggregate aggregate) {

    return processService.startWorkflow(aggregate);

  }

  @WorkflowTask(taskDefinition = "doTheWork")
  public void doTheWork(
      final OpenJobsAggregate aggregate) {

    SERVED_AGGREGATE_IDS.add(aggregate.getId());

  }

}
