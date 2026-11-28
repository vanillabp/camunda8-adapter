package io.vanillabp.camunda8.springboot.processtest.it;

import org.springframework.stereotype.Service;

import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * The workflow service. Its one task marks the order as processed.
 */
@Service
@WorkflowService(workflowAggregateClass = Order.class, bpmnProcess = @BpmnProcess(bpmnProcessId = "DemoProcess"))
public class OrderWorkflow {

  /**
   * Marks the order as processed.
   *
   * @param order The order of the workflow
   */
  @WorkflowTask(taskDefinition = "processOrder")
  public void processOrder(
      final Order order) {

    order.setStatus("PROCESSED");

  }

}
