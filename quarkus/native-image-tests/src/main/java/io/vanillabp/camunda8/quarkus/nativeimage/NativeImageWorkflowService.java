package io.vanillabp.camunda8.quarkus.nativeimage;

import java.util.concurrent.CountDownLatch;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * The workflow of the native-image application: one service task, which is enough to
 * need everything a native image lacks by default - the BPMN is parsed, modified,
 * serialized, deployed, and the job comes back from the cluster into a handler.
 */
@ApplicationScoped
@WorkflowService(
    workflowAggregateClass = NativeImageAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "NativeProcess"))
public class NativeImageWorkflowService {

  /**
   * Counted down by the handler, awaited by the application's main: a job which never
   * arrives must not end in a green run.
   */
  public static final CountDownLatch SERVED = new CountDownLatch(1);

  @Inject
  ProcessService<NativeImageAggregate> processService;

  public NativeImageAggregate startWorkflow() {

    return processService.startWorkflow(new NativeImageAggregate());

  }

  @WorkflowTask(taskDefinition = "nativeTask")
  public void nativeTask(
      final NativeImageAggregate aggregate) {

    aggregate.setStatus("served");
    SERVED.countDown();

  }

}
