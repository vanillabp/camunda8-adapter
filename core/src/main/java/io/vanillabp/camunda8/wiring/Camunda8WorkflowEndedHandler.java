package io.vanillabp.camunda8.wiring;

import java.time.Instant;

import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.worker.JobClient;
import io.camunda.client.api.worker.JobHandler;
import io.vanillabp.integration.adapter.spi.workflowend.WorkflowEndedContext;
import io.vanillabp.integration.adapter.spi.workflowend.WorkflowEndedInvoker;
import io.vanillabp.spi.service.WorkflowEnd;
import lombok.extern.slf4j.Slf4j;

/**
 * Consumes the END execution-listener jobs of a process whose application wants to
 * be told that a workflow ended (story 43). The job is activated after the last
 * element of the process completed and gates the disappearance of the instance,
 * which is the window VanillaBP uses to call the application.
 * <p>
 * The cluster reports a COMPLETED end only: a cancelled instance is removed without
 * running end listeners, so this adapter cannot tell the application about
 * cancellations - and says so rather than faking a distinction.
 */
@Slf4j
public class Camunda8WorkflowEndedHandler implements JobHandler {

  private final String adapterId;

  private final String workflowModuleId;

  /**
   * The PLAIN BPMN process id (what the application and the core know).
   */
  private final String bpmnProcessId;

  /**
   * The name of the variable carrying the workflow aggregate's ID.
   */
  private final String aggregateIdVariable;

  private final WorkflowEndedInvoker workflowEndedInvoker;

  public Camunda8WorkflowEndedHandler(
      final String adapterId,
      final String workflowModuleId,
      final String bpmnProcessId,
      final String aggregateIdVariable,
      final WorkflowEndedInvoker workflowEndedInvoker) {

    this.adapterId = adapterId;
    this.workflowModuleId = workflowModuleId;
    this.bpmnProcessId = bpmnProcessId;
    this.aggregateIdVariable = aggregateIdVariable;
    this.workflowEndedInvoker = workflowEndedInvoker;

  }

  @Override
  public void handle(
      final JobClient client,
      final ActivatedJob job) {

    final var aggregateId = job.getVariablesAsMap().get(aggregateIdVariable);
    if (aggregateId == null) {
      // not a VanillaBP workflow, or its aggregate-ID variable was removed: there
      // is nothing this end could be reported for
      log
          .debug(
              "Camunda8[{}]: the instance '{}' of '{}' carries no '{}' variable - its end is not reported",
              adapterId,
              job.getProcessInstanceKey(),
              bpmnProcessId,
              aggregateIdVariable);
    } else {
      workflowEndedInvoker
          .workflowEnded(
              workflowModuleId,
              bpmnProcessId,
              contextOf(job, String.valueOf(aggregateId)));
    }

    client
        .newCompleteCommand(job.getKey())
        .send()
        .join();

  }

  private WorkflowEndedContext contextOf(
      final ActivatedJob job,
      final String aggregateId) {

    return new WorkflowEndedContext() {

      @Override
      public String getWorkflowAggregateId() {
        return aggregateId;
      }

      @Override
      public WorkflowEnd.Kind getKind() {
        // the cluster runs end listeners of completed instances only
        return WorkflowEnd.Kind.COMPLETED;
      }

      @Override
      public Instant getEndTime() {
        return Instant.now();
      }

      @Override
      public String getEndEventId() {
        // the listener sits on the PROCESS, so the cluster reports the process as
        // the element - which end event was reached is not part of the job
        return null;
      }

      @Override
      public String getProcessVersion() {
        return String.valueOf(job.getProcessDefinitionVersion());
      }

    };

  }

}
