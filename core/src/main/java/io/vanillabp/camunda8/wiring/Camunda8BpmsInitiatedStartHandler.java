package io.vanillabp.camunda8.wiring;

import java.time.Instant;
import java.util.Map;

import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.worker.JobClient;
import io.camunda.client.api.worker.JobHandler;
import io.vanillabp.integration.adapter.spi.AggregateSyncMode;
import io.vanillabp.integration.adapter.spi.workflowstart.BpmsInitiatedStartContext;
import io.vanillabp.integration.adapter.spi.workflowstart.BpmsInitiatedStartInvoker;
import io.vanillabp.spi.service.BpmsStartTrigger;
import lombok.extern.slf4j.Slf4j;

/**
 * Consumes the START execution-listener jobs of start events the cluster fires on
 * its own (story 41). The listener gates the workflow: nothing of the process runs
 * before this job is completed, which is exactly the window VanillaBP needs to build
 * the workflow aggregate and to write its ID into the instance.
 * <p>
 * The job is completed with the aggregate's ID (named after the aggregate's ID
 * attribute - how this adapter addresses workflows) plus the values shared per
 * {@code @SyncWithBPMS}. A failing build fails the job, so the cluster retries and
 * finally raises an incident: a workflow without an aggregate could never be
 * processed.
 */
@Slf4j
public class Camunda8BpmsInitiatedStartHandler implements JobHandler {

  private final String adapterId;

  private final String workflowModuleId;

  /**
   * The PLAIN BPMN process id (what the application and the core know).
   */
  private final String bpmnProcessId;

  private final String startEventId;

  private final BpmsStartTrigger.Kind kind;

  private final String signalName;

  private final BpmsInitiatedStartInvoker bpmsInitiatedStartInvoker;

  public Camunda8BpmsInitiatedStartHandler(
      final String adapterId,
      final String workflowModuleId,
      final String bpmnProcessId,
      final String startEventId,
      final BpmsStartTrigger.Kind kind,
      final String signalName,
      final BpmsInitiatedStartInvoker bpmsInitiatedStartInvoker) {

    this.adapterId = adapterId;
    this.workflowModuleId = workflowModuleId;
    this.bpmnProcessId = bpmnProcessId;
    this.startEventId = startEventId;
    this.kind = kind;
    this.signalName = signalName;
    this.bpmsInitiatedStartInvoker = bpmsInitiatedStartInvoker;

  }

  @Override
  public void handle(
      final JobClient client,
      final ActivatedJob job) {

    final var result = bpmsInitiatedStartInvoker
        .startWorkflowByBpms(workflowModuleId, bpmnProcessId, contextOf(job));

    log
        .debug(
            "Camunda8[{}]: the cluster started '{}' of workflow module '{}' by start event '{}' - "
                + "workflow aggregate '{}' {}",
            adapterId,
            bpmnProcessId,
            workflowModuleId,
            startEventId,
            result.workflowAggregateId(),
            result.created()
                ? "created"
                : "existed already");

    client
        .newCompleteCommand(job.getKey())
        .variables(result.variables())
        .send()
        .join();

  }

  private BpmsInitiatedStartContext contextOf(
      final ActivatedJob job) {

    // whatever the model set before the start event completed - an input mapping of
    // the start event, or the payload a broadcast signal carried
    final Map<String, Object> variables = Map.copyOf(job.getVariablesAsMap());

    return new BpmsInitiatedStartContext() {

      @Override
      public String getStartEventId() {
        return startEventId;
      }

      @Override
      public BpmsStartTrigger.Kind getKind() {
        return kind;
      }

      @Override
      public Instant getTriggerTime() {
        // the cluster does not report a timer's scheduled time to the listener job,
        // so this is the moment the job is processed - it is not what identifies the
        // start here, see getNaturalIdentity
        return Instant.now();
      }

      @Override
      public String getNaturalIdentity() {
        // the process instance exists before this job is activated and its key
        // survives every retry of the job: deriving the aggregate's ID from it is
        // what keeps a redelivered listener job from building a second aggregate
        // after the first attempt failed on its way back to the cluster
        return String.valueOf(job.getProcessInstanceKey());
      }

      @Override
      public String getSignalName() {
        return signalName;
      }

      @Override
      public Map<String, Object> getVariables() {
        return variables;
      }

      @Override
      public String getNativeInstanceId() {
        return String.valueOf(job.getProcessInstanceKey());
      }

      @Override
      public String getProcessVersion() {
        return String.valueOf(job.getProcessDefinitionVersion());
      }

      @Override
      public AggregateSyncMode getAggregateSyncMode() {
        // a remote BPMS holds the values it evaluates: what the aggregate shares
        // has to travel with the completion of this job
        return AggregateSyncMode.FULL;
      }

    };

  }

}
