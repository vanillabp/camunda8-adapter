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
 * be told that a workflow ended. The job is activated after the last
 * element of the process completed and gates the disappearance of the instance,
 * which is the window VanillaBP uses to call the application.
 * <p>
 * The cluster reports a COMPLETED end only: a cancelled instance is removed without
 * running end listeners, so this adapter cannot tell the application about
 * cancellations - and says so rather than faking a distinction.
 * <p>
 * A failing notification fails the job with one retry less, the way the handler of a
 * service task does, so the cluster retries and finally raises an incident. While the
 * workflow module is shutting down the job is left to its lock instead: a
 * notification cut off by a restart is not a defect of the application, and the cluster
 * hands the job out again with its retries intact. Both commands this handler sends back
 * are repeated where the cluster rejected them for backpressure, and a job which is failed
 * after all gets a <code>retry-backoff</code>.
 * <p>
 * Why the end of a workflow is reported through a listener the adapter injected is decision 5 in
 * the repository's DECISIONS.md; why this handler stays silent about a job during shutdown is
 * decision 6 in the repository's DECISIONS.md.
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

  /**
   * What the workflow module has in flight, and whether it is going down.
   * Never <code>null</code> - a handler built without one (tests) gets a drain of its own,
   * which never shuts down.
   */
  private final io.vanillabp.camunda8.client.Camunda8Drain drain;

  /**
   * What kind of worker this is, in the messages about a shutdown.
   */
  static final String KIND = "workflow-end listener";

  /**
   * How long the cluster waits before it hands a failed job out again. May be
   * <code>null</code> (tests) - then
   * {@link Camunda8RetryBackoffResolver#DEFAULT_RETRY_BACKOFF} applies.
   */
  private final Camunda8RetryBackoffResolver retryBackoffResolver;

  public Camunda8WorkflowEndedHandler(
      final String adapterId,
      final String workflowModuleId,
      final String bpmnProcessId,
      final String aggregateIdVariable,
      final WorkflowEndedInvoker workflowEndedInvoker) {

    this(adapterId, workflowModuleId, bpmnProcessId, aggregateIdVariable, workflowEndedInvoker, null);

  }

  public Camunda8WorkflowEndedHandler(
      final String adapterId,
      final String workflowModuleId,
      final String bpmnProcessId,
      final String aggregateIdVariable,
      final WorkflowEndedInvoker workflowEndedInvoker,
      final io.vanillabp.camunda8.client.Camunda8Drain drain) {

    this(adapterId, workflowModuleId, bpmnProcessId, aggregateIdVariable, workflowEndedInvoker, drain, null);

  }

  public Camunda8WorkflowEndedHandler(
      final String adapterId,
      final String workflowModuleId,
      final String bpmnProcessId,
      final String aggregateIdVariable,
      final WorkflowEndedInvoker workflowEndedInvoker,
      final io.vanillabp.camunda8.client.Camunda8Drain drain,
      final Camunda8RetryBackoffResolver retryBackoffResolver) {

    this.retryBackoffResolver = retryBackoffResolver;
    this.drain = drain == null
        ? new io.vanillabp.camunda8.client.Camunda8Drain(adapterId, workflowModuleId)
        : drain;
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

    drain.jobStarted(job.getKey(), KIND, job.getType(), bpmnProcessId);
    try {

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

      io.vanillabp.camunda8.client.Camunda8CommandRetry.send(
          adapterId,
          "completion",
          job.getKey(),
          job.getType(),
          job.getDeadline(),
          drain::isShuttingDown,
          () -> client
              .newCompleteCommand(job.getKey())
              .send()
              .join());

    } catch (final Exception e) {
      // While the module is going down, the failure is the shutdown and not the
      // application - the job keeps its lock and its retries
      if (drain.leaveJobToItsLock(job.getKey(), KIND, job.getType(), e)) {
        return;
      }
      // and otherwise the same treatment a service task gets
      final var retryBackoff = Camunda8RetryBackoffResolver
          .resolve(retryBackoffResolver, workflowModuleId, bpmnProcessId, null)
          .duration();
      log.warn(
          "Camunda8[{}]: reporting the end of the instance '{}' of BPMN process '{}' (job '{}') failed - "
              + "failing the job with {} retries left, to be handed out again in {}",
          adapterId,
          job.getProcessInstanceKey(),
          bpmnProcessId,
          job.getKey(),
          job.getRetries() - 1,
          retryBackoff,
          e);
      io.vanillabp.camunda8.client.Camunda8CommandRetry.send(
          adapterId,
          "failure",
          job.getKey(),
          job.getType(),
          job.getDeadline(),
          drain::isShuttingDown,
          () -> client
              .newFailCommand(job.getKey())
              .retries(job.getRetries() - 1)
              .retryBackoff(retryBackoff)
              .errorMessage(io.vanillabp.camunda8.client.Camunda8Errors.incidentMessage(e))
              .send()
              .join());
    } finally {
      drain.jobFinished(job.getKey());
    }

  }

  private WorkflowEndedContext contextOf(
      final ActivatedJob job,
      final String aggregateId) {

    return new WorkflowEndedContext() {

      @Override
      public String getAdapterId() {
        return adapterId;
      }

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
