package io.vanillabp.camunda8;

import java.util.LinkedHashMap;
import java.util.Map;

import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import lombok.Getter;

/**
 * Adapter-specific processing context accumulated across all BPMN files of a workflow
 * module during the deployment pipeline
 * ({@code readBpmn} &rarr; {@code prepareBpmn} &rarr; {@code wireBpmn} &rarr;
 * {@code deployResources} &rarr; {@code startWorkflowProcessing}).
 * <p>
 * It collects the deployable BPMN resources (keyed by filename, so a file containing
 * several executable processes is deployed only once) together with the discovered
 * executable BPMN process IDs. {@link Camunda8DeploymentService#deployResources} sends all
 * collected resources of the module to Camunda 8 in a single deployment.
 */
public class Camunda8ProcessingContext {

  @Getter
  private final String workflowModuleId;

  /**
   * Deployable BPMN resources of the workflow module, keyed by filename. A
   * {@link LinkedHashMap} keeps the deployment order stable and deduplicates files that
   * contain multiple executable processes.
   */
  private final Map<String, BpmnModelInstance> resources = new LinkedHashMap<>();

  /**
   * The job-worker tasks of all executable processes of the module, collected
   * during wireBpmn - startWorkflowProcessing opens one worker per distinct task
   * definition.
   */
  @Getter
  private final java.util.List<io.vanillabp.camunda8.wiring.Camunda8TaskWiring.Camunda8TaskToWire> tasksToWire = new java.util.LinkedList<>();

  /**
   * The Camunda-managed user tasks collected during {@code wireBpmn} -
   * one listener-job worker is opened per distinct listener job type.
   */
  @Getter
  private final java.util.List<io.vanillabp.camunda8.wiring.Camunda8TaskWiring.Camunda8UserTaskToWire> userTasksToWire = new java.util.LinkedList<>();

  /**
   * The start events the cluster fires on its own, collected while wiring
   * and served by one worker each once workflow processing starts.
   */
  @Getter
  private final java.util.List<io.vanillabp.camunda8.wiring.Camunda8TaskWiring.Camunda8BpmsInitiatedStartToWire> bpmsInitiatedStartsToWire = new java.util.LinkedList<>();

  /**
   * The BPMN processes whose end has to be reported to the application,
   * as (scoped process id) - one worker each once workflow processing starts.
   */
  @Getter
  private final java.util.List<String> workflowEndedProcessesToWire = new java.util.LinkedList<>();

  /**
   * The workers opened by startWorkflowProcessing, closed by
   * stopWorkflowProcessing (reverse order).
   */
  @Getter
  private final java.util.List<io.camunda.client.api.worker.JobWorker> openWorkers = new java.util.LinkedList<>();

  public Camunda8ProcessingContext(
      final String workflowModuleId) {

    this.workflowModuleId = workflowModuleId;

  }

  /**
   * Adds a deployable BPMN resource. Idempotent per filename: multiple executable
   * processes of the same file register the same model only once.
   *
   * @param filename The BPMN filename (used as the deployment resource name)
   * @param model The parsed BPMN model
   */
  /**
   * The PLAIN BPMN process ids of the module's executable processes, collected in
   * {@code prepareBpmn} - the input of the collision check (two processes must not
   * end up under the same prefixed identifier, see decision 2 in the repository's
   * README.md).
   */
  @Getter
  private final java.util.List<String> deployedProcessIds = new java.util.LinkedList<>();

  /**
   * Records an executable BPMN process of this workflow module.
   *
   * @param bpmnProcessId The plain BPMN process ID
   */
  public void recordDeployedProcess(
      final String bpmnProcessId) {

    if ((bpmnProcessId != null) && !deployedProcessIds.contains(bpmnProcessId)) {
      deployedProcessIds.add(bpmnProcessId);
    }

  }

  public void addResource(
      final String filename,
      final BpmnModelInstance model) {

    resources.putIfAbsent(filename, model);

  }

  public Map<String, BpmnModelInstance> getResources() {

    return resources;

  }

  public boolean isEmpty() {

    return resources.isEmpty();

  }

}
