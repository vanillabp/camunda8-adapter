package io.vanillabp.camunda8;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.camunda.client.api.worker.JobWorker;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.vanillabp.camunda8.wiring.Camunda8TaskWiring;
import lombok.Getter;

/**
 * Adapter-specific processing context accumulated across all BPMN files of a workflow
 * module during the deployment pipeline
 * ({@code readBpmn} &rarr; {@code prepareBpmn} &rarr; {@code wireBpmn} &rarr;
 * {@code deployResources} &rarr; {@code startWorkflowProcessing}).
 * <p>
 * It collects the deployable BPMN resources (keyed by filename, so a file containing
 * several executable processes is deployed only once) together with the discovered
 * executable BPMN process IDs. {@code Camunda8DeploymentService#deployResources} sends all
 * collected resources of the module to Camunda 8 in a single deployment.
 */
public class Camunda8ProcessingContext {

  private final String workflowModuleId;

  /**
   * Deployable BPMN resources of the workflow module, keyed by filename. A
   * {@link LinkedHashMap} keeps the deployment order stable and deduplicates files that
   * contain multiple executable processes.
   */
  @Getter
  private final Map<String, BpmnModelInstance> resources = new LinkedHashMap<>();

  /**
   * The decision tables of the workflow module, keyed by filename - deployed with its
   * processes in the same command. Bytes rather than a model: nothing here has to
   * understand a decision, and the one thing which is rewritten, the decision id under
   * prefix scoping, was rewritten while the file was read.
   */
  @Getter
  private final Map<String, byte[]> decisions = new LinkedHashMap<>();

  /**
   * Remembers a decision table for deployment.
   *
   * @param filename The DMN file name - it keeps its extension, which is how the cluster
   *          tells a decision from a process
   * @param dmn The file
   */
  public void addDecision(
      final String filename,
      final byte[] dmn) {

    decisions.putIfAbsent(filename, dmn);

  }

  /**
   * The job-worker tasks of all executable processes of the module, collected
   * during wireBpmn - startWorkflowProcessing opens one worker per distinct task
   * definition.
   */
  @Getter
  private final List<Camunda8TaskWiring.Camunda8TaskToWire> tasksToWire = new LinkedList<>();

  /**
   * The Camunda-managed user tasks collected during {@code wireBpmn} -
   * one listener-job worker is opened per distinct listener job type.
   */
  @Getter
  private final List<Camunda8TaskWiring.Camunda8UserTaskToWire> userTasksToWire = new LinkedList<>();

  /**
   * The start events the cluster fires on its own, collected while wiring
   * and served by one worker each once workflow processing starts.
   */
  @Getter
  private final List<Camunda8TaskWiring.Camunda8BpmsInitiatedStartToWire> bpmsInitiatedStartsToWire = new LinkedList<>();

  /**
   * The BPMN processes whose end has to be reported to the application,
   * as (scoped process id) - one worker each once workflow processing starts.
   */
  @Getter
  private final List<String> workflowEndedProcessesToWire = new LinkedList<>();

  /**
   * The workers opened by startWorkflowProcessing, closed by
   * stopWorkflowProcessing (reverse order).
   */
  @Getter
  private final List<JobWorker> openWorkers = new LinkedList<>();

  public Camunda8ProcessingContext(
      final String workflowModuleId) {

    this.workflowModuleId = workflowModuleId;

  }

  /**
   * The PLAIN BPMN process ids of the module's executable processes, collected in
   * {@code prepareBpmn} - the input of the collision check (two processes must not
   * end up under the same prefixed identifier, see decision 2 in the repository's
   * DECISIONS.md).
   */
  @Getter
  private final List<String> deployedProcessIds = new LinkedList<>();

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

  /**
   * Adds a deployable BPMN resource. Idempotent per filename: multiple executable
   * processes of the same file register the same model only once.
   *
   * @param filename The BPMN filename (used as the deployment resource name)
   * @param model The parsed BPMN model
   */
  public void addResource(
      final String filename,
      final BpmnModelInstance model) {

    resources.putIfAbsent(filename, model);

  }

  public boolean isEmpty() {

    return resources.isEmpty();

  }

}
