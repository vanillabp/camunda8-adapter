package io.vanillabp.camunda8.deployment;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.camunda.zeebe.model.bpmn.BpmnModelInstance;

/**
 * What THIS application version deployed to Camunda 8 - per adapter id, filled by
 * {@link Camunda8DeploymentService#deployResources} at every boot.
 * <p>
 * <b>Why the adapter keeps this:</b> Camunda 8 is remote and its process
 * definitions/XML are only readable through the query API (secondary storage,
 * eventually consistent). VanillaBP's deployment pipeline reads every workflow
 * module's BPMN at EVERY boot anyway, so the adapter can serve the viewer API's
 * definitions and BPMN XML from these freshly read models - no cluster round trip,
 * no consistency lag, and it works on clusters without secondary storage.
 * <p>
 * <b>The boundary:</b> only definitions deployed by the RUNNING application version
 * are held here. A workflow still running on a definition deployed by a PREVIOUS
 * application version (a long-running workflow surviving a redeployment) is served
 * from the cluster instead ({@code ProcessDefinitionGetXmlRequest}), which needs the
 * query API. Without it, the viewer falls back to the version deployed now - see the
 * README.
 */
public class Camunda8DeployedProcesses {

  /**
   * A process deployed by this application version.
   *
   * @param workflowModuleId The workflow module the process belongs to
   * @param bpmnProcessId The BPMN process id
   * @param processDefinitionKey The Camunda 8 process definition key (the
   *        adapter-native definition id)
   * @param version The version assigned by the cluster
   * @param model The BPMN model AS DEPLOYED (VanillaBP's wiring modifications
   *        included - what the cluster runs)
   */
  public record DeployedProcess(
                                String workflowModuleId,
                                String bpmnProcessId,
                                String processDefinitionKey,
                                int version,
                                BpmnModelInstance model) {
  }

  /**
   * By process definition key - the lookup of {@code getBpmnXml}.
   */
  private final Map<String, DeployedProcess> byDefinitionKey = new ConcurrentHashMap<>();

  /**
   * By {@code <workflow module>|<bpmn process id>} - the lookup of "which version
   * would be executed next".
   */
  private final Map<String, DeployedProcess> byProcess = new ConcurrentHashMap<>();

  public void record(
      final DeployedProcess deployedProcess) {

    byDefinitionKey.put(deployedProcess.processDefinitionKey(), deployedProcess);
    byProcess.put(
        key(deployedProcess.workflowModuleId(), deployedProcess.bpmnProcessId()),
        deployedProcess);

  }

  /**
   * @param processDefinitionKey The Camunda 8 process definition key
   * @return The deployed process or <code>null</code> if not deployed by this
   *         application version
   */
  /**
   * Every process of a workflow module deployed by THIS application version - the
   * models the message-name check of {@code correlateMessage} reads (story 73). A
   * workflow module whose processes were deployed by a previous application version
   * yields an empty collection, and the check then stays silent.
   *
   * @param workflowModuleId The workflow module
   * @return The processes deployed by this application version
   */
  public java.util.Collection<DeployedProcess> ofWorkflowModule(
      final String workflowModuleId) {

    return byProcess
        .values()
        .stream()
        .filter(deployed -> deployed
            .workflowModuleId()
            .equals(workflowModuleId))
        .toList();

  }

  /**
   * Every process this application version deployed through this adapter id - the set the
   * awareness probes derive the adapter's SCOPE from (story 103).
   *
   * @return The deployed processes, empty where nothing was deployed (a module whose
   *         deployment failed under the 'warn' policy, or a test)
   */
  public java.util.Collection<DeployedProcess> all() {

    return java.util.List.copyOf(byProcess.values());

  }

  public DeployedProcess byDefinitionKey(
      final String processDefinitionKey) {

    return byDefinitionKey.get(processDefinitionKey);

  }

  /**
   * @param workflowModuleId The workflow module id
   * @param bpmnProcessId The BPMN process id
   * @return The version deployed by this application version or <code>null</code>
   */
  public DeployedProcess deployedVersionOf(
      final String workflowModuleId,
      final String bpmnProcessId) {

    return byProcess.get(key(workflowModuleId, bpmnProcessId));

  }

  private static String key(
      final String workflowModuleId,
      final String bpmnProcessId) {

    return workflowModuleId
        + "|"
        + bpmnProcessId;

  }

}
