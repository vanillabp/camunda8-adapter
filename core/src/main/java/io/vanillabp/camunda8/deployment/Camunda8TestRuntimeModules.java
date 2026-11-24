package io.vanillabp.camunda8.deployment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;

/**
 * The workflow modules of one adapter id whose cluster a test runtime hands over. The
 * platform deploys a module and starts its processing once, while the application starts.
 * There is no cluster at that moment, so this class keeps the module and does both again
 * every time a cluster arrives. The test runtime deletes everything in its cluster after each
 * test method, its deployments included. So each test method gets the deployment again, and
 * workers which are open against it.
 * <p>
 * Before the cluster leaves, the workers of every module are closed the way a shutdown closes
 * them: the handlers in flight may finish, then the workers go.
 */
final class Camunda8TestRuntimeModules implements Camunda8ClientFactory.TestRuntimeParticipant {

  /**
   * The modules to deploy, in the order the platform deployed them.
   */
  private final Map<String, Camunda8ProcessingContext> toDeploy = new LinkedHashMap<>();

  /**
   * The modules whose processing the platform started, in that order. A module is removed
   * when the platform stops its processing.
   */
  private final Map<String, Camunda8ProcessingContext> toProcess = new LinkedHashMap<>();

  private final BiConsumer<String, Camunda8ProcessingContext> deployment;

  private final BiConsumer<String, Camunda8ProcessingContext> openTheWorkers;

  private final BiConsumer<String, Camunda8ProcessingContext> closeTheWorkers;

  /**
   * Whether the cluster of the test runtime is there right now.
   */
  private boolean clusterPresent;

  /**
   * @param deployment Sends the deployment of one module to the cluster which just arrived
   * @param openTheWorkers Opens the workers of one module against that cluster
   * @param closeTheWorkers Closes the workers of one module before the cluster leaves
   */
  Camunda8TestRuntimeModules(
      final BiConsumer<String, Camunda8ProcessingContext> deployment,
      final BiConsumer<String, Camunda8ProcessingContext> openTheWorkers,
      final BiConsumer<String, Camunda8ProcessingContext> closeTheWorkers) {

    this.deployment = deployment;
    this.openTheWorkers = openTheWorkers;
    this.closeTheWorkers = closeTheWorkers;

  }

  /**
   * Keeps a workflow module whose resources the platform deployed while no cluster was there.
   *
   * @param workflowModuleId The workflow module
   * @param context Its resources, ready to deploy
   */
  synchronized void deployOnceTheClusterArrives(
      final String workflowModuleId,
      final Camunda8ProcessingContext context) {

    toDeploy.put(workflowModuleId, context);

  }

  /**
   * Keeps a workflow module whose processing the platform started. Its workers are opened
   * at once if a cluster is there, and with every cluster which arrives later.
   *
   * @param workflowModuleId The workflow module
   * @param context What the module serves
   */
  synchronized void processOnceTheClusterArrives(
      final String workflowModuleId,
      final Camunda8ProcessingContext context) {

    toProcess.put(workflowModuleId, context);
    if (clusterPresent) {
      openTheWorkers.accept(workflowModuleId, context);
    }

  }

  /**
   * Forgets a workflow module whose processing the platform stopped. A cluster which arrives
   * later gets no workers for it.
   *
   * @param workflowModuleId The workflow module
   */
  synchronized void stopProcessing(
      final String workflowModuleId) {

    toProcess.remove(workflowModuleId);

  }

  @Override
  public synchronized void clusterArrived() {

    clusterPresent = true;
    toDeploy.forEach(deployment);
    toProcess.forEach(openTheWorkers);

  }

  @Override
  public synchronized void clusterLeaves() {

    // the modules opened last are closed first, as the platform does on a shutdown
    final var modules = new ArrayList<>(toProcess.entrySet());
    Collections.reverse(modules);
    modules.forEach(module -> closeTheWorkers.accept(module.getKey(), module.getValue()));
    clusterPresent = false;

  }

}
