package io.vanillabp.camunda8.deployment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What the deployment service does with its workflow modules each time a test runtime hands
 * over its cluster or takes it back. Nothing here reaches a cluster: the three steps are
 * recorded instead of run.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8TestRuntimeModulesTest {

  private final List<String> steps = new ArrayList<>();

  private boolean failTheNextDeployment;

  private final Camunda8TestRuntimeModules modules = new Camunda8TestRuntimeModules(
      this::forget, this::deploy, this::open, this::close);

  private void forget() {

    steps.add("forget");

  }

  private void deploy(
      final String module,
      final Camunda8ProcessingContext context) {

    if (failTheNextDeployment) {
      failTheNextDeployment = false;
      throw new IllegalStateException("the cluster refused the deployment of '%s'".formatted(module));
    }
    steps.add("deploy %s".formatted(module));

  }

  private void open(
      final String module,
      final Camunda8ProcessingContext context) {

    steps.add("open %s".formatted(module));

  }

  private void close(
      final String module,
      final Camunda8ProcessingContext context) {

    steps.add("close %s".formatted(module));

  }

  private static Camunda8ProcessingContext contextOf(
      final String module) {

    return new Camunda8ProcessingContext("c8", module, null);

  }

  @Test
  @DisplayName("Each cluster gets every deployment and every worker, and loses the workers in reverse order")
  public void eachClusterGetsEverything() {

    modules.deployOnceTheClusterArrives("first", contextOf("first"));
    modules.deployOnceTheClusterArrives("second", contextOf("second"));
    modules.processOnceTheClusterArrives("first", contextOf("first"));
    modules.processOnceTheClusterArrives("second", contextOf("second"));
    // nothing happens while there is no cluster
    assertEquals(List.of(), steps);

    modules.clusterArrived();
    modules.clusterLeaves();
    modules.clusterArrived();

    assertEquals(
        List.of("forget", "deploy first", "deploy second", "open first", "open second", "close second", "close first",
            "forget", "deploy first", "deploy second", "open first", "open second"),
        steps);

  }

  @Test
  @DisplayName("A module which starts processing while a cluster is there opens its workers at once")
  public void aModuleStartingWhileAClusterIsThereOpensAtOnce() {

    modules.clusterArrived();
    steps.clear();

    modules.processOnceTheClusterArrives("late", contextOf("late"));

    assertEquals(List.of("open late"), steps);

  }

  @Test
  @DisplayName("A module whose processing stopped gets no workers from a later cluster")
  public void aStoppedModuleGetsNoWorkers() {

    modules.deployOnceTheClusterArrives("kept", contextOf("kept"));
    modules.processOnceTheClusterArrives("kept", contextOf("kept"));
    modules.processOnceTheClusterArrives("stopped", contextOf("stopped"));

    modules.stopProcessing("stopped");
    modules.clusterArrived();
    modules.clusterLeaves();

    assertEquals(List.of("forget", "deploy kept", "open kept", "close kept"), steps);

  }

  @Test
  @DisplayName("A deployment which fails fails that handover, and the next cluster gets everything again")
  public void aFailedDeploymentFailsOneHandoverOnly() {

    modules.deployOnceTheClusterArrives("orders", contextOf("orders"));
    modules.processOnceTheClusterArrives("orders", contextOf("orders"));
    failTheNextDeployment = true;

    final var refused = assertThrows(IllegalStateException.class, modules::clusterArrived);
    assertEquals("the cluster refused the deployment of 'orders'", refused.getMessage());
    modules.clusterLeaves();
    modules.clusterArrived();

    assertEquals(List.of("forget", "close orders", "forget", "deploy orders", "open orders"), steps);

  }

}
