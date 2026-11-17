package io.vanillabp.camunda8.client;

import java.util.UUID;

import io.camunda.client.CamundaClient;
import io.camunda.zeebe.model.bpmn.Bpmn;

/**
 * Where a cluster keeps the identity which VanillaBP gave it. This is the 8.8 variant.
 * <p>
 * The clusters of this line have no cluster variables. So the identity is the NAME of a small
 * process definition, {@code vanillabp-cluster-identity}, which the first start deploys with a random UUID as
 * its name. It is never started. A deployment is part of the state of the cluster, so a restored
 * backup of the same cluster keeps it, and a new cluster does not have it. The key and the
 * version of the definition cannot serve as the identity, because a new cluster hands out the
 * same key and version 1 again. The name is what differs.
 * <p>
 * If two instances of the application deploy at the same moment, the cluster keeps both as
 * version 1 and 2. Version 1 is the identity, so every instance reads the same one.
 * <p>
 * Reading the definition needs the query API, so a cluster without secondary storage cannot
 * tell its identity, and the caller goes on without one.
 * <p>
 * It is public because the client factory of each adapter id asks it, see
 * {@link Camunda8ClientFactory#clusterIdentity()}. It is not on the list of what an extension
 * of the pipeline is told, so it may move with the next change.
 */
public final class Camunda8ClusterIdentity {

  /**
   * The BPMN process id of the marker. Operate shows the definition under this id.
   */
  private static final String MARKER_ID = "vanillabp-cluster-identity";

  /**
   * How often the read is tried after a deployment which was not the first one. The read
   * model may not show version 1 yet.
   */
  private static final int READS_AFTER_A_LATE_DEPLOYMENT = 20;

  private static final long PAUSE_BETWEEN_READS_MILLIS = 500;

  private Camunda8ClusterIdentity() {
  }

  /**
   * Reads the identity of the cluster, and deploys the marker where the cluster has none yet.
   *
   * @param client The client of the adapter id
   * @return The identity
   * @throws RuntimeException If the cluster cannot be asked. The caller reports it and goes
   *           on without an identity
   */
  public static String readOrCreate(
      final CamundaClient client) {

    final var found = read(client);
    if (found != null) {
      return found;
    }
    final var identity = UUID.randomUUID().toString();
    final var marker = Bpmn
        .createExecutableProcess(MARKER_ID)
        .name(identity)
        .startEvent()
        .endEvent()
        .done();
    final var deployed = client
        .newDeployResourceCommand()
        .addProcessModel(marker, MARKER_ID
            + ".bpmn")
        .send()
        .join()
        .getProcesses()
        .getFirst();
    if (deployed.getVersion() == 1) {
      return identity;
    }
    // another instance deployed its marker a moment earlier. Its version 1 is the identity
    for (var attempt = 0; attempt < READS_AFTER_A_LATE_DEPLOYMENT; ++attempt) {
      pause();
      final var deployedByAnotherInstance = read(client);
      if (deployedByAnotherInstance != null) {
        return deployedByAnotherInstance;
      }
    }
    throw new IllegalStateException(
        "The marker '%s' was deployed as version %d, but version 1 did not show within %d ms"
            .formatted(MARKER_ID, deployed.getVersion(), READS_AFTER_A_LATE_DEPLOYMENT * PAUSE_BETWEEN_READS_MILLIS));

  }

  /**
   * The name of version 1 of the marker, or <code>null</code> where the cluster holds none.
   */
  private static String read(
      final CamundaClient client) {

    return client
        .newProcessDefinitionSearchRequest()
        .filter(filter -> filter
            .processDefinitionId(MARKER_ID)
            .version(Integer.valueOf(1)))
        .page(page -> page.limit(Integer.valueOf(1)))
        .send()
        .join()
        .items()
        .stream()
        .findFirst()
        .map(io.camunda.client.api.search.response.ProcessDefinition::getName)
        .orElse(null);

  }

  private static void pause() {

    try {
      Thread.sleep(PAUSE_BETWEEN_READS_MILLIS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for the identity of the cluster", e);
    }

  }

}
