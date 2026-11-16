package io.vanillabp.camunda8.client;

import java.util.UUID;

import io.camunda.client.CamundaClient;

/**
 * Where a cluster keeps the identity which VanillaBP gave it. This is the 8.9 variant.
 * <p>
 * The clusters of this line have globally scoped cluster variables. The first start creates
 * {@value #NAME} with a random UUID, and every later start reads it back. A cluster variable is
 * part of the state of the cluster, so a restored backup of the same cluster keeps it and a new
 * cluster does not have it. That is exactly what the identity has to do. The topology of a
 * cluster has no such id, and every key a cluster hands out is the same in every new cluster.
 * <p>
 * It is public because the client factory of each adapter id asks it, see
 * {@link Camunda8ClientFactory#clusterIdentity()}. It is not on the list of what an extension
 * of the pipeline is told, so it may move with the next change.
 */
public final class Camunda8ClusterIdentity {

  /**
   * The name of the cluster variable. Operate and the REST API show it under this name.
   */
  public static final String NAME = "vanillabp-cluster-identity";

  /**
   * How often a read is tried after the cluster refused a second create. The variable was
   * created a moment ago by another instance of the application, and the read model may not
   * show it yet.
   */
  private static final int READS_AFTER_A_REFUSED_CREATE = 20;

  private static final long PAUSE_BETWEEN_READS_MILLIS = 500;

  private Camunda8ClusterIdentity() {
  }

  /**
   * Reads the identity of the cluster, and creates it where the cluster has none yet.
   * <p>
   * It reads first, because every start but the very first finds the variable. A read which
   * finds nothing creates it. If the create is refused because the variable exists, another
   * instance of the application created it at the same moment, and the read is repeated until
   * the read model shows it.
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
    try {
      client
          .newGloballyScopedClusterVariableCreateRequest()
          .create(NAME, identity)
          .send()
          .join();
      return identity;
    } catch (final RuntimeException refused) {
      if (!alreadyExists(refused)) {
        throw refused;
      }
    }
    for (var attempt = 0; attempt < READS_AFTER_A_REFUSED_CREATE; ++attempt) {
      pause();
      final var createdByAnotherInstance = read(client);
      if (createdByAnotherInstance != null) {
        return createdByAnotherInstance;
      }
    }
    throw new IllegalStateException(
        "The cluster refused to create the variable '%s' because it exists, but did not show it within %d ms"
            .formatted(NAME, READS_AFTER_A_REFUSED_CREATE * PAUSE_BETWEEN_READS_MILLIS));

  }

  /**
   * The identity the cluster holds, or <code>null</code> where it holds none yet.
   */
  private static String read(
      final CamundaClient client) {

    try {
      final var variable = client
          .newGloballyScopedClusterVariableGetRequest()
          .withName(NAME)
          .send()
          .join();
      return withoutJsonQuotes(variable.getValue());
    } catch (final RuntimeException e) {
      if (Camunda8Errors.notFound(e)) {
        return null;
      }
      throw e;
    }

  }

  /**
   * The cluster stores a value as JSON, so a text comes back in quotes.
   */
  private static String withoutJsonQuotes(
      final String value) {

    if ((value != null) && (value.length() >= 2) && value.startsWith("\"") && value.endsWith("\"")) {
      return value.substring(1, value.length() - 1);
    }
    return value;

  }

  /**
   * Whether the cluster refused a create because the variable exists. The REST gateway says so
   * with HTTP 409, and the cause chain is walked because the client wraps what it got.
   */
  private static boolean alreadyExists(
      final Throwable failure) {

    for (var cause = failure; cause != null; cause = cause.getCause()) {
      if ((cause instanceof io.camunda.client.api.command.ClientHttpException http) && (http.code() == 409)) {
        return true;
      }
      if (cause.getCause() == cause) {
        return false;
      }
    }
    return false;

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
