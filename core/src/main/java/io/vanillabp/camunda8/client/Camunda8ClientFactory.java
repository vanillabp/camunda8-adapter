package io.vanillabp.camunda8.client;

import java.net.URI;

import io.camunda.client.CamundaClient;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Lazily builds and owns the single {@link CamundaClient} of one Camunda 8 adapter
 * instance (adapter ID). One factory exists <b>per adapter ID</b> (not per adapter type)
 * because the same BPMS type may be configured multiple times for a BPMS migration.
 * <p>
 * The client is built on first use ({@link #getClient()}) from the
 * {@link Camunda8AdapterConfiguration} and closed on {@link #close()} (called on
 * application shutdown by the platform bean lifecycle). Building the client neither opens
 * a connection nor contacts the cluster - that happens only when the first command is
 * sent.
 * <p>
 * An application which configures a Camunda 8 adapter but never uses it still boots: the
 * configuration is validated lazily, so a missing connection property fails only on first
 * use with a message naming the exact missing property (see
 * {@link Camunda8AdapterConfiguration#validate(String)}).
 */
@Slf4j
public class Camunda8ClientFactory implements AutoCloseable {

  @Getter
  private final String adapterId;

  @Getter
  private final Camunda8AdapterConfiguration configuration;

  private volatile CamundaClient client;

  public Camunda8ClientFactory(
      final String adapterId,
      final Camunda8AdapterConfiguration configuration) {

    this.adapterId = adapterId;
    this.configuration = configuration;

  }

  /**
   * Validates that the adapter instance is configured without building the client or
   * contacting the cluster. Used by phase one of starting a workflow (which runs inside
   * the caller's database transaction and must not do any remote call).
   *
   * @throws IllegalStateException If a required connection property is missing
   */
  public void validateConfigured() {

    configuration.validate(adapterId);

  }

  /**
   * @return The lazily built {@link CamundaClient} of this adapter instance
   * @throws IllegalStateException If a required connection property is missing
   */
  public CamundaClient getClient() {

    var result = client;
    if (result == null) {
      synchronized (this) {
        result = client;
        if (result == null) {
          configuration.validate(adapterId);
          result = build();
          client = result;
        }
      }
    }
    return result;

  }

  private CamundaClient build() {

    if (configuration.getMode() == Camunda8AdapterConfiguration.Mode.SAAS) {
      log.info("Building Camunda 8 SaaS client for adapter '{}' (cluster '{}', region '{}')",
          adapterId, configuration.getClusterId(), configuration.getRegion());
      final var builder = CamundaClient
          .newCloudClientBuilder()
          .withClusterId(configuration.getClusterId())
          .withClientId(configuration.getClientId())
          .withClientSecret(configuration.getClientSecret())
          .withRegion(configuration.getRegion());
      if (hasText(configuration.getTenantId())) {
        builder.defaultTenantId(configuration.getTenantId());
      }
      return builder.build();
    }

    log.info("Building Camunda 8 self-managed client for adapter '{}' (rest-address '{}', grpc-address '{}', "
        + "prefer-rest-over-grpc {})",
        adapterId, configuration.getRestAddress(), configuration.getGrpcAddress(),
        configuration.isPreferRestOverGrpc());
    final var builder = CamundaClient
        .newClientBuilder()
        .preferRestOverGrpc(configuration.isPreferRestOverGrpc());
    if (hasText(configuration.getRestAddress())) {
      builder.restAddress(URI.create(configuration.getRestAddress()));
    }
    if (hasText(configuration.getGrpcAddress())) {
      builder.grpcAddress(URI.create(configuration.getGrpcAddress()));
    }
    if (hasText(configuration.getTenantId())) {
      builder.defaultTenantId(configuration.getTenantId());
    }
    return builder.build();

  }

  private static boolean hasText(
      final String value) {

    return value != null && !value.isBlank();

  }

  @Override
  public synchronized void close() {

    if (client != null) {
      log.info("Closing Camunda 8 client of adapter '{}'", adapterId);
      client.close();
      client = null;
    }

  }

}
