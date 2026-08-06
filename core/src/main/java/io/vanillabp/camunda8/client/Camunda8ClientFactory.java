package io.vanillabp.camunda8.client;

import java.net.URI;

import io.camunda.client.CamundaClient;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds and owns the single {@link CamundaClient} of one Camunda 8 adapter instance
 * (adapter ID). One factory exists <b>per adapter ID</b> (not per adapter type)
 * because the same BPMS type may be configured multiple times for a BPMS migration.
 * <p>
 * The client is built EAGERLY at construction time (i.e. at application startup) if
 * the connection configuration is complete - configuration defects surface at boot,
 * not first at runtime (see {@link Camunda8StartupValidation}). Building the client
 * neither opens a connection nor contacts the cluster - that happens only when the
 * first command is sent. The factory is closed on {@link #close()} (called on
 * application shutdown by the platform bean lifecycle).
 * <p>
 * An application which configures a Camunda 8 adapter incompletely may still boot
 * (absent configuration, or the degraded 'warn' policy): no client is built then, and
 * {@link #getClient()} fails as a runtime BACKSTOP with a message naming the missing
 * properties (see {@link Camunda8AdapterConfiguration#validate(String)}).
 */
@Slf4j
public class Camunda8ClientFactory implements AutoCloseable {

  @Getter
  private final String adapterId;

  @Getter
  private final Camunda8AdapterConfiguration configuration;

  /**
   * The per-adapter-id record of what this application version deployed (story
   * 26's viewer API). It lives here because the factory is the one object BOTH
   * the deployment service (which fills it) and the process service (which reads
   * it) already receive per adapter id on both platforms.
   */
  @Getter
  private final io.vanillabp.camunda8.deployment.Camunda8DeployedProcesses deployedProcesses = new io.vanillabp.camunda8.deployment.Camunda8DeployedProcesses();

  private CamundaClient client;

  public Camunda8ClientFactory(
      final String adapterId,
      final Camunda8AdapterConfiguration configuration) {

    this.adapterId = adapterId;
    this.configuration = configuration;
    // eager: configuration defects surface at startup, not first at runtime; an
    // incompletely configured adapter (absent / degraded) builds no client and
    // fails on first use instead (backstop)
    if (configuration.missingConnectionProperties().isEmpty()) {
      this.client = build();
    }

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
   * Whether {@link #close()} was called: a dispatch racing the shutdown must not
   * use a client which is about to be closed.
   */
  private volatile boolean closed = false;

  /**
   * @return The eagerly built {@link CamundaClient} of this adapter instance
   * @throws IllegalStateException If the adapter's connection configuration is
   *         incomplete (runtime backstop naming the missing properties) or the
   *         factory was already closed (application shutdown)
   */
  public CamundaClient getClient() {

    if (closed) {
      throw new IllegalStateException(
          "The Camunda 8 client factory of adapter '%s' was already closed (application shutdown)!"
              .formatted(adapterId));
    }
    if (client == null) {
      // backstop for adapters which booted unconfigured/degraded - throws with a
      // guiding message naming the missing properties
      configuration.validate(adapterId);
    }
    return client;

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

    closed = true;
    if (client != null) {
      log.info("Closing Camunda 8 client of adapter '{}'", adapterId);
      client.close();
      client = null;
    }

  }

}
