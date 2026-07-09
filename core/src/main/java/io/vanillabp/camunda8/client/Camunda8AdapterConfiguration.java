package io.vanillabp.camunda8.client;

import lombok.Getter;
import lombok.Setter;

/**
 * Resolved, platform-neutral connection configuration of one Camunda 8 adapter instance
 * (keyed by adapter ID). The platform modules (Spring Boot, Quarkus) read their
 * respective configuration source and populate this object; the plain-Java
 * {@link Camunda8ClientFactory} turns it into a {@code CamundaClient}.
 * <p>
 * <b>Provisional flat configuration namespace</b> (documented in the repository-root
 * {@code README.md}), keyed by adapter ID:
 * <ul>
 *   <li>{@code camunda8-adapter.<adapter-id>.mode} - {@code self-managed} (default) or
 *       {@code saas}</li>
 *   <li>self-managed: {@code .rest-address} (required unless
 *       {@code .prefer-rest-over-grpc=false}), {@code .grpc-address} (optional, required
 *       when {@code .prefer-rest-over-grpc=false})</li>
 *   <li>saas: {@code .cluster-id}, {@code .region}, {@code .client-id},
 *       {@code .client-secret} (all required)</li>
 *   <li>{@code .tenant-id} (optional, both modes) - Camunda 8 multi-tenancy tenant</li>
 *   <li>{@code .prefer-rest-over-grpc} (optional, default {@code true}) - whether the
 *       client uses the REST API (recommended) or gRPC for its commands</li>
 * </ul>
 * All fields are optional at binding time so applications which configure a Camunda 8
 * adapter but never actually use it still boot; {@link #validate(String)} enforces the
 * required fields lazily on first use of the client.
 */
@Getter
@Setter
public class Camunda8AdapterConfiguration {

  /**
   * The prefix of the provisional flat configuration namespace (see class javadoc).
   */
  public static final String CONFIGURATION_PREFIX = "camunda8-adapter";

  /**
   * Connection mode of a Camunda 8 adapter instance.
   */
  public enum Mode {
    /** Self-managed cluster (on-premises or self-hosted) addressed by REST/gRPC. */
    SELF_MANAGED,
    /** Camunda 8 SaaS, addressed via the cloud client builder. */
    SAAS
  }

  private Mode mode = Mode.SELF_MANAGED;

  private String restAddress;

  private String grpcAddress;

  private boolean preferRestOverGrpc = true;

  private String tenantId;

  private String clusterId;

  private String region;

  private String clientId;

  private String clientSecret;

  /**
   * Validates that all properties required for the configured {@link #mode} are present.
   * Called lazily by {@link Camunda8ClientFactory} on first use of the client, so an
   * application configuring - but not using - a Camunda 8 adapter still boots.
   *
   * @param adapterId The adapter ID (used to build the property name in error messages)
   * @throws IllegalStateException If a required property is missing, naming the exact
   *         missing configuration property
   */
  public void validate(
      final String adapterId) {

    if (mode == Mode.SAAS) {
      requireProperty(adapterId, "cluster-id", clusterId);
      requireProperty(adapterId, "region", region);
      requireProperty(adapterId, "client-id", clientId);
      requireProperty(adapterId, "client-secret", clientSecret);
    } else if (preferRestOverGrpc) {
      requireProperty(adapterId, "rest-address", restAddress);
    } else {
      requireProperty(adapterId, "grpc-address", grpcAddress);
    }

  }

  private void requireProperty(
      final String adapterId,
      final String key,
      final String value) {

    if (value == null || value.isBlank()) {
      throw new IllegalStateException(
          ("Camunda 8 adapter '%s' is used but not configured: the property '%s.%s.%s' is "
              + "missing. Configure the Camunda 8 connection for this adapter instance.")
              .formatted(adapterId, CONFIGURATION_PREFIX, adapterId, key));
    }

  }

}
