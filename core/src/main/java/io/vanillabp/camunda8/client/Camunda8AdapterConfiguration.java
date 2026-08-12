package io.vanillabp.camunda8.client;

import lombok.Getter;
import lombok.Setter;

/**
 * Resolved, platform-neutral connection configuration of one Camunda 8 adapter instance
 * (keyed by adapter ID). The platform modules (Spring Boot, Quarkus) read their
 * respective configuration source and populate this object; the plain-Java
 * {@link Camunda8ClientFactory} turns it into a {@code CamundaClient}.
 * <p>
 * <b>Canonical configuration namespace</b> (documented in the repository-root
 * {@code README.md}), keyed by adapter ID - the adapter's keys live in the shared
 * VanillaBP tree at {@code vanillabp.adapters.<adapter-id>.*} (contributed via the
 * platform overlays, see the platform modules):
 * <ul>
 *   <li>{@code vanillabp.adapters.<adapter-id>.mode} - {@code self-managed} (default) or
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
   * The prefix of the canonical per-adapter configuration namespace (see class
   * javadoc) - the shared VanillaBP tree's adapters section.
   */
  public static final String CONFIGURATION_PREFIX = "vanillabp.adapters";

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

  /**
   * Whether one of the defaulted properties ({@link #mode},
   * {@link #preferRestOverGrpc}) was set explicitly - required to distinguish the
   * "not configured yet" state (see {@link #isAbsent()}) from an inconsistent
   * configuration like <code>mode: saas</code> without any credential.
   */
  private boolean defaultedPropertySet = false;

  public void setMode(
      final Mode mode) {

    this.mode = mode;
    this.defaultedPropertySet = true;

  }

  private String restAddress;

  private String grpcAddress;

  private boolean preferRestOverGrpc = true;

  public void setPreferRestOverGrpc(
      final boolean preferRestOverGrpc) {

    this.preferRestOverGrpc = preferRestOverGrpc;
    this.defaultedPropertySet = true;

  }

  private String tenantId;

  private String clusterId;

  private String region;

  private String clientId;

  private String clientSecret;

  /**
   * The worker's job timeout (lock duration) - adapter-level base of the
   * most-specific-wins resolution (task &gt; workflow &gt; workflow-module &gt;
   * adapter). Default: 5 minutes.
   */
  private java.time.Duration jobTimeout;

  /**
   * Whether the application states that its identifiers are unique across all of its
   * workflow modules, which is what the name-clash-avoidance mode <code>none</code>
   * relies on. It silences the WARN the adapter logs per workflow module while that
   * mode applies - a deliberate acknowledgement, not a log-level setting: with a wrong
   * one, two workflow modules address the same processes and jobs. Default
   * <code>false</code>.
   */
  private boolean acceptUnscopedIdentifiers = false;

  /**
   * How long a <code>&#64;TaskId</code> job stays dormant awaiting its
   * asynchronous completion (the job's lock is extended once to this duration
   * when the handler returns without completing). Default: 14 days.
   */
  private java.time.Duration asyncTaskTimeout;

  /**
   * Whether NO connection property is set at all - the "not configured yet" state:
   * the application still boots (with a guiding startup warning), only using the
   * adapter fails. The defaulted properties ({@link #mode},
   * {@link #preferRestOverGrpc}) do not count.
   *
   * @return Whether the connection configuration is entirely absent
   */
  public boolean isAbsent() {

    return !defaultedPropertySet && isBlank(restAddress) && isBlank(grpcAddress) && isBlank(tenantId) && isBlank(
        clusterId) && isBlank(
            region) && isBlank(clientId) && isBlank(clientSecret);

  }

  /**
   * The connection properties required for the configured {@link #mode} which are
   * not set (property KEY names relative to
   * <code>vanillabp.adapters.&lt;id&gt;.</code> - values are never part of
   * messages). An empty list means the configuration is complete.
   *
   * @return The missing property keys
   */
  public java.util.List<String> missingConnectionProperties() {

    final var missing = new java.util.LinkedList<String>();
    if (mode == Mode.SAAS) {
      if (isBlank(clusterId)) {
        missing.add("cluster-id");
      }
      if (isBlank(region)) {
        missing.add("region");
      }
      if (isBlank(clientId)) {
        missing.add("client-id");
      }
      if (isBlank(clientSecret)) {
        missing.add("client-secret");
      }
    } else if (preferRestOverGrpc) {
      if (isBlank(restAddress)) {
        missing.add("rest-address");
      }
    } else {
      if (isBlank(grpcAddress)) {
        missing.add("grpc-address");
      }
    }
    return missing;

  }

  /**
   * Validates that all properties required for the configured {@link #mode} are
   * present. The PRIMARY validation happens at startup
   * ({@link Camunda8StartupValidation}); this method remains the runtime BACKSTOP
   * called before the client is used, so a degraded adapter (startup policy
   * <code>warn</code>) still fails its first use with a guiding message instead of
   * an obscure connection error.
   *
   * @param adapterId The adapter ID (used to build the property names in error messages)
   * @throws IllegalStateException If a required property is missing, naming the exact
   *         missing configuration properties
   */
  public void validate(
      final String adapterId) {

    final var missing = missingConnectionProperties();
    if (!missing.isEmpty()) {
      throw new IllegalStateException(
          ("Camunda 8 adapter '%s' is used but not configured: the %s missing. "
              + "Configure the Camunda 8 connection for this adapter instance.")
              .formatted(
                  adapterId,
                  missing.size() == 1
                      ? "property '%s' is".formatted(propertyKey(adapterId, missing.getFirst()))
                      : "properties %s are".formatted(missing
                          .stream()
                          .map(key -> "'%s'".formatted(propertyKey(adapterId, key)))
                          .collect(java.util.stream.Collectors.joining(", ")))));
    }

  }

  /**
   * Builds the full property key of a connection property of the given adapter
   * instance (e.g. <code>vanillabp.adapters.c8.rest-address</code>).
   *
   * @param adapterId The adapter ID
   * @param key The property key relative to the adapter's section
   * @return The full property key
   */
  public static String propertyKey(
      final String adapterId,
      final String key) {

    return "%s.%s.%s".formatted(CONFIGURATION_PREFIX, adapterId, key);

  }

  private static boolean isBlank(
      final String value) {

    return (value == null) || value.isBlank();

  }

}
