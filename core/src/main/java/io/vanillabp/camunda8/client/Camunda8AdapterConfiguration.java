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
 *   <li>{@code .auth.*} - how the adapter authenticates, see
 *       {@link Camunda8AuthConfiguration}</li>
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
   * How this adapter instance proves who it is - the block
   * <code>vanillabp.adapters.&lt;id&gt;.auth.*</code>. Never <code>null</code>: an
   * adapter without the block authenticates with <code>none</code>, which is what every
   * configuration written before story 88 did.
   */
  private Camunda8AuthConfiguration auth = new Camunda8AuthConfiguration();

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
   * How this adapter instance runs what it delivers: a positive number of platform
   * threads, or the literal <code>virtual</code>. Default: four platform threads.
   * <p>
   * The number is adapter-wide on purpose: the Camunda client owns one executor and
   * this adapter owns one client per adapter id, so every worker of the adapter shares
   * it, across every workflow module that adapter serves. See
   * {@link Camunda8ExecutionModel} for what the number should be sized against.
   */
  private String workerThreads;

  /**
   * How many handlers may run at the same time while
   * {@link #workerThreads} is <code>virtual</code> - virtual threads have no limit of
   * their own, and the client's limit is per worker. Default: the number the
   * platform-thread mode would use, so switching the mode changes how threads are made
   * and not how much runs at once. Configuring it without the virtual mode fails the
   * boot, because it would be ignored.
   */
  private Integer workerThreadsBound;

  /**
   * How many jobs one worker may hold at the same time (the client's
   * <code>maxJobsActive</code>). Default: eight per execution slot, capped at the
   * client's own 32 - so the last job of a batch waits for at most seven handler
   * runtimes at the default of four threads instead of the thirty-one it would wait
   * with one thread. Camunda's rule is
   * <code>maxJobsActive &lt; threads &times; (jobTimeout / avgHandlerDuration)</code>.
   */
  private Integer maxJobsActive;

  /**
   * How long a worker waits between two activation requests. Default: the client's 100
   * milliseconds.
   */
  private java.time.Duration pollInterval;

  /**
   * How long a request to the cluster may take, which for an activation request is also
   * the long-polling window. Default: the client's 10 seconds.
   */
  private java.time.Duration requestTimeout;

  /**
   * Whether the cluster PUSHES jobs to the workers instead of only answering their
   * polls. Default: the client's <code>false</code>. It lowers the delivery latency and
   * adds no concurrency, and it makes the client wrap the execution slots in a second
   * semaphore of {@link #maxJobsActive} permits whose acquire waits for the job timeout.
   */
  private Boolean streamEnabled;

  /**
   * How long a job stream stays open before the client re-opens it. Default: the
   * client's. Only relevant with {@link #streamEnabled}.
   */
  private java.time.Duration streamTimeout;

  /**
   * How long the cluster buffers a published message waiting for a subscription, which
   * is also the window a message id deduplicates in. Default: the client's one hour.
   */
  private java.time.Duration messageTimeToLive;

  /**
   * The client's maximum inbound message size in bytes. Default: the client's.
   */
  private Integer maxMessageSize;

  /**
   * The keep-alive interval of the client's connections. Default: the client's.
   */
  private java.time.Duration keepAlive;

  /**
   * How many HTTP connections the REST transport may open. Default: the client's.
   */
  private Integer maxHttpConnections;

  /**
   * The authority the TLS certificate is verified against, for a gateway reached under
   * another name than the certificate carries. Default: none.
   */
  private String overrideAuthority;

  /**
   * How long a workflow this cluster holds may stay invisible to the query API the
   * awareness probe searches: the exporter feeding that read model runs behind the
   * engine, so a workflow started moments ago is not findable yet.
   * <p>
   * VanillaBP waits this out where it knows the workflow is here (after a start it
   * dispatched, or after this cluster delivered a job of that workflow), and never
   * for a workflow nobody ever heard of. Raise it for a slow exporter, set it to
   * zero to switch the waiting off. Default: 10 seconds.
   */
  private java.time.Duration workflowVisibilityTimeout;

  /**
   * Whether NO connection property is set at all - the "not configured yet" state:
   * the application still boots (with a guiding startup warning), only using the
   * adapter fails. The defaulted properties ({@link #mode},
   * {@link #preferRestOverGrpc}) do not count.
   *
   * @return Whether the connection configuration is entirely absent
   */
  public boolean isAbsent() {

    return !defaultedPropertySet && auth.isAbsent() && isBlank(restAddress) && isBlank(grpcAddress) && isBlank(
        tenantId) && isBlank(
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
   * The client's own default for <code>max-jobs-active</code>, which is also the cap of
   * this adapter's default.
   */
  public static final int CLIENT_MAX_JOBS_ACTIVE = 32;

  /**
   * How many jobs one worker holds per execution slot where nothing is configured.
   */
  public static final int JOBS_PER_SLOT = 8;

  /**
   * The resolved execution model of this adapter instance.
   *
   * @param adapterId The adapter id (used to build property keys in messages)
   * @return The model
   * @throws IllegalStateException If <code>worker-threads</code> or
   *           <code>worker-threads-bound</code> is not usable
   */
  public Camunda8ExecutionModel executionModel(
      final String adapterId) {

    return Camunda8ExecutionModel.resolve(adapterId, workerThreads, workerThreadsBound);

  }

  /**
   * The resolved <code>max-jobs-active</code> of this adapter instance: the configured
   * value, or {@value #JOBS_PER_SLOT} per execution slot capped at the client's
   * {@value #CLIENT_MAX_JOBS_ACTIVE}.
   *
   * @param adapterId The adapter id (used to build property keys in messages)
   * @return How many jobs one worker of this adapter may hold at the same time
   * @throws IllegalStateException If the configured value would leave execution slots
   *           idle by construction
   */
  public int resolvedMaxJobsActive(
      final String adapterId) {

    final var model = executionModel(adapterId);
    if (maxJobsActive == null) {
      return Math.min(model.slots() * JOBS_PER_SLOT, CLIENT_MAX_JOBS_ACTIVE);
    }
    if (maxJobsActive < model.slots()) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' has '%s: %d' while '%s' allows %d handlers at the same time. \
              A worker never holds more jobs than it was allowed to activate, so %d of the %d \
              execution slots would stay idle by construction. Raise the one or lower the other \
              (the default is %d per slot, capped at the client's %d)."""
              .formatted(
                  adapterId,
                  propertyKey(adapterId, "max-jobs-active"),
                  maxJobsActive,
                  propertyKey(adapterId, "worker-threads"),
                  model.slots(),
                  model.slots() - maxJobsActive,
                  model.slots(),
                  JOBS_PER_SLOT,
                  CLIENT_MAX_JOBS_ACTIVE));
    }
    return maxJobsActive;

  }

  /**
   * Validates everything about the way this adapter instance runs its workers - the
   * execution model, its bound and <code>max-jobs-active</code>. Called AT STARTUP for
   * every configured adapter id, independently of whether the connection configuration
   * is complete: a number which cannot work is a typo, and a typo is worth the boot.
   *
   * @param adapterId The adapter id
   * @throws IllegalStateException If a value is not usable, naming the property and the
   *           way out
   */
  public void validateWorkerConfiguration(
      final String adapterId) {

    resolvedMaxJobsActive(adapterId);

  }

  /**
   * Validates how this adapter instance authenticates - AT STARTUP, for every configured
   * adapter id, and independently of whether the connection configuration is complete.
   * Credentials which cannot be built are a defect of their own.
   *
   * @param adapterId The adapter id
   * @throws IllegalStateException If the authentication block cannot be used, naming the
   *           method, the missing keys and the YAML which completes them
   */
  public void validateAuthentication(
      final String adapterId) {

    auth.validate(adapterId, mode);

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
