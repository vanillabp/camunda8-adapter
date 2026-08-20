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
 *   <li>{@code .async-task-lock-renewal} (optional, default one hour) - the window the
 *       lock of a job left open by a {@code @TaskId} handler is renewed in, see
 *       {@link #asyncTaskLockRenewal}</li>
 *   <li>{@code .async-task-max-age-action} (optional, default {@code report}) - what
 *       this adapter does with a task the core reports as older than
 *       {@code vanillabp.delivery.max-task-age}, see {@link #asyncTaskMaxAgeAction}</li>
 *   <li>{@code .retry-backoff} (optional, default
 *       {@value io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver#DEFAULT_RETRY_BACKOFF_ISO},
 *       resolvable per workflow module, workflow and task like {@code job-timeout}) - how
 *       long the cluster waits before it hands a FAILED job out again, see
 *       {@link #retryBackoff}</li>
 *   <li>{@code .health-timeout} (optional, default {@value #DEFAULT_HEALTH_TIMEOUT_ISO}) -
 *       how long the health check waits for the cluster's topology, see
 *       {@link #healthTimeout}</li>
 *   <li>{@code .shutdown-grace} (optional, default {@value #DEFAULT_SHUTDOWN_GRACE_ISO}) -
 *       how long a shutdown waits for the handlers it has in flight, see
 *       {@link #shutdownGrace}</li>
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
   * How long the lock of a job left open by a <code>&#64;TaskId</code> handler is
   * extended for, and with it how often that lock is renewed: the cluster hands the
   * job out again when the window passed, VanillaBP answers the redelivery from the
   * record it wrote when the handler ran, and the adapter extends the lock by another
   * window. The renewal is therefore driven by the cluster's own redelivery, and the
   * application never notices it. Default: {@value #DEFAULT_ASYNC_TASK_LOCK_RENEWAL_ISO}.
   * <p>
   * <b>Why an hour.</b> The window has to sit clearly below
   * <code>vanillabp.outbox.retention</code> (seven days by default), because the record
   * is what answers the redelivery: once it is cleaned up, the redelivery reaches the
   * application's method a second time. An hour is a factor of 168 below the default
   * retention, which survives a retention somebody lowered without thinking. The cost is
   * one activation, one read of the delivery record and one lock command per open task
   * per window - with 1.000 open asynchronous tasks that is 0,28 per second, with 10.000
   * it is 2,8 per second - and those renewals run on the same execution slots as real
   * work ({@link #workerThreads}), which is the reason not to make the window minutes.
   * The granularity of the age check follows the window, and hourly is fine for
   * something measured in days.
   * <p>
   * <b>No callback into the application.</b> There is deliberately no keep-alive hook and
   * no aware interface asking whether an open task is still wanted. An application which
   * knows its task is obsolete has <code>ProcessService#cancelTask</code>, which is
   * BPMS-neutral and exists; an application which lost track of the task could not answer
   * a liveness question truthfully anyway. What VanillaBP does instead is measure how long
   * the task has been open (<code>vanillabp.delivery.max-task-age</code>) and let this
   * adapter react to it, see {@link #asyncTaskMaxAgeAction}.
   */
  private java.time.Duration asyncTaskLockRenewal;

  /**
   * How long the cluster waits before it hands a FAILED job out again - adapter-level base
   * of the most-specific-wins resolution (task &gt; workflow &gt; workflow-module &gt;
   * adapter), see
   * {@link io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver} for the reasoning
   * behind the default of ten seconds.
   */
  private java.time.Duration retryBackoff;

  /**
   * The default of {@link #asyncTaskLockRenewal} in ISO-8601 notation, for javadoc and
   * messages.
   */
  public static final String DEFAULT_ASYNC_TASK_LOCK_RENEWAL_ISO = "PT1H";

  /**
   * The default of {@link #asyncTaskLockRenewal}: one hour.
   */
  public static final java.time.Duration DEFAULT_ASYNC_TASK_LOCK_RENEWAL = java.time.Duration
      .parse(DEFAULT_ASYNC_TASK_LOCK_RENEWAL_ISO);

  /**
   * The key this adapter used before the renewal window was named after what it is. It
   * exists only to be REJECTED at startup: it carried a horizon of fourteen days, which
   * outlived the retention of the delivery records and made every asynchronous task open
   * longer than that run the application's method a second time. VanillaBP 2 is not
   * released, so a loud rename is better than a silent one.
   */
  private java.time.Duration asyncTaskTimeout;

  /**
   * What this adapter does with a task the core reports as older than
   * <code>vanillabp.delivery.max-task-age</code>.
   */
  public enum AsyncTaskMaxAgeAction {
    /**
     * Nothing beyond the report the core writes anyway - the default, and what every
     * BPMS without a lock to renew is limited to.
     */
    REPORT,
    /**
     * Stop renewing the job's lock and fail the job, so the cluster raises an incident
     * naming the workflow aggregate and the age. Operators watch incidents, which is
     * more than can be said for a log line.
     */
    INCIDENT
  }

  /**
   * What this adapter does about a task which stayed open longer than
   * <code>vanillabp.delivery.max-task-age</code> allows (thirty days by default). Default:
   * {@link AsyncTaskMaxAgeAction#REPORT}, which leaves it at the core's message.
   * <p>
   * <code>incident</code> is what a cluster can do beyond a log line: the adapter stops
   * renewing the lock and fails the job with a message naming the aggregate and the age,
   * so the incident shows up where operators already look. It is deliberately not the
   * default - a task waiting for a person or for a partner may legitimately run for
   * weeks, and an incident on such a task would be worse than the leak the age looks for.
   */
  private AsyncTaskMaxAgeAction asyncTaskMaxAgeAction = AsyncTaskMaxAgeAction.REPORT;

  /**
   * How long the shutdown of a workflow module waits for the handlers this adapter has in
   * flight before the client is closed under them. Default:
   * {@value #DEFAULT_SHUTDOWN_GRACE_ISO}.
   * <p>
   * <b>Why twenty seconds.</b> The Camunda client does not drain: closing a worker returns
   * without waiting for the jobs it already activated, and closing the client interrupts
   * the running handlers within milliseconds. An interrupted handler throws, and a handler
   * which throws used to have its job failed with one retry less - so every restart cost a
   * retry per job in flight. The number therefore has to be longer than an ordinary handler
   * (a remote call plus a commit) and shorter than the shutdown budget of whatever runs the
   * application, so VanillaBP is never the reason a container is killed: Spring Boot's
   * <code>spring.lifecycle.timeout-per-shutdown-phase</code> and Kubernetes'
   * <code>terminationGracePeriodSeconds</code> both default to thirty seconds, and twenty
   * leaves both of them a margin. Raising this value means raising those two as well, which
   * is what the startup warning says.
   * <p>
   * <code>PT0S</code> switches the drain off: the workers are closed and the client goes
   * down right behind them. Nothing is lost by that either - a handler cut off this way has
   * its job left to its lock rather than failed - but the work it did up to the interrupt is
   * repeated on the redelivery.
   */
  private java.time.Duration shutdownGrace;

  /**
   * How long the health check of this adapter instance waits for the cluster to answer its
   * topology request. Default: {@value #DEFAULT_HEALTH_TIMEOUT_ISO}.
   * <p>
   * <b>Why two seconds.</b> The check runs on the thread serving the health request, and
   * whatever polls that endpoint has a timeout of its own - Kubernetes' readiness probe
   * defaults to one second and gives up after three failures. A check which waits longer
   * than the probe turns a slow cluster into a failing probe without ever reporting DOWN,
   * which is the worst of both. Two seconds is far above the round trip to a healthy cluster
   * and below what a probe is willing to wait. The client's own
   * <code>request-timeout</code> is deliberately NOT reused: ten seconds is right for a
   * command carrying work and wrong for a question about liveness.
   * <p>
   * <code>PT0S</code> switches the check off: the adapter then reports UNKNOWN with a note
   * saying so, and the endpoint stops talking to the cluster at all.
   */
  private java.time.Duration healthTimeout;

  /**
   * The default of {@link #healthTimeout} in ISO-8601 notation, for javadoc and messages.
   */
  public static final String DEFAULT_HEALTH_TIMEOUT_ISO = "PT2S";

  /**
   * The default of {@link #healthTimeout}: two seconds.
   */
  public static final java.time.Duration DEFAULT_HEALTH_TIMEOUT = java.time.Duration
      .parse(DEFAULT_HEALTH_TIMEOUT_ISO);

  /**
   * The default of {@link #shutdownGrace} in ISO-8601 notation, for javadoc and messages.
   */
  public static final String DEFAULT_SHUTDOWN_GRACE_ISO = "PT20S";

  /**
   * The default of {@link #shutdownGrace}: twenty seconds.
   */
  public static final java.time.Duration DEFAULT_SHUTDOWN_GRACE = java.time.Duration
      .parse(DEFAULT_SHUTDOWN_GRACE_ISO);

  /**
   * The shutdown budget both Spring Boot and Kubernetes default to, which
   * {@link #shutdownGrace} has to stay below.
   */
  public static final java.time.Duration PLATFORM_SHUTDOWN_BUDGET = java.time.Duration.ofSeconds(30);

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
   * The renewal window of this adapter instance's open asynchronous tasks: the
   * configured value or {@link #DEFAULT_ASYNC_TASK_LOCK_RENEWAL}.
   *
   * @return The window a dormant job's lock is extended by
   */
  public java.time.Duration resolvedAsyncTaskLockRenewal() {

    return asyncTaskLockRenewal != null
        ? asyncTaskLockRenewal
        : DEFAULT_ASYNC_TASK_LOCK_RENEWAL;

  }

  /**
   * The adapter-level backoff of a failed job: the configured value or
   * {@link io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver#DEFAULT_RETRY_BACKOFF}.
   *
   * @return The backoff, never <code>null</code>
   */
  public java.time.Duration resolvedRetryBackoff() {

    return retryBackoff != null
        ? retryBackoff
        : io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver.DEFAULT_RETRY_BACKOFF;

  }

  /**
   * Validates the backoff of a failed job - AT STARTUP, because what it decides happens
   * long after the boot and only when something already went wrong.
   *
   * @param adapterId The adapter id
   * @throws IllegalStateException If the backoff is negative
   */
  public void validateRetryBackoff(
      final String adapterId) {

    if ((retryBackoff == null) || !retryBackoff.isNegative()) {
      return;
    }
    throw new IllegalStateException(
        """
            Camunda 8 adapter '%s' has '%s: %s'. The backoff is how long the cluster waits before it hands \
            a failed job out again, so it cannot be negative. Give it a duration (the default is %s), or \
            'PT0S' to have the job handed out again as fast as the cluster can - which is what burns a \
            job's retries while the cause of the failure has not passed yet."""
            .formatted(
                adapterId,
                propertyKey(adapterId, "retry-backoff"),
                retryBackoff,
                io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver.DEFAULT_RETRY_BACKOFF_ISO));

  }

  /**
   * How long the health check of this adapter instance waits for the cluster: the configured
   * value or {@link #DEFAULT_HEALTH_TIMEOUT}.
   *
   * @return The timeout, never <code>null</code>
   */
  public java.time.Duration resolvedHealthTimeout() {

    return healthTimeout != null
        ? healthTimeout
        : DEFAULT_HEALTH_TIMEOUT;

  }

  /**
   * Validates the health timeout of this adapter instance - AT STARTUP, because a health
   * endpoint is read when something is already wrong and must not be the second problem.
   *
   * @param adapterId The adapter id
   * @throws IllegalStateException If the timeout is negative
   */
  public void validateHealthTimeout(
      final String adapterId) {

    if ((healthTimeout == null) || !healthTimeout.isNegative()) {
      return;
    }
    throw new IllegalStateException(
        """
            Camunda 8 adapter '%s' has '%s: %s'. The timeout is how long the health check waits for the \
            cluster to answer, so it cannot be negative. Give it a duration (the default is %s), or \
            'PT0S' to switch the check off."""
            .formatted(
                adapterId,
                propertyKey(adapterId, "health-timeout"),
                healthTimeout,
                DEFAULT_HEALTH_TIMEOUT_ISO));

  }

  /**
   * How long this adapter instance's shutdown waits for the handlers it has in flight: the
   * configured value or {@link #DEFAULT_SHUTDOWN_GRACE}.
   *
   * @return The grace period, never <code>null</code>
   */
  public java.time.Duration resolvedShutdownGrace() {

    return shutdownGrace != null
        ? shutdownGrace
        : DEFAULT_SHUTDOWN_GRACE;

  }

  /**
   * Validates the shutdown grace of this adapter instance - AT STARTUP, because what it
   * decides happens when nobody is watching. A negative value is a typo and fails the boot;
   * a value which does not fit into the shutdown budget of the runtime is legitimate but
   * only works if that budget is raised too, so it is a guiding warning rather than a
   * failure.
   *
   * @param adapterId The adapter id
   * @param warnLogger Sink for the guiding warning
   * @throws IllegalStateException If the grace is negative
   */
  public void validateShutdownGrace(
      final String adapterId,
      final java.util.function.Consumer<String> warnLogger) {

    if (shutdownGrace == null) {
      return;
    }
    if (shutdownGrace.isNegative()) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' has '%s: %s'. The grace is how long a shutdown waits for the \
              handlers it has in flight, so it cannot be negative. Give it a duration (the default is \
              %s), or 'PT0S' to close the workers and the client without waiting at all."""
              .formatted(
                  adapterId,
                  propertyKey(adapterId, "shutdown-grace"),
                  shutdownGrace,
                  DEFAULT_SHUTDOWN_GRACE_ISO));
    }
    if (shutdownGrace.compareTo(PLATFORM_SHUTDOWN_BUDGET) < 0) {
      return;
    }
    warnLogger.accept(
        """
            Camunda 8 adapter '%s' has '%s: %s', which reaches into the shutdown budget of the runtime \
            around it: 'spring.lifecycle.timeout-per-shutdown-phase' and Kubernetes' \
            'terminationGracePeriodSeconds' both default to %s. With this grace the application can be \
            killed while VanillaBP is still waiting for its handlers, which is the opposite of what the \
            grace is for. Raise both budgets above the grace, or lower the grace below them (the default \
            is %s)."""
            .formatted(
                adapterId,
                propertyKey(adapterId, "shutdown-grace"),
                shutdownGrace,
                PLATFORM_SHUTDOWN_BUDGET,
                DEFAULT_SHUTDOWN_GRACE_ISO));

  }

  /**
   * Validates how this adapter instance keeps an open asynchronous task alive - AT
   * STARTUP, for every configured adapter id. Two things can be wrong here, and both are
   * silent at runtime:
   * <ul>
   * <li>the removed key <code>async-task-timeout</code> is still configured. It used to
   * be a horizon of days; it is a renewal window now, and a value meant as the one would
   * be a defect as the other;</li>
   * <li>the window is not shorter than <code>vanillabp.outbox.retention</code>. The
   * record of the delivery is what answers the redelivery which renews the lock, so a
   * window outliving the retention means the application's method runs a second time -
   * exactly the defect this window exists to fix.</li>
   * </ul>
   *
   * @param adapterId The adapter id
   * @param outboxRetention How long a delivery record is kept
   *          (<code>vanillabp.outbox.retention</code>)
   * @throws IllegalStateException If the removed key is configured or the window does not
   *           fit below the retention, naming both properties and both values
   */
  public void validateAsyncTaskLockRenewal(
      final String adapterId,
      final java.time.Duration outboxRetention) {

    if (asyncTaskTimeout != null) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' configures '%s', which does not exist any more. The property was a \
              HORIZON of 14 days a dormant job's lock was extended to once, and it outlived the records \
              which keep a redelivery from running the @WorkflowTask method again - every asynchronous \
              task open longer than the horizon was processed twice. What the adapter does now is RENEW \
              the lock in a window, driven by the cluster's own redelivery. Rename the property to '%s' \
              and give it a window rather than a horizon; the default is %s."""
              .formatted(
                  adapterId,
                  propertyKey(adapterId, "async-task-timeout"),
                  propertyKey(adapterId, "async-task-lock-renewal"),
                  DEFAULT_ASYNC_TASK_LOCK_RENEWAL_ISO));
    }

    if (outboxRetention == null) {
      return;
    }
    final var renewal = resolvedAsyncTaskLockRenewal();
    if (renewal.compareTo(outboxRetention) < 0) {
      return;
    }
    throw new IllegalStateException(
        """
            Camunda 8 adapter '%s' has '%s: %s', which is not shorter than 'vanillabp.outbox.retention: %s'. \
            The lock of a task left open by a @TaskId handler is renewed whenever the cluster hands the job \
            out again, and the redelivery is answered from the record VanillaBP wrote when the handler ran. \
            A record deleted before the next renewal therefore lets the @WorkflowTask method run a second \
            time. Choose a window well below the retention - at most a tenth of it, %s or less here - or \
            raise 'vanillabp.outbox.retention'."""
            .formatted(
                adapterId,
                propertyKey(adapterId, "async-task-lock-renewal"),
                renewal,
                outboxRetention,
                outboxRetention.dividedBy(10)));

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
