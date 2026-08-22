package io.vanillabp.camunda8.client;

import java.net.URI;

import io.camunda.client.CamundaClient;
import io.camunda.client.CamundaClientBuilder;
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
    // how this adapter runs what it delivers - resolved before anything is built, so an
    // unusable number fails the boot with a guiding message instead of being inherited
    this.executionModel = configuration.executionModel(adapterId);
    // and how it proves who it is - checked here for the same reason: credentials which
    // cannot be built are a boot failure, not a surprise on the first command
    configuration.validateAuthentication(adapterId);
    // eager: configuration defects surface at startup, not first at runtime; an
    // incompletely configured adapter (absent / degraded) builds no client and
    // fails on first use instead (backstop)
    if (configuration.missingConnectionProperties().isEmpty()) {
      this.authentication = Camunda8Authentication.of(adapterId, configuration, System::getenv);
      this.client = build();
    } else {
      // there is no cluster to authenticate against yet, and a SaaS adapter without its
      // client id cannot even build a provider - the missing connection keys are the
      // message that boot has to give
      this.authentication = null;
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

    final CamundaClientBuilder builder;
    if (configuration.getMode() == Camunda8AdapterConfiguration.Mode.SAAS) {
      log.info("Building Camunda 8 SaaS client for adapter '{}' (cluster '{}', region '{}', authentication {})",
          adapterId, configuration.getClusterId(), configuration.getRegion(), authentication.describe());
      builder = CamundaClient
          .newCloudClientBuilder()
          .withClusterId(configuration.getClusterId())
          .withClientId(configuration.getClientId())
          .withClientSecret(configuration.getClientSecret())
          .withRegion(configuration.getRegion());
      if (hasText(configuration.getTenantId())) {
        builder.defaultTenantId(configuration.getTenantId());
      }
    } else {
      log.info("Building Camunda 8 self-managed client for adapter '{}' (rest-address '{}', grpc-address '{}', "
          + "prefer-rest-over-grpc {}, authentication {})",
          adapterId, configuration.getRestAddress(), configuration.getGrpcAddress(),
          configuration.isPreferRestOverGrpc(), authentication.describe());
      builder = CamundaClient
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
    }

    applyExecutionModel(builder);
    applyWorkerDefaults(builder);
    applyTransportOptions(builder);
    applyAuthentication(builder);

    final var built = builder.build();
    reportSizing();
    reportEnvironmentOverrides(built);
    return built;

  }

  /**
   * The resolved execution model of this adapter instance - what the startup line names
   * and what {@link #virtualThreadExecutor} builds where the mode is virtual.
   */
  @Getter
  private final Camunda8ExecutionModel executionModel;

  /**
   * How this adapter instance authenticates: the resolved method, the provider handed to
   * the client, and the message said once when the cluster refuses a request.
   * <code>null</code> while the connection configuration is incomplete, where no client
   * was built either.
   */
  @Getter
  private final Camunda8Authentication authentication;

  /**
   * The executor handed to the client where the execution model is virtual, kept for the
   * tests which assert the bound; <code>null</code> in the platform-thread mode, where
   * the client builds its own pool.
   */
  @Getter
  private Camunda8VirtualThreadExecutor virtualThreadExecutor;

  private void applyExecutionModel(
      final CamundaClientBuilder builder) {

    if (!executionModel.virtual()) {
      // the client builds its own scheduled pool of this size, which on the 8.8 line
      // runs the handlers AND the polls of every worker - see Camunda8ExecutionModel
      builder.numJobWorkerExecutionThreads(executionModel.slots());
      return;
    }
    virtualThreadExecutor = new Camunda8VirtualThreadExecutor(adapterId, executionModel.slots());
    // which builder methods take it differs per release line, see Camunda8JobExecutors
    Camunda8JobExecutors.install(builder, virtualThreadExecutor);

  }

  private void applyWorkerDefaults(
      final CamundaClientBuilder builder) {

    // set on the CLIENT rather than on every worker: the worker builder inherits the
    // client's defaults, and an environment variable can then still overrule what was
    // configured - which is the escape hatch reportEnvironmentOverrides makes visible
    builder.defaultJobWorkerMaxJobsActive(configuration.resolvedMaxJobsActive(adapterId));
    if (configuration.getPollInterval() != null) {
      builder.defaultJobPollInterval(configuration.getPollInterval());
    }
    if (configuration.getRequestTimeout() != null) {
      builder.defaultRequestTimeout(configuration.getRequestTimeout());
    }
    if (configuration.getStreamEnabled() != null) {
      builder.defaultJobWorkerStreamEnabled(configuration.getStreamEnabled());
    }
    if (configuration.getMessageTimeToLive() != null) {
      builder.defaultMessageTimeToLive(configuration.getMessageTimeToLive());
    }

  }

  private void applyTransportOptions(
      final CamundaClientBuilder builder) {

    if (configuration.getMaxMessageSize() != null) {
      builder.maxMessageSize(configuration.getMaxMessageSize());
    }
    if (configuration.getKeepAlive() != null) {
      builder.keepAlive(configuration.getKeepAlive());
    }
    if (configuration.getMaxHttpConnections() != null) {
      builder.maxHttpConnections(configuration.getMaxHttpConnections());
    }
    if (hasText(configuration.getOverrideAuthority())) {
      builder.overrideAuthority(configuration.getOverrideAuthority());
    }
    if (hasText(configuration.getAuth().getCaCertificatePath())) {
      builder.caCertificatePath(configuration.getAuth().getCaCertificatePath());
    }

  }

  /**
   * Hands the client the credentials provider of this adapter instance - or, where the
   * method is <code>none</code> and the environment carries credentials, hands it none,
   * so the client keeps building the provider it always built from those variables (see
   * {@link Camunda8Authentication}).
   */
  private void applyAuthentication(
      final CamundaClientBuilder builder) {

    final var provider = authentication.providerFor(message -> log.warn("{}", message));
    if (provider != null) {
      builder.credentialsProvider(provider);
    }

  }

  /**
   * The four numbers which together are the sizing decision of an adapter instance, in
   * one line, because none of them appeared anywhere before: how the handlers run, how
   * many may run at once, how many jobs one worker holds while they do, and how long a
   * delivered job stays locked meanwhile.
   */
  private void reportSizing() {

    log.info(
        "Camunda8[{}]: {} run every worker of this adapter id, max-jobs-active {} per worker, "
            + "job-timeout {} by default. All workflow modules of this adapter share those {} execution "
            + "slots, and a handler holds one for its whole runtime",
        adapterId,
        executionModel.describe(),
        configuration.resolvedMaxJobsActive(adapterId),
        configuration.getJobTimeout() == null
            ? io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT
            : configuration.getJobTimeout(),
        executionModel.slots());

  }

  private void reportEnvironmentOverrides(
      final CamundaClient client) {

    final var credentialSelection = Camunda8EnvironmentOverrides
        .describeCredentialSelection(adapterId, authentication, System::getenv);
    if (credentialSelection != null) {
      log.warn("{}", credentialSelection);
    }
    final var overrides = Camunda8EnvironmentOverrides.detect(
        adapterId, configuration, client.getConfiguration(), System::getenv);
    if (overrides.isEmpty()) {
      return;
    }
    log.warn("{}", Camunda8EnvironmentOverrides.describe(adapterId, overrides));

  }

  private static boolean hasText(
      final String value) {

    return value != null && !value.isBlank();

  }

  /**
   * The other <code>camunda8</code> adapter ids addressing the SAME cluster (story 103),
   * told by {@link Camunda8ClientFactoryRegistry} at startup. Empty for the ordinary
   * application with one Camunda 8 adapter.
   * <p>
   * What it decides: keys are unique per cluster and not per tenant or prefix, so where
   * this list is not empty an awareness probe has to find out which scope a key belongs
   * to before it claims the task. That answer costs a query-API round trip, which is why
   * it is only paid where two ids can actually be confused.
   */
  private java.util.List<String> adapterIdsSharingTheCluster = java.util.List.of();

  /**
   * @param adapterIds The other adapter ids addressing this cluster
   */
  void sharesItsClusterWith(
      final java.util.List<String> adapterIds) {

    this.adapterIdsSharingTheCluster = java.util.List.copyOf(adapterIds);

  }

  /**
   * @return The other adapter ids addressing this cluster, empty where this id is alone
   */
  public java.util.List<String> getAdapterIdsSharingTheCluster() {

    return adapterIdsSharingTheCluster;

  }

  /**
   * @return Whether another <code>camunda8</code> adapter id addresses the same cluster
   */
  public boolean sharesItsCluster() {

    return !adapterIdsSharingTheCluster.isEmpty();

  }

  /**
   * How the factory stops the workers of one workflow module on a path which did not
   * reach {@code stopWorkflowProcessing} (story 102). Implemented by the deployment
   * service, which owns the workers and the drain.
   */
  @FunctionalInterface
  public interface WorkflowModuleShutdown {

    /**
     * Closes the workers of that workflow module and waits for it the way the ordinary
     * shutdown does. Has to be idempotent: the module may stop itself a moment later.
     */
    void stopWorkflowProcessing();

  }

  /**
   * The workflow modules of this adapter instance whose workers are open right now,
   * registered when processing starts and removed when it stops.
   * <p>
   * Story 90 promised that the workers of a module are closed before its client, and on
   * the ordinary path that is the order the platform's lifecycle produces. This map is
   * what makes the promise hold on EVERY path: whatever is left in it when the client
   * goes down is stopped here first, and the operator is told that a hook was missing.
   * Order matters more than it looks (story 102): an activation request which is parked
   * at the cluster when its client is closed stays parked, and a job created within the
   * request timeout afterwards waits for its lock instead of reaching the next worker.
   */
  private final java.util.Map<String, WorkflowModuleShutdown> openWorkflowModules = new java.util.LinkedHashMap<>();

  /**
   * Registers a workflow module whose workers are now open.
   *
   * @param workflowModuleId The workflow module
   * @param shutdown How to stop it if the client is closed before it stopped itself
   */
  public synchronized void workflowModuleStarted(
      final String workflowModuleId,
      final WorkflowModuleShutdown shutdown) {

    openWorkflowModules.put(workflowModuleId, shutdown);

  }

  /**
   * Deregisters a workflow module which stopped its own workers.
   *
   * @param workflowModuleId The workflow module
   */
  public synchronized void workflowModuleStopped(
      final String workflowModuleId) {

    openWorkflowModules.remove(workflowModuleId);

  }

  /**
   * @return The workflow modules of this adapter instance whose workers are open
   */
  public synchronized java.util.Set<String> getOpenWorkflowModules() {

    return java.util.Set.copyOf(openWorkflowModules.keySet());

  }

  @Override
  public synchronized void close() {

    closeWorkersOfModulesWhichDidNotStop();
    closed = true;
    if (client != null) {
      log.info("Closing Camunda 8 client of adapter '{}'", adapterId);
      client.close();
      client = null;
    }

  }

  /**
   * The backstop of story 102: a workflow module which never reached
   * {@code stopWorkflowProcessing} is stopped here, before the client goes down, and the
   * missing hook is named. The client is still open at this point, so the module's
   * handlers can finish and its workers can be released the ordinary way.
   */
  private void closeWorkersOfModulesWhichDidNotStop() {

    if (openWorkflowModules.isEmpty()) {
      return;
    }
    final var pending = java.util.List.copyOf(openWorkflowModules.entrySet());
    log.warn(
        "Camunda8[{}]: the client is being closed while the workers of the workflow module(s) '{}' are "
            + "still open, which means the shutdown of this application did not stop workflow processing. "
            + "Closing them now, before the client, so no activation request of theirs stays parked at the "
            + "cluster - a job created within '{}' of a client closed under its open workers waits for '{}' "
            + "before any worker sees it. Report this: on both supported platforms the lifecycle reaches "
            + "the adapter, so a path which does not is a defect of the wiring rather than of the "
            + "application",
        adapterId,
        String.join("', '", openWorkflowModules.keySet()),
        Camunda8AdapterConfiguration.propertyKey(adapterId, "request-timeout"),
        Camunda8AdapterConfiguration.propertyKey(adapterId, "job-timeout"));
    pending.forEach(entry -> {
      try {
        entry.getValue().stopWorkflowProcessing();
      } catch (final Exception e) {
        log.warn(
            "Camunda8[{}]: stopping the workers of workflow module '{}' failed while the client was being "
                + "closed. The client goes down now anyway",
            adapterId,
            entry.getKey(),
            e);
      }
    });
    openWorkflowModules.clear();

  }

}
