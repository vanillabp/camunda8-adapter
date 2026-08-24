package io.vanillabp.camunda8.quarkus.runtime;

import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;

/**
 * The Camunda 8 adapter's OVERLAY of the shared <code>vanillabp.*</code> configuration
 * tree: the adapter's connection settings live at the canonical per-adapter location
 * <code>vanillabp.adapters.&lt;id&gt;.*</code> (see
 * {@link Camunda8AdapterConfiguration}). A second RUN_TIME {@code @ConfigMapping} over
 * the same prefix coexists with the platform's mapping; since the platform dropped the
 * blanket {@code withMappingIgnore}, this overlay doubles as the unknown-key
 * validation coverage for the adapter's keys.
 * <p>
 * The adapter-id set is NEVER derived from this overlay map - it always comes from the
 * platform's core properties ({@code adapterTypes()} filtered by type
 * {@code camunda8}); the overlay is a per-known-id lookup only.
 */
@StaticInitSafe
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
@ConfigMapping(prefix = "vanillabp")
public interface VanillaBpCamunda8Properties {

  /**
   * The adapter sections of the shared tree, keyed by adapter ID - only the
   * Camunda 8 connection keys are modeled here.
   */
  Map<String, Camunda8AdapterKeys> adapters();

  /**
   * The workflow-module sections of the shared tree - the overlay mirrors the
   * levels of the most-specific-wins resolution of scope-specific adapter keys
   * (task &gt; workflow &gt; workflow-module &gt; adapter), currently:
   * <code>job-timeout</code>, <code>retry-backoff</code> and
   * <code>fetch-variables</code>.
   *
   * @return The workflow-module sections, keyed by workflow module ID
   */
  Map<String, ModuleOverlay> workflowModules();

  /**
   * Resolves the job timeout for a task with most-specific-wins semantics across
   * the four levels; falls back to the adapter-level value and finally the
   * default.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param taskDefinition The task definition (job type)
   * @param adapterId The adapter ID
   * @return The most specific configured job timeout or the default
   */
  default java.time.Duration jobTimeoutFor(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String taskDefinition,
      final String adapterId) {

    final var scoped = scopedKeysMostSpecificFirst(workflowModuleId, bpmnProcessId, taskDefinition, adapterId)
        .map(Camunda8ScopedKeys::jobTimeout)
        .flatMap(Optional::stream)
        .findFirst();
    if (scoped.isPresent()) {
      return scoped.get();
    }
    final var adapter = adapters().get(adapterId);
    return adapter != null
        ? adapter
            .jobTimeout()
            .orElse(io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT)
        : io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT;

  }

  /**
   * Resolves the backoff of a FAILED job with the same most-specific-wins semantics;
   * falls back to the adapter-level value and finally the default of ten seconds.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param taskDefinition The task definition (job type)
   * @param adapterId The adapter ID
   * @return The most specific configured backoff or the default
   */
  default java.time.Duration retryBackoffFor(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String taskDefinition,
      final String adapterId) {

    final var scoped = scopedKeysMostSpecificFirst(workflowModuleId, bpmnProcessId, taskDefinition, adapterId)
        .map(Camunda8ScopedKeys::retryBackoff)
        .flatMap(Optional::stream)
        .findFirst();
    if (scoped.isPresent()) {
      return scoped.get();
    }
    final var adapter = adapters().get(adapterId);
    return adapter != null
        ? adapter
            .retryBackoff()
            .orElse(io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver.DEFAULT_RETRY_BACKOFF)
        : io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver.DEFAULT_RETRY_BACKOFF;

  }

  /**
   * Resolves whether a worker fetches the DERIVED variables or all of them with the same
   * most-specific-wins semantics; falls back to the adapter-level value and finally the
   * default {@code derived}.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param taskDefinition The task definition (job type)
   * @param adapterId The adapter ID
   * @return The most specific configured mode or the default
   */
  default io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Mode fetchVariablesFor(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String taskDefinition,
      final String adapterId) {

    final var scoped = scopedKeysMostSpecificFirst(workflowModuleId, bpmnProcessId, taskDefinition, adapterId)
        .map(Camunda8ScopedKeys::fetchVariables)
        .flatMap(Optional::stream)
        .findFirst();
    if (scoped.isPresent()) {
      return scoped.get();
    }
    final var adapter = adapters().get(adapterId);
    return adapter != null
        ? adapter
            .fetchVariables()
            .orElse(io.vanillabp.camunda8.wiring.Camunda8FetchVariablesResolver.DEFAULT_FETCH_VARIABLES)
        : io.vanillabp.camunda8.wiring.Camunda8FetchVariablesResolver.DEFAULT_FETCH_VARIABLES;

  }

  /**
   * The <code>adapters.&lt;id&gt;</code> sections of the three levels below the adapter,
   * most specific first - what every scope-specific key is resolved through.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param taskDefinition The task definition (job type)
   * @param adapterId The adapter ID
   * @return The sections which exist, most specific first
   */
  private java.util.stream.Stream<Camunda8ScopedKeys> scopedKeysMostSpecificFirst(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String taskDefinition,
      final String adapterId) {

    final var module = workflowModuleId != null
        ? workflowModules().get(workflowModuleId)
        : null;
    final var workflow = (module != null) && (bpmnProcessId != null)
        ? module.workflows().get(bpmnProcessId)
        : null;
    final var task = (workflow != null) && (taskDefinition != null)
        ? workflow.tasks().get(taskDefinition)
        : null;

    final var levelsMostSpecificFirst = new java.util.LinkedList<Map<String, Camunda8ScopedKeys>>();
    if (task != null) {
      levelsMostSpecificFirst.add(task.adapters());
    }
    if (workflow != null) {
      levelsMostSpecificFirst.add(workflow.adapters());
    }
    if (module != null) {
      levelsMostSpecificFirst.add(module.adapters());
    }
    return levelsMostSpecificFirst
        .stream()
        .map(level -> level.get(adapterId))
        .filter(java.util.Objects::nonNull);

  }

  /**
   * The Camunda 8 connection keys of one <code>vanillabp.adapters.&lt;id&gt;</code>
   * section (see {@link Camunda8AdapterConfiguration} for the semantics).
   */
  interface Camunda8AdapterKeys {

    /**
     * Connection mode: <code>self-managed</code> (default) or <code>saas</code>.
     */
    Optional<Camunda8AdapterConfiguration.Mode> mode();

    /**
     * REST API address of a self-managed cluster (e.g.
     * <code>http://localhost:8080</code>).
     */
    Optional<String> restAddress();

    /**
     * gRPC address of a self-managed cluster (required when
     * <code>prefer-rest-over-grpc</code> is <code>false</code>).
     */
    Optional<String> grpcAddress();

    /**
     * Whether the client uses the REST API (recommended, default) or gRPC for its
     * commands.
     */
    Optional<Boolean> preferRestOverGrpc();

    /**
     * The Camunda 8 multi-tenancy tenant (optional, both modes).
     */
    Optional<String> tenantId();

    /**
     * SaaS cluster ID.
     */
    Optional<String> clusterId();

    /**
     * SaaS region.
     */
    Optional<String> region();

    /**
     * SaaS OAuth client ID.
     */
    Optional<String> clientId();

    /**
     * SaaS OAuth client secret.
     */
    Optional<String> clientSecret();

    /**
     * OPTIONAL acknowledgement that the application's identifiers are unique across
     * all of its workflow modules - it silences the WARN logged while the
     * name-clash-avoidance mode <code>none</code> applies. Default
     * <code>false</code>.
     *
     * @return Whether unscoped identifiers are accepted deliberately
     */
    Optional<Boolean> acceptUnscopedIdentifiers();

    /**
     * The worker's job timeout (lock duration) - adapter-level base of the
     * most-specific-wins resolution.
     *
     * @return The job timeout
     */
    Optional<java.time.Duration> jobTimeout();

    /**
     * How long the cluster waits before it hands a FAILED job out again - adapter-level
     * base of the most-specific-wins resolution. Default: ten seconds.
     *
     * @return The backoff of a failed job
     */
    Optional<java.time.Duration> retryBackoff();

    /**
     * Whether the workers of this adapter instance ask the cluster for the variables the
     * adapter derived or for all of them - adapter-level base of the most-specific-wins
     * resolution. Default: <code>derived</code>.
     *
     * @return The mode
     */
    Optional<io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Mode> fetchVariables();

    /**
     * The window the lock of a job left open by a <code>&#64;TaskId</code> handler is
     * renewed in.
     *
     * @return The renewal window
     */
    Optional<java.time.Duration> asyncTaskLockRenewal();

    /**
     * The key which used to carry a dormancy horizon of days. Bound only so the boot can
     * REJECT it with a message naming its successor.
     *
     * @return The removed key's value, if somebody still configures it
     */
    Optional<java.time.Duration> asyncTaskTimeout();

    /**
     * How long the shutdown of a workflow module waits for the handlers this adapter has
     * in flight before the client is closed under them. Default: 20 seconds.
     *
     * @return The grace period
     */
    Optional<java.time.Duration> shutdownGrace();

    /**
     * How long the health check waits for the cluster to answer its topology request,
     * <code>PT0S</code> switching the check off.
     *
     * @return The timeout
     */
    Optional<java.time.Duration> healthTimeout();

    /**
     * What this adapter does with a task the core reports as older than
     * <code>vanillabp.delivery.max-task-age</code>.
     *
     * @return The action
     */
    Optional<io.vanillabp.camunda8.client.Camunda8AdapterConfiguration.AsyncTaskMaxAgeAction> asyncTaskMaxAgeAction();

    /**
     * How long a workflow this cluster holds may stay invisible to the query API
     * the awareness probe searches (the exporter feeding it runs behind the
     * engine). Zero switches the waiting off. Default: 10 seconds.
     *
     * @return The visibility window
     */
    Optional<java.time.Duration> workflowVisibilityTimeout();

    /**
     * How this adapter instance runs what it delivers: a positive number of platform
     * threads, or the literal <code>virtual</code>. Default: four platform threads.
     *
     * @return The execution model
     */
    Optional<String> workerThreads();

    /**
     * How many handlers may run at the same time while <code>worker-threads</code> is
     * <code>virtual</code>. Default: the number the platform-thread mode would use.
     *
     * @return The bound of the virtual-thread executor
     */
    Optional<Integer> workerThreadsBound();

    /**
     * How many jobs one worker may hold at the same time. Default: eight per execution
     * slot, capped at the client's 32.
     *
     * @return The worker's job capacity
     */
    Optional<Integer> maxJobsActive();

    /**
     * How long a worker waits between two activation requests. Default: the client's
     * 100 milliseconds.
     *
     * @return The poll interval
     */
    Optional<java.time.Duration> pollInterval();

    /**
     * How long a request to the cluster may take, which for an activation request is
     * also the long-polling window. Default: the client's 10 seconds.
     *
     * @return The request timeout
     */
    Optional<java.time.Duration> requestTimeout();

    /**
     * Whether the cluster pushes jobs to the workers instead of only answering their
     * polls. Default: the client's <code>false</code>.
     *
     * @return Whether job streaming is switched on
     */
    Optional<Boolean> streamEnabled();

    /**
     * How long a job stream stays open before the client re-opens it. Default: the
     * client's.
     *
     * @return The stream timeout
     */
    Optional<java.time.Duration> streamTimeout();

    /**
     * How long the cluster buffers a published message waiting for a subscription,
     * which is also the window a message id deduplicates in. Default: the client's one
     * hour.
     *
     * @return The message time-to-live
     */
    Optional<java.time.Duration> messageTimeToLive();

    /**
     * The client's maximum inbound message size in bytes. Default: the client's.
     *
     * @return The maximum message size
     */
    Optional<Integer> maxMessageSize();

    /**
     * The keep-alive interval of the client's connections. Default: the client's.
     *
     * @return The keep-alive interval
     */
    Optional<java.time.Duration> keepAlive();

    /**
     * How many HTTP connections the REST transport may open. Default: the client's.
     *
     * @return The connection limit
     */
    Optional<Integer> maxHttpConnections();

    /**
     * The authority the TLS certificate is verified against. Default: none.
     *
     * @return The overridden authority
     */
    Optional<String> overrideAuthority();

    /**
     * How this adapter instance authenticates against its cluster.
     *
     * @return The authentication block
     */
    AuthKeys auth();

  }

  /**
   * The <code>vanillabp.adapters.&lt;id&gt;.auth.*</code> keys (see
   * {@link io.vanillabp.camunda8.client.Camunda8AuthConfiguration} for the semantics and
   * the defaults the Camunda client brings).
   */
  interface AuthKeys {

    /**
     * <code>none</code>, <code>basic</code> or <code>oidc</code>. Absent means the
     * method is detected from the keys which are set, and the detection is logged.
     *
     * @return The method
     */
    Optional<io.vanillabp.camunda8.client.Camunda8AuthConfiguration.Method> method();

    /**
     * The user name of the method <code>basic</code>.
     *
     * @return The user name
     */
    Optional<String> username();

    /**
     * The password of the method <code>basic</code>.
     *
     * @return The password
     */
    Optional<String> password();

    /**
     * The OIDC client requesting the token.
     *
     * @return The client id
     */
    Optional<String> clientId();

    /**
     * The secret of the OIDC client.
     *
     * @return The client secret
     */
    Optional<String> clientSecret();

    /**
     * The token endpoint of the identity provider.
     *
     * @return The authorization server URL
     */
    Optional<String> authorizationServerUrl();

    /**
     * The audience the cluster expects in the token.
     *
     * @return The audience
     */
    Optional<String> audience();

    /**
     * The scopes requested with the token.
     *
     * @return The scope
     */
    Optional<String> scope();

    /**
     * Where the client caches the tokens it fetched. Default: the client's
     * <code>${user.home}/.camunda/credentials</code>.
     *
     * @return The cache file
     */
    Optional<String> credentialsCachePath();

    /**
     * How long connecting to the authorization server may take. Default: the client's 5
     * seconds.
     *
     * @return The connect timeout
     */
    Optional<java.time.Duration> connectTimeout();

    /**
     * How long reading the token response may take. Default: the client's 5 seconds.
     *
     * @return The read timeout
     */
    Optional<java.time.Duration> readTimeout();

    /**
     * The keystore holding the client certificate the AUTHORIZATION SERVER asks for.
     *
     * @return The keystore file
     */
    Optional<String> keystorePath();

    /**
     * The password of that keystore.
     *
     * @return The keystore password
     */
    Optional<String> keystorePassword();

    /**
     * The password of the key inside that keystore.
     *
     * @return The key password
     */
    Optional<String> keystoreKeyPassword();

    /**
     * The truststore the AUTHORIZATION SERVER's certificate is verified against.
     *
     * @return The truststore file
     */
    Optional<String> truststorePath();

    /**
     * The password of that truststore.
     *
     * @return The truststore password
     */
    Optional<String> truststorePassword();

    /**
     * The certificate authority the CLUSTER's TLS certificate is verified against.
     *
     * @return The certificate file
     */
    Optional<String> caCertificatePath();

  }


  /**
   * The scope-specific Camunda 8 keys of one <code>adapters.&lt;id&gt;</code>
   * section below a workflow-module/workflow/task level.
   */
  interface Camunda8ScopedKeys {

    /**
     * The worker's job timeout (lock duration) at this level.
     *
     * @return The job timeout
     */
    Optional<java.time.Duration> jobTimeout();

    /**
     * How long the cluster waits before it hands a FAILED job out again, at this level.
     *
     * @return The backoff of a failed job
     */
    Optional<java.time.Duration> retryBackoff();

    /**
     * Whether a worker fetches the derived variables or all of them, at this level.
     *
     * @return The mode
     */
    Optional<io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Mode> fetchVariables();

  }

  /**
   * The Camunda 8 adapter's view of one workflow-module section.
   */
  interface ModuleOverlay {

    /**
     * The module-level adapter sections, keyed by adapter ID.
     *
     * @return The adapter sections
     */
    Map<String, Camunda8ScopedKeys> adapters();

    /**
     * The workflow sections of the module, keyed by BPMN process ID.
     *
     * @return The workflow sections
     */
    Map<String, WorkflowOverlay> workflows();

  }

  /**
   * The Camunda 8 adapter's view of one workflow section.
   */
  interface WorkflowOverlay {

    /**
     * The workflow-level adapter sections, keyed by adapter ID.
     *
     * @return The adapter sections
     */
    Map<String, Camunda8ScopedKeys> adapters();

    /**
     * The task sections of the workflow, keyed by task definition.
     *
     * @return The task sections
     */
    Map<String, TaskOverlay> tasks();

  }

  /**
   * The Camunda 8 adapter's view of one task section - the MOST specific level.
   */
  interface TaskOverlay {

    /**
     * The task-level adapter sections, keyed by adapter ID.
     *
     * @return The adapter sections
     */
    Map<String, Camunda8ScopedKeys> adapters();

  }

}
