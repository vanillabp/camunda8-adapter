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
   * <code>job-timeout</code>.
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
    final var scoped = levelsMostSpecificFirst
        .stream()
        .map(level -> level.get(adapterId))
        .filter(java.util.Objects::nonNull)
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
     * The worker's job timeout (lock duration) - adapter-level base of the
     * most-specific-wins resolution.
     *
     * @return The job timeout
     */
    Optional<java.time.Duration> jobTimeout();

    /**
     * How long a <code>&#64;TaskId</code> job stays dormant awaiting its
     * asynchronous completion.
     *
     * @return The dormancy duration
     */
    Optional<java.time.Duration> asyncTaskTimeout();

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
