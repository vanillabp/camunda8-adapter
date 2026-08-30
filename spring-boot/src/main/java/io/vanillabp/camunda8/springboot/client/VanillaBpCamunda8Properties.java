package io.vanillabp.camunda8.springboot.client;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import lombok.Getter;
import lombok.Setter;

/**
 * The Camunda 8 adapter's OVERLAY of the shared <code>vanillabp.*</code> configuration
 * tree: the adapter's connection settings live at the canonical per-adapter location
 * <code>vanillabp.adapters.&lt;id&gt;.*</code> (keys documented in
 * {@link Camunda8AdapterConfiguration}: <code>mode</code>, <code>rest-address</code>,
 * <code>grpc-address</code>, <code>prefer-rest-over-grpc</code>, <code>tenant-id</code>,
 * <code>cluster-id</code>, <code>region</code>, <code>client-id</code>,
 * <code>client-secret</code>, and the <code>auth.*</code> block of
 * {@link io.vanillabp.camunda8.client.Camunda8AuthConfiguration}). A second
 * {@code @ConfigurationProperties} class over the
 * same prefix coexists with the platform's binding of the core model; keys unknown to
 * either view are ignored by the JavaBean binding.
 * <p>
 * The adapter-id set is NEVER derived from this overlay map - it always comes from the
 * platform's core properties ({@code adapterTypes()} filtered by type
 * {@code camunda8}); the overlay is a per-known-id lookup only (environment-variable
 * overrides can materialize phantom map entries in the overlay).
 */
@ConfigurationProperties("vanillabp")
@Getter
@Setter
public class VanillaBpCamunda8Properties {

  /**
   * The adapter sections of the shared tree, keyed by adapter ID - only the
   * Camunda 8 connection keys are modeled here (bound directly onto the
   * platform-neutral {@link Camunda8AdapterConfiguration}).
   */
  private Map<String, Camunda8AdapterConfiguration> adapters = Map.of();

  /**
   * The workflow-module sections of the shared tree - the overlay mirrors the
   * levels of the most-specific-wins resolution of scope-specific adapter keys
   * (task &gt; workflow &gt; workflow-module &gt; adapter), currently:
   * <code>job-timeout</code>, <code>retry-backoff</code> and
   * <code>fetch-variables</code>. <code>message-time-to-live</code> resolves the same way
   * with a MESSAGE as its most specific level instead of a task.
   */
  private Map<String, ModuleOverlay> workflowModules = Map.of();

  /**
   * Resolves the job timeout for a task with most-specific-wins semantics across
   * the four levels; falls back to the adapter-level value and finally the
   * default.
   */
  public java.time.Duration jobTimeoutFor(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String taskDefinition,
      final String adapterId) {

    final var scoped = scopedKeysMostSpecificFirst(workflowModuleId, bpmnProcessId, taskDefinition, adapterId)
        .map(Camunda8ScopedKeys::getJobTimeout)
        .filter(java.util.Objects::nonNull)
        .findFirst();
    if (scoped.isPresent()) {
      return scoped.get();
    }
    final var adapter = adapters.get(adapterId);
    return (adapter != null) && (adapter.getJobTimeout() != null)
        ? adapter.getJobTimeout()
        : io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT;

  }

  /**
   * Resolves the backoff of a FAILED job with the same most-specific-wins semantics;
   * falls back to the adapter-level value and finally the default of ten seconds.
   */
  public java.time.Duration retryBackoffFor(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String taskDefinition,
      final String adapterId) {

    return configuredRetryBackoffFor(workflowModuleId, bpmnProcessId, taskDefinition, adapterId)
        .duration();

  }

  /**
   * The same answer, plus the one thing a BPMN model can argue with: whether the TASK
   * level is where the value comes from. A task header
   * {@value io.vanillabp.camunda8.wiring.Camunda8RetryBackoffHeader#HEADER_NAME} says as
   * much about one task as that level does and beats every level above it.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param taskDefinition The task definition (job type)
   * @param adapterId The adapter ID
   * @return The most specific configured backoff and the level it was found at
   */
  public io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver.Configured configuredRetryBackoffFor(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String taskDefinition,
      final String adapterId) {

    final var perTask = taskLevelKeys(workflowModuleId, bpmnProcessId, taskDefinition, adapterId);
    if ((perTask != null) && (perTask.getRetryBackoff() != null)) {
      return new io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver.Configured(
          perTask.getRetryBackoff(), true);
    }
    final var scoped = scopedKeysMostSpecificFirst(workflowModuleId, bpmnProcessId, taskDefinition, adapterId)
        .map(Camunda8ScopedKeys::getRetryBackoff)
        .filter(java.util.Objects::nonNull)
        .findFirst();
    if (scoped.isPresent()) {
      return new io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver.Configured(scoped.get(), false);
    }
    final var adapter = adapters.get(adapterId);
    return new io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver.Configured(
        adapter != null
            ? adapter.resolvedRetryBackoff()
            : io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver.DEFAULT_RETRY_BACKOFF, false);

  }

  /**
   * The <code>adapters.&lt;id&gt;</code> section of the TASK level alone, or
   * <code>null</code> where the configuration says nothing about this task.
   */
  private Camunda8ScopedKeys taskLevelKeys(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String taskDefinition,
      final String adapterId) {

    final var module = workflowModuleId != null
        ? workflowModules.get(workflowModuleId)
        : null;
    final var workflow = (module != null) && (bpmnProcessId != null)
        ? module.getWorkflows().get(bpmnProcessId)
        : null;
    final var task = (workflow != null) && (taskDefinition != null)
        ? workflow.getTasks().get(taskDefinition)
        : null;
    return task != null
        ? task.getAdapters().get(adapterId)
        : null;

  }

  /**
   * Resolves the time-to-live of a published message with most-specific-wins semantics,
   * where the MOST specific level is the message rather than a task; falls back to the
   * adapter-level value and finally to <code>null</code>, which leaves the command alone
   * and lets the client's own default apply.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param messageName The BPMN message name as the application wrote it
   * @param adapterId The adapter ID
   * @return The most specific configured time-to-live or <code>null</code>
   */
  public java.time.Duration messageTimeToLiveFor(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String messageName,
      final String adapterId) {

    final var scoped = messageScopedKeysMostSpecificFirst(
        workflowModuleId, bpmnProcessId, messageName, adapterId)
        .map(Camunda8ScopedKeys::getMessageTimeToLive)
        .filter(java.util.Objects::nonNull)
        .findFirst();
    if (scoped.isPresent()) {
      return scoped.get();
    }
    final var adapter = adapters.get(adapterId);
    return adapter == null
        ? null
        : adapter.getMessageTimeToLive();

  }

  /**
   * The <code>adapters.&lt;id&gt;</code> sections of the three levels below the adapter for
   * a MESSAGE, most specific first. Separate from
   * {@link #scopedKeysMostSpecificFirst} because the most specific level is a different
   * map: a message is not a task, and giving it the task level would make an override
   * meant for one apply to the other.
   */
  private java.util.stream.Stream<Camunda8ScopedKeys> messageScopedKeysMostSpecificFirst(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String messageName,
      final String adapterId) {

    final var module = workflowModuleId != null
        ? workflowModules.get(workflowModuleId)
        : null;
    final var workflow = (module != null) && (bpmnProcessId != null)
        ? module.getWorkflows().get(bpmnProcessId)
        : null;
    final var message = (workflow != null) && (messageName != null)
        ? workflow.getMessages().get(messageName)
        : null;

    final var levelsMostSpecificFirst = new java.util.LinkedList<Map<String, Camunda8ScopedKeys>>();
    if (message != null) {
      levelsMostSpecificFirst.add(message.getAdapters());
    }
    if (workflow != null) {
      levelsMostSpecificFirst.add(workflow.getAdapters());
    }
    if (module != null) {
      levelsMostSpecificFirst.add(module.getAdapters());
    }
    return levelsMostSpecificFirst
        .stream()
        .map(level -> level.get(adapterId))
        .filter(java.util.Objects::nonNull);

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
  public io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Mode fetchVariablesFor(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String taskDefinition,
      final String adapterId) {

    final var scoped = scopedKeysMostSpecificFirst(workflowModuleId, bpmnProcessId, taskDefinition, adapterId)
        .map(Camunda8ScopedKeys::getFetchVariables)
        .filter(java.util.Objects::nonNull)
        .findFirst();
    if (scoped.isPresent()) {
      return scoped.get();
    }
    final var adapter = adapters.get(adapterId);
    return adapter != null
        ? adapter.resolvedFetchVariables()
        : io.vanillabp.camunda8.wiring.Camunda8FetchVariablesResolver.DEFAULT_FETCH_VARIABLES;

  }

  /**
   * The <code>adapters.&lt;id&gt;</code> sections of the three levels below the adapter,
   * most specific first - what every scope-specific key is resolved through.
   */
  private java.util.stream.Stream<Camunda8ScopedKeys> scopedKeysMostSpecificFirst(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String taskDefinition,
      final String adapterId) {

    final var module = workflowModuleId != null
        ? workflowModules.get(workflowModuleId)
        : null;
    final var workflow = (module != null) && (bpmnProcessId != null)
        ? module.getWorkflows().get(bpmnProcessId)
        : null;
    final var task = (workflow != null) && (taskDefinition != null)
        ? workflow.getTasks().get(taskDefinition)
        : null;

    final var levelsMostSpecificFirst = new java.util.LinkedList<Map<String, Camunda8ScopedKeys>>();
    if (task != null) {
      levelsMostSpecificFirst.add(task.getAdapters());
    }
    if (workflow != null) {
      levelsMostSpecificFirst.add(workflow.getAdapters());
    }
    if (module != null) {
      levelsMostSpecificFirst.add(module.getAdapters());
    }
    return levelsMostSpecificFirst
        .stream()
        .map(level -> level.get(adapterId))
        .filter(java.util.Objects::nonNull);

  }

  /**
   * The scope-specific Camunda 8 keys of one <code>adapters.&lt;id&gt;</code>
   * section below a workflow-module/workflow/task level.
   */
  @Getter
  @Setter
  public static class Camunda8ScopedKeys {

    private java.time.Duration jobTimeout;

    private java.time.Duration retryBackoff;

    private io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Mode fetchVariables;

    private java.time.Duration messageTimeToLive;

  }

  /**
   * The Camunda 8 adapter's view of one workflow-module section.
   */
  @Getter
  @Setter
  public static class ModuleOverlay {

    private Map<String, Camunda8ScopedKeys> adapters = Map.of();

    private Map<String, WorkflowOverlay> workflows = Map.of();

  }

  /**
   * The Camunda 8 adapter's view of one workflow section.
   */
  @Getter
  @Setter
  public static class WorkflowOverlay {

    private Map<String, Camunda8ScopedKeys> adapters = Map.of();

    private Map<String, TaskOverlay> tasks = Map.of();

    /**
     * The message sections of this workflow - the most specific level of the keys which
     * are about a MESSAGE rather than about a task.
     */
    private Map<String, MessageOverlay> messages = Map.of();

  }

  /**
   * The Camunda 8 adapter's view of one message section - the most specific level for a
   * key about a published message.
   */
  @Getter
  @Setter
  public static class MessageOverlay {

    private Map<String, Camunda8ScopedKeys> adapters = Map.of();

  }

  /**
   * The Camunda 8 adapter's view of one task section - the MOST specific level.
   */
  @Getter
  @Setter
  public static class TaskOverlay {

    private Map<String, Camunda8ScopedKeys> adapters = Map.of();

  }

}
