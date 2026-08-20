package io.vanillabp.camunda8.wiring;

import java.time.Duration;

/**
 * Resolves how long the cluster waits before it hands a FAILED job out again - implemented
 * by the platform modules on top of the adapter's configuration overlay with
 * most-specific-wins semantics across the four levels (task &gt; workflow &gt;
 * workflow-module &gt; adapter):
 *
 * <pre>
 * vanillabp.adapters.&lt;id&gt;.retry-backoff
 * vanillabp.workflow-modules.&lt;m&gt;.adapters.&lt;id&gt;.retry-backoff
 * vanillabp.workflow-modules.&lt;m&gt;.workflows.&lt;w&gt;.adapters.&lt;id&gt;.retry-backoff
 * vanillabp.workflow-modules.&lt;m&gt;.workflows.&lt;w&gt;.tasks.&lt;taskDefinition&gt;.adapters.&lt;id&gt;.retry-backoff
 * </pre>
 *
 * Unlike <code>job-timeout</code> this is resolved per COMMAND and not per worker: the
 * backoff travels with the fail command, so two BPMN processes served by one worker may
 * have different ones and nothing has to be aligned.
 */
@FunctionalInterface
public interface Camunda8RetryBackoffResolver {

  /**
   * How long the cluster waits before the next attempt where no level configures
   * anything: ten seconds.
   * <p>
   * <b>Why ten seconds.</b> Without a backoff the cluster hands a failed job out again as
   * fast as it can, so a handler failing on something which needs a moment - a locked row,
   * a service which is restarting, a rate limit - burns its three retries in less time
   * than the cause takes to pass, and the incident says nothing about what really
   * happened. Ten seconds is long enough for such a cause to clear and short enough that a
   * job with three retries is done deciding within half a minute rather than within
   * minutes. Size it against how long the dependency your handlers call needs to come
   * back, not against the length of the handler.
   */
  String DEFAULT_RETRY_BACKOFF_ISO = "PT10S";

  /**
   * {@link #DEFAULT_RETRY_BACKOFF_ISO} as a duration.
   */
  Duration DEFAULT_RETRY_BACKOFF = Duration.parse(DEFAULT_RETRY_BACKOFF_ISO);

  /**
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param taskDefinition The task definition (job type), or <code>null</code> for the
   *          workers which serve no task
   * @return The most specific configured retry backoff or {@link #DEFAULT_RETRY_BACKOFF}
   */
  Duration retryBackoffFor(
      String workflowModuleId,
      String bpmnProcessId,
      String taskDefinition);

  /**
   * Asks a resolver which may not be there - the handlers are built without one in tests,
   * and a resolver is free to answer nothing.
   *
   * @param resolver The resolver or <code>null</code>
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param taskDefinition The task definition, or <code>null</code>
   * @return The resolved backoff, never <code>null</code>
   */
  static Duration resolve(
      final Camunda8RetryBackoffResolver resolver,
      final String workflowModuleId,
      final String bpmnProcessId,
      final String taskDefinition) {

    if (resolver == null) {
      return DEFAULT_RETRY_BACKOFF;
    }
    final var resolved = resolver.retryBackoffFor(workflowModuleId, bpmnProcessId, taskDefinition);
    return resolved == null
        ? DEFAULT_RETRY_BACKOFF
        : resolved;

  }

}
