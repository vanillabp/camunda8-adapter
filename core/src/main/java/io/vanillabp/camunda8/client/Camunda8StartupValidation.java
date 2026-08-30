package io.vanillabp.camunda8.client;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Startup validation of one Camunda 8 adapter instance's connection configuration -
 * configuration is validated AT STARTUP, never first at runtime (a VanillaBP core
 * principle). Three states:
 * <ul>
 *   <li><b>complete</b> - nothing to report;</li>
 *   <li><b>absent</b> (no connection key at all) - the application still boots:
 *       a guiding WARN names the adapter id and the exact keys to add;</li>
 *   <li><b>inconsistent</b> (partially configured, e.g. <code>mode: saas</code>
 *       without <code>cluster-id</code>) - a genuine defect: the boot FAILS with a
 *       message naming the missing keys. Exception: an adapter that is NOWHERE
 *       first in any prioritized-adapters list (globally, per module, per workflow)
 *       may honor its <code>deployment-failure: warn</code> policy and degrade to a
 *       WARN - the migration scenario's old BPMS must not block the boot.</li>
 * </ul>
 * Messages name property KEYS only, never values - credentials
 * (<code>client-secret</code> etc.) are never echoed.
 */
public final class Camunda8StartupValidation {

  private Camunda8StartupValidation() {
  }

  /**
   * Validates the given adapter instance's connection configuration at startup.
   *
   * @param adapterId The adapter ID
   * @param configuration The (bound) connection configuration
   * @param firstPriorityAnywhere Whether the adapter is first priority at any level
   *          (see {@code MigrationAdapterProperties#isFirstPriorityAnywhere})
   * @param deploymentFailureWarn Whether the adapter's deployment-failure policy is
   *          <code>warn</code>
   * @param deliveryRetention How long a delivery record is kept
   *          (<code>vanillabp.delivery.retention</code>) - the bound the renewal window of
   *          open asynchronous tasks has to stay below
   * @param warnLogger Sink for guiding warnings (the application keeps booting)
   * @throws IllegalStateException If the configuration is inconsistent and the
   *           adapter must not degrade (first priority somewhere or policy
   *           <code>fail</code>)
   */
  public static void validateAtStartup(
      final String adapterId,
      final Camunda8AdapterConfiguration configuration,
      final boolean firstPriorityAnywhere,
      final boolean deploymentFailureWarn,
      final Duration deliveryRetention,
      final Consumer<String> warnLogger) {

    // how the adapter runs its workers is independent of whether it can reach a cluster,
    // and a number which cannot work is a typo rather than a migration scenario - so this
    // fails the boot for every adapter id, degraded or not
    configuration.validateWorkerConfiguration(adapterId);
    // and neither is how it proves who it is: a method whose credentials are incomplete
    // cannot be built at all, so it fails the boot naming the method and the keys
    configuration.validateAuthentication(adapterId);
    // and neither is how an open asynchronous task is kept alive: a window which cannot
    // work outlives the record answering its redelivery, which is silent at runtime
    configuration.validateAsyncTaskLockRenewal(adapterId, deliveryRetention);
    // and neither is how long a restart waits for the handlers in flight: a grace which
    // outlives the shutdown budget around it is never granted, and one nobody notices is
    // the reason a restart burns a retry per job
    configuration.validateShutdownGrace(adapterId, warnLogger);
    configuration.validateHealthTimeout(adapterId);
    // and neither is how long the cluster keeps a message this adapter publishes: zero
    // drops every one of them on arrival, and nothing at runtime says so
    configuration.validateMessageTimeToLive(adapterId);
    // and neither is how long the cluster waits before it hands a failed job out again: a
    // negative duration is a typo, and it decides something nobody watches
    configuration.validateRetryBackoff(adapterId);
    // and neither is the deadline every request of this adapter gets: a value which is too
    // short makes a healthy cluster answer too late, which reads like a network problem
    configuration.validateRequestTimeout(adapterId, warnLogger);
    // and neither is how long the start waits for a cluster which is not answering yet
    configuration.validateStartupWait(adapterId);

    if (configuration.isAbsent()) {
      warnLogger.accept(
          """
              Camunda 8 adapter '%s' has no connection configuration yet - the application boots, but \
              deploying BPMNs or starting workflows via this adapter will fail until configured. Add the \
              connection properties for this adapter instance:
                %s (self-managed | saas; default self-managed)
                %s (self-managed)
                %s, %s, %s, %s (saas)
              A self-managed cluster usually wants credentials as well, which is what '%s' is for; \
              without it the adapter sends none."""
              .formatted(
                  adapterId,
                  Camunda8AdapterConfiguration.propertyKey(adapterId, "mode"),
                  Camunda8AdapterConfiguration.propertyKey(adapterId, "rest-address"),
                  Camunda8AdapterConfiguration.propertyKey(adapterId, "cluster-id"),
                  Camunda8AdapterConfiguration.propertyKey(adapterId, "region"),
                  Camunda8AdapterConfiguration.propertyKey(adapterId, "client-id"),
                  Camunda8AdapterConfiguration.propertyKey(adapterId, "client-secret"),
                  Camunda8AdapterConfiguration.propertyKey(adapterId, "auth.method")));
      return;
    }

    final var missing = configuration.missingConnectionProperties();
    if (missing.isEmpty()) {
      return;
    }

    final var missingKeys = missing
        .stream()
        .map(key -> Camunda8AdapterConfiguration.propertyKey(adapterId, key))
        .collect(Collectors.joining("\n  "));
    if (!firstPriorityAnywhere && deploymentFailureWarn) {
      warnLogger.accept(
          """
              Camunda 8 adapter '%s' is configured inconsistently - these properties are missing:
                %s
              The adapter is nowhere first priority and its deployment-failure policy is 'warn', so the \
              application boots DEGRADED: any use of this adapter will fail until the properties are added."""
              .formatted(adapterId, missingKeys));
      return;
    }

    throw new IllegalStateException(
        """
            Camunda 8 adapter '%s' is configured inconsistently - these properties are missing:
              %s
            Add the missing properties. (An adapter that is nowhere first in a prioritized-adapters list \
            may instead set '%s' to 'warn' to boot degraded - e.g. the old BPMS during a migration.)"""
            .formatted(
                adapterId,
                missingKeys,
                Camunda8AdapterConfiguration.propertyKey(adapterId, "deployment-failure")));

  }

}
