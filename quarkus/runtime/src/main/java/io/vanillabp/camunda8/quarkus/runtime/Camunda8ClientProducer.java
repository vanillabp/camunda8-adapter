package io.vanillabp.camunda8.quarkus.runtime;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import io.smallrye.config.SmallRyeConfig;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8AuthConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.client.Camunda8StartupValidation;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.integration.adapter.migration.config.DeploymentFailurePolicy;
import io.vanillabp.integration.adapter.migration.config.MigrationAdapterProperties;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

/**
 * Produces the {@link Camunda8ClientFactoryRegistry} (one lazily built
 * {@code CamundaClient} per Camunda 8 adapter ID) from the canonical per-adapter
 * configuration <code>vanillabp.adapters.&lt;id&gt;.*</code>, bound by the adapter's
 * overlay mapping ({@link VanillaBpCamunda8Properties}) - property names are NEVER
 * parsed programmatically (that pattern is lossy for environment-variable sources).
 * <p>
 * The adapter-id set comes from the platform's core properties (adapter ids of type
 * {@code camunda8}); the overlay map is used as a per-known-id lookup only. Every
 * instance's connection configuration is VALIDATED AT STARTUP (the
 * {@link Camunda8StartupObserver} forces this producer on {@code StartupEvent}):
 * an entirely unconfigured adapter boots with a guiding warning, an inconsistently
 * configured one fails the boot (unless it is nowhere first priority and its
 * deployment-failure policy is 'warn'); clients of completely configured adapters
 * are built eagerly. The registry is closed on application shutdown via the
 * {@link #close(Camunda8ClientFactoryRegistry) disposer}, closing all clients.
 */
@ApplicationScoped
public class Camunda8ClientProducer {

  private static final Logger log = Logger.getLogger(Camunda8ClientProducer.class);

  @Produces
  @Singleton
  public Camunda8ClientFactoryRegistry camunda8ClientFactoryRegistry(
      final MigrationAdapterProperties properties) {

    final var overlay = ConfigProvider
        .getConfig()
        .unwrap(SmallRyeConfig.class)
        .getConfigMapping(VanillaBpCamunda8Properties.class);

    final Map<String, Camunda8AdapterConfiguration> configurations = new HashMap<>();
    properties
        .adapterTypes()
        .entrySet()
        .stream()
        .filter(adapter -> Camunda8DeploymentService.ADAPTER_TYPE.equals(adapter.getValue()))
        .map(Map.Entry::getKey)
        .forEach(adapterId -> {
          final var configuration = toConfiguration(overlay.adapters().get(adapterId));
          Camunda8StartupValidation.validateAtStartup(
              adapterId,
              configuration,
              properties.isFirstPriorityAnywhere(adapterId),
              properties.getDeploymentFailureFor(
                  adapterId) == DeploymentFailurePolicy.WARN,
              properties.resolvedDeliveryRetention(),
              log::warn);
          configurations.put(adapterId, configuration);
        });

    return new Camunda8ClientFactoryRegistry(configurations);

  }

  private static Camunda8AdapterConfiguration toConfiguration(
      final VanillaBpCamunda8Properties.Camunda8AdapterKeys keys) {

    final var configuration = new Camunda8AdapterConfiguration();
    if (keys == null) {
      return configuration;
    }
    keys.mode().ifPresent(configuration::setMode);
    keys.restAddress().ifPresent(configuration::setRestAddress);
    keys.grpcAddress().ifPresent(configuration::setGrpcAddress);
    keys.preferRestOverGrpc().ifPresent(configuration::setPreferRestOverGrpc);
    keys.tenantId().ifPresent(configuration::setTenantId);
    keys.acceptUnscopedIdentifiers().ifPresent(configuration::setAcceptUnscopedIdentifiers);
    keys.clusterId().ifPresent(configuration::setClusterId);
    keys.region().ifPresent(configuration::setRegion);
    keys.clientId().ifPresent(configuration::setClientId);
    keys.clientSecret().ifPresent(configuration::setClientSecret);
    keys.jobTimeout().ifPresent(configuration::setJobTimeout);
    keys.retryBackoff().ifPresent(configuration::setRetryBackoff);
    keys.fetchVariables().ifPresent(configuration::setFetchVariables);
    keys.asyncTaskLockRenewal().ifPresent(configuration::setAsyncTaskLockRenewal);
    // bound only so the removed key can be REJECTED with a guiding message instead of
    // SmallRye's "does not map to any root"
    keys.asyncTaskTimeout().ifPresent(configuration::setAsyncTaskTimeout);
    keys.asyncTaskMaxAgeAction().ifPresent(configuration::setAsyncTaskMaxAgeAction);
    keys.shutdownGrace().ifPresent(configuration::setShutdownGrace);
    keys.healthTimeout().ifPresent(configuration::setHealthTimeout);
    keys.startupWait().ifPresent(configuration::setStartupWait);
    keys.workflowVisibilityTimeout().ifPresent(configuration::setWorkflowVisibilityTimeout);
    keys.workerThreads().ifPresent(configuration::setWorkerThreads);
    keys.workerThreadsBound().ifPresent(configuration::setWorkerThreadsBound);
    keys.maxJobsActive().ifPresent(configuration::setMaxJobsActive);
    keys.pollInterval().ifPresent(configuration::setPollInterval);
    keys.requestTimeout().ifPresent(configuration::setRequestTimeout);
    keys.streamEnabled().ifPresent(configuration::setStreamEnabled);
    keys.streamTimeout().ifPresent(configuration::setStreamTimeout);
    keys.messageTimeToLive().ifPresent(configuration::setMessageTimeToLive);
    keys.maxMessageSize().ifPresent(configuration::setMaxMessageSize);
    keys.keepAlive().ifPresent(configuration::setKeepAlive);
    keys.maxHttpConnections().ifPresent(configuration::setMaxHttpConnections);
    keys.overrideAuthority().ifPresent(configuration::setOverrideAuthority);
    toAuthConfiguration(keys.auth(), configuration.getAuth());
    return configuration;

  }

  private static void toAuthConfiguration(
      final VanillaBpCamunda8Properties.AuthKeys keys,
      final Camunda8AuthConfiguration auth) {

    if (keys == null) {
      return;
    }
    keys.method().ifPresent(auth::setMethod);
    keys.username().ifPresent(auth::setUsername);
    keys.password().ifPresent(auth::setPassword);
    keys.clientId().ifPresent(auth::setClientId);
    keys.clientSecret().ifPresent(auth::setClientSecret);
    keys.authorizationServerUrl().ifPresent(auth::setAuthorizationServerUrl);
    keys.audience().ifPresent(auth::setAudience);
    keys.scope().ifPresent(auth::setScope);
    keys.credentialsCachePath().ifPresent(auth::setCredentialsCachePath);
    keys.connectTimeout().ifPresent(auth::setConnectTimeout);
    keys.readTimeout().ifPresent(auth::setReadTimeout);
    keys.keystorePath().ifPresent(auth::setKeystorePath);
    keys.keystorePassword().ifPresent(auth::setKeystorePassword);
    keys.keystoreKeyPassword().ifPresent(auth::setKeystoreKeyPassword);
    keys.truststorePath().ifPresent(auth::setTruststorePath);
    keys.truststorePassword().ifPresent(auth::setTruststorePassword);
    keys.caCertificatePath().ifPresent(auth::setCaCertificatePath);

  }

  public void close(
      @Disposes final Camunda8ClientFactoryRegistry registry) {

    registry.close();

  }

}
