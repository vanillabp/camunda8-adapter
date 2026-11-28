package io.vanillabp.camunda8.springboot.client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;

import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.client.Camunda8StartupValidation;
import io.vanillabp.camunda8.client.Camunda8VersionOneKeys;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.camunda8.observability.MicrometerCamunda8Metrics;
import io.vanillabp.camunda8.wiring.Camunda8Connectors;
import io.vanillabp.camunda8.wiring.Camunda8FetchVariables;
import io.vanillabp.camunda8.wiring.Camunda8Listeners;
import io.vanillabp.integration.adapter.migration.config.DeploymentFailurePolicy;
import io.vanillabp.integration.config.VanillaBpConfigurationProperties;
import lombok.extern.slf4j.Slf4j;

/**
 * Exposes the {@link Camunda8ClientFactoryRegistry}. It is built from the configuration
 * of each adapter at <code>vanillabp.adapters.&lt;id&gt;.*</code>, which the adapter's
 * overlay {@link VanillaBpCamunda8Properties} binds. The registry owns one
 * {@code CamundaClient} per adapter ID. It is a managed bean, so its {@code close()} runs
 * when the application shuts down, and that closes all clients.
 * <p>
 * The set of adapter ids comes from the platform's core properties (the ids of type
 * {@code camunda8}). The overlay map is only used to look up an id the core already knows.
 * The connection configuration of every instance is VALIDATED AT STARTUP
 * ({@link Camunda8StartupValidation}):
 * <ul>
 * <li>A self-managed adapter without an address uses the address of the local cluster of
 * its release line, and the start warns about it.</li>
 * <li>An adapter with an inconsistent configuration ends the boot. The exception is an
 * adapter which is nowhere first priority and whose deployment-failure policy is
 * 'warn'.</li>
 * <li>The clients of completely configured adapters are built eagerly.</li>
 * <li>A key of version 1 which this version does not read ends the boot
 * ({@link Camunda8VersionOneKeys}).</li>
 * <li>An <code>allow-connectors</code> or <code>allow-listeners</code> at task level ends the boot,
 * because nothing reads it there.</li>
 * </ul>
 * In a test whose cluster a test runtime hands over (a bean of type
 * {@link Camunda8ClusterOfATestRuntime} is there), no client is built at startup, and the
 * configured connection is neither checked nor used. Every other check runs as usual.
 */
@Slf4j
@AutoConfiguration
@EnableConfigurationProperties({
    VanillaBpConfigurationProperties.class, VanillaBpCamunda8Properties.class
})
public class Camunda8ClientAutoConfiguration {

  /**
   * Built by Spring Boot while it reads the auto-configuration imports.
   */
  public Camunda8ClientAutoConfiguration() {
  }

  /**
   * Builds the one registry of this application, with a client factory per configured adapter id
   * of type <code>camunda8</code>. The ids come from the platform's own properties, and the
   * adapter's overlay is read per id.
   *
   * @param coreProperties The platform's properties, which name the configured adapter ids and
   *          their types
   * @param overlay The adapter's own view of the same tree
   * @param environment Where the keys of version 1 are looked for, which this version does
   *          not read and which therefore end the boot
   * @param testRuntime There in a test whose cluster a test runtime hands over, and empty
   *          everywhere else
   * @param beans Where a test on Camunda Process Test is recognised which lacks the support of
   *          this adapter
   * @return The registry, with every completely configured client already built
   */
  @Bean(destroyMethod = "close")
  public Camunda8ClientFactoryRegistry camunda8ClientFactoryRegistry(
      final VanillaBpConfigurationProperties coreProperties,
      final VanillaBpCamunda8Properties overlay,
      final Environment environment,
      final ObjectProvider<Camunda8ClusterOfATestRuntime> testRuntime,
      final ListableBeanFactory beans) {

    final var followsATestRuntime = testRuntime.getIfAvailable() != null;

    final var adapterIds = coreProperties
        .adapterTypes()
        .entrySet()
        .stream()
        .filter(adapter -> Camunda8DeploymentService.ADAPTER_TYPE.equals(adapter.getValue()))
        .map(Map.Entry::getKey)
        .toList();
    // a key of version 1 which this version does not read would change nothing in silence.
    // It is refused first, because a later message may be about the very value it meant to set
    Camunda8VersionOneKeys.refuseKeysOfVersionOne(adapterIds, propertyNames(environment));
    // a test on Camunda Process Test without the support would wait for a cluster which
    // starts only after the context, for the whole startup-wait
    if (!followsATestRuntime && !adapterIds.isEmpty()) {
      refuseCamundaProcessTestWithoutItsSupport(adapterIds, beans);
    }

    final Map<String, Camunda8AdapterConfiguration> configurations = new HashMap<>();
    adapterIds
        .forEach(adapterId -> {
          final var configuration = overlay
              .getAdapters()
              .getOrDefault(adapterId, new Camunda8AdapterConfiguration());
          // a removed key is refused before anything else, wherever it is set. The
          // workers no longer read it, and silence would hide that from the application
          Camunda8FetchVariables.rejectTheRemovedKey(adapterId, overlay.fetchVariablesKeys(adapterId));
          if (followsATestRuntime) {
            Camunda8StartupValidation.validateAtStartupForATestRuntime(
                adapterId, configuration, coreProperties.resolvedDeliveryRetention(), log::warn, log::info);
          } else {
            Camunda8StartupValidation.validateAtStartup(
                adapterId,
                configuration,
                coreProperties.isFirstPriorityAnywhere(adapterId),
                coreProperties.getDeploymentFailureFor(
                    adapterId) == DeploymentFailurePolicy.WARN,
                coreProperties.resolvedDeliveryRetention(),
                // the warning about the local cluster names the key which moves this
                // application off the port that cluster takes
                "server.port",
                log::warn,
                log::info);
          }
          // a key at a level which does not read it would change nothing in silence
          Camunda8Connectors.refuseKeysSetAtTaskLevel(adapterId, overlay.allowConnectorsKeysAtTaskLevel(adapterId));
          Camunda8Listeners.refuseKeysSetAtTaskLevel(adapterId, overlay.allowListenersKeysAtTaskLevel(adapterId));
          configurations.put(adapterId, configuration);
        });

    return new Camunda8ClientFactoryRegistry(configurations, followsATestRuntime);

  }

  /**
   * The marker bean which {@code @CamundaSpringProcessTest} imports into the context of a test
   * class. It is named as a string, because this artifact must not depend on Camunda Process
   * Test.
   */
  static final String CAMUNDA_PROCESS_TEST_MARKER = "io.camunda.client.spring.testsupport.CamundaSpringProcessTestContext";

  /**
   * The artifact which lets this adapter follow the cluster of Camunda Process Test.
   */
  static final String PROCESS_TEST_ARTIFACT = "io.vanillabp:camunda8-adapter-spring-boot-process-test";

  /**
   * Ends the start of a test on Camunda Process Test whose classpath lacks the artifact which
   * lets this adapter follow the cluster of that test. Without it the adapter would build its
   * client against the configured cluster, and it would wait for it until {@code startup-wait}
   * runs out, ten minutes by default, because Camunda Process Test starts its cluster only
   * after the context.
   *
   * @param adapterIds The configured adapter ids of type {@code camunda8}
   * @param beans The beans of the context
   */
  static void refuseCamundaProcessTestWithoutItsSupport(
      final List<String> adapterIds,
      final ListableBeanFactory beans) {

    if (!ClassUtils.isPresent(CAMUNDA_PROCESS_TEST_MARKER, null)) {
      return;
    }
    final var marker = ClassUtils.resolveClassName(CAMUNDA_PROCESS_TEST_MARKER, null);
    if (beans.getBeanNamesForType(marker, false, false).length == 0) {
      return;
    }
    throw new IllegalStateException(
        """
            This test runs on Camunda Process Test (@CamundaSpringProcessTest), but the Camunda 8 adapter \
            '%s' cannot follow its cluster. Add the artifact '%s' as a test dependency, in the version of \
            your adapter. Camunda Process Test starts its cluster after the application context and \
            empties it after each test method. Without that artifact the adapter would wait for its \
            configured cluster until '%s' runs out."""
            .formatted(
                String.join("', '", adapterIds),
                PROCESS_TEST_ARTIFACT,
                Camunda8AdapterConfiguration.propertyKey(adapterIds.getFirst(), "startup-wait")));

  }

  /**
   * The names of every property a configuration file or a property source with a list of
   * names holds. A source which cannot list its names is skipped.
   */
  private static List<String> propertyNames(
      final Environment environment) {

    if (!(environment instanceof ConfigurableEnvironment configurable)) {
      return List.of();
    }
    return configurable
        .getPropertySources()
        .stream()
        .filter(EnumerablePropertySource.class::isInstance)
        .flatMap(source -> Arrays.stream(((EnumerablePropertySource<?>) source).getPropertyNames()))
        .toList();

  }

  /**
   * What this adapter measures on top of the core's meters: the client's own job counters
   * per worker and the execution slots of every adapter instance. Micrometer is optional,
   * so this whole configuration depends on it. An application without Micrometer boots
   * unchanged and reports nothing.
   */
  @Configuration(proxyBeanMethods = false)
  // by NAME, not by class literal. Spring reads the annotation of a nested configuration
  // class by reflection. A class literal of a missing optional dependency would fail
  // there, before the condition is ever checked
  @ConditionalOnClass(
      name = "io.micrometer.core.instrument.MeterRegistry")
  public static class Camunda8MetricsConfiguration {

    /**
     * Built by Spring Boot where Micrometer is on the classpath.
     */
    public Camunda8MetricsConfiguration() {
    }

    /**
     * Publishes the adapter's meter binder, which Spring Boot then applies to its registry.
     *
     * @return The meter binder of this adapter's own numbers
     */
    @Bean
    @ConditionalOnMissingBean
    public MicrometerCamunda8Metrics camunda8Metrics() {

      return new MicrometerCamunda8Metrics();

    }

  }

}
