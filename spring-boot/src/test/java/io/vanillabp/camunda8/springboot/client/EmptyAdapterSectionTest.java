package io.vanillabp.camunda8.springboot.client;

import java.io.IOException;
import java.time.Duration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.ConfigurationPropertiesBindHandlerAdvisor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.io.ClassPathResource;

import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.integration.config.EmptySections;
import io.vanillabp.integration.processservice.SpringBootMigrationAdapterAutoConfiguration;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * An adapter section written as <code>{}</code> in a YAML file keeps the keys which other
 * property sources set below it. Spring Boot reads the empty map as an empty value at the
 * section's own name. Without help, that value hides every key of the id when Spring Boot
 * binds the overlay {@link VanillaBpCamunda8Properties}. The platform's {@link EmptySections}
 * prevents this for every class of VanillaBP bound below <code>vanillabp</code>, so this
 * adapter needs no advisor of its own. These tests check that the overlay and the client
 * registry built from it get the keys. None of them needs a cluster.
 */
@ExtendWith(SuppressOutputExtension.class)
public class EmptyAdapterSectionTest {

  private static final String ID = "c8";

  private static final String SECTIONS = "empty-adapter-section/sections.yaml";

  private static final String NOTHING_BUT_EMPTY_SECTIONS = "empty-adapter-section/nothing-but-empty.yaml";

  private static final String REST_ADDRESS = "http://from-the-command-line:8088";

  private static void addSources(
      final ConfigurableEnvironment environment,
      final String yamlFile,
      final String... commandLine) throws IOException {

    // a later document of a YAML file wins over an earlier one, as in Spring Boot
    for (final var document : new YamlPropertySourceLoader().load(yamlFile, new ClassPathResource(yamlFile))) {
      environment
          .getPropertySources()
          .addFirst(document);
    }
    environment
        .getPropertySources()
        .addFirst(new SimpleCommandLinePropertySource(commandLine));

  }

  /**
   * A context which binds only the overlay as {@code @ConfigurationProperties}. The whole
   * auto-configuration of the platform would need an adapter bean to start.
   */
  private static ApplicationContextRunner overlayContextWithoutTheAdvisor(
      final String yamlFile,
      final String... commandLine) {

    return new ApplicationContextRunner()
        .withInitializer(context -> {
          try {
            addSources(context.getEnvironment(), yamlFile, commandLine);
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
        })
        .withUserConfiguration(OverlayOnly.class);

  }

  /**
   * The same context with the advisor bean of the platform's auto-configuration, which every
   * booted application has.
   */
  private static ApplicationContextRunner overlayContext(
      final String yamlFile,
      final String... commandLine) {

    return overlayContextWithoutTheAdvisor(yamlFile, commandLine)
        .withBean(
            "vanillaBpEmptySectionsKeepTheirKeys",
            ConfigurationPropertiesBindHandlerAdvisor.class,
            SpringBootMigrationAdapterAutoConfiguration::vanillaBpEmptySectionsKeepTheirKeys);

  }

  @EnableConfigurationProperties(VanillaBpCamunda8Properties.class)
  static class OverlayOnly {

  }

  @Test
  @DisplayName("Without the platform's advisor, the overlay loses the keys of an empty section")
  public void withoutTheAdvisorTheKeysAreLost() throws IOException {

    final var firstDocument = (EnumerablePropertySource<?>) new YamlPropertySourceLoader()
        .load(SECTIONS, new ClassPathResource(SECTIONS))
        .getFirst();
    // the cause. If Spring Boot stops doing this, the platform's EmptySections is not needed any more
    Assertions.assertEquals("", String.valueOf(firstDocument.getProperty("vanillabp.adapters.c8")));

    overlayContextWithoutTheAdvisor(
        SECTIONS,
        "--vanillabp.adapters.c8.rest-address=http://from-the-command-line:8088")
        .run(context -> {

          Assertions.assertNull(context.getStartupFailure(), "context should start");
          final var section = context
              .getBean(VanillaBpCamunda8Properties.class)
              .getAdapters()
              .get(ID);
          Assertions.assertNull(section.getRestAddress());
          Assertions.assertNull(section.getJobTimeout());

        });

  }

  @Test
  @DisplayName("The overlay keeps the keys of an empty adapter section and of an empty module section")
  public void theOverlayKeepsTheKeys() {

    overlayContext(
        SECTIONS,
        "--vanillabp.adapters.c8.rest-address=http://from-the-command-line:8088",
        "--vanillabp.workflow-modules.loans.adapters.c8.tenant-id=loans-tenant")
        .run(context -> {

          Assertions.assertNull(context.getStartupFailure(), "context should start");
          final var overlay = context.getBean(VanillaBpCamunda8Properties.class);
          final var section = overlay
              .getAdapters()
              .get(ID);
          Assertions.assertEquals(REST_ADDRESS, section.getRestAddress());
          Assertions.assertEquals(Duration.ofMinutes(7), section.getJobTimeout());
          Assertions.assertEquals(
              "loans-tenant",
              overlay
                  .configuredTenantFor(ID, "loans")
                  .tenantId());

        });

  }

  @Test
  @DisplayName("A section of nothing but {} still configures the id, with the default keys")
  public void aSectionOfNothingButEmptyBraces() {

    overlayContext(NOTHING_BUT_EMPTY_SECTIONS).run(context -> {

      Assertions.assertNull(context.getStartupFailure(), "context should start");
      final var overlay = context.getBean(VanillaBpCamunda8Properties.class);
      Assertions.assertTrue(
          overlay
              .getAdapters()
              .containsKey(ID),
          "the id of a section of nothing but {} should be bound");
      Assertions.assertNull(
          overlay
              .getAdapters()
              .get(ID)
              .getRestAddress());
      Assertions.assertNull(overlay.configuredTenantFor(ID, "loans"));

    });

  }

  @Test
  @DisplayName("The client of an empty section is built with the type and the address set beside it")
  public void theClientIsBuiltWithTheKeys() {

    new ApplicationContextRunner()
        .withPropertyValues(
            "spring.config.location=classpath:empty-adapter-section/application.yaml",
            "vanillabp.adapters.c8.type=camunda8",
            "vanillabp.adapters.c8.rest-address=http://from-the-command-line:8088",
            "vanillabp.adapters.c8.job-lease=do-not-use")
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withBean(
            "vanillaBpEmptySectionsKeepTheirKeys",
            ConfigurationPropertiesBindHandlerAdvisor.class,
            SpringBootMigrationAdapterAutoConfiguration::vanillaBpEmptySectionsKeepTheirKeys)
        .withConfiguration(AutoConfigurations.of(Camunda8ClientAutoConfiguration.class))
        .run(context -> {

          // without the job-lease key from the command line, the startup validation would end the start
          Assertions.assertNull(context.getStartupFailure(), "context should start");
          // the type comes from the core properties. Without it, the id would not be one of this adapter
          final var client = context
              .getBean(Camunda8ClientFactoryRegistry.class)
              .getFactory(ID)
              .getClient();
          Assertions.assertEquals(
              REST_ADDRESS,
              client
                  .getConfiguration()
                  .getRestAddress()
                  .toString());
          // the platform's advisor keeps the keys of empty sections, the adapter brings none of its own
          Assertions.assertEquals(
              1,
              context
                  .getBeansOfType(ConfigurationPropertiesBindHandlerAdvisor.class)
                  .size());

        });

  }

}
