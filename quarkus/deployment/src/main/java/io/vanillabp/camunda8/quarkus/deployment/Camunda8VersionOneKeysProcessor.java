package io.vanillabp.camunda8.quarkus.deployment;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

import io.quarkus.arc.deployment.ValidationPhaseBuildItem.ValidationErrorBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.vanillabp.camunda8.client.Camunda8VersionOneKeys;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;

/**
 * Ends the build of an application whose configuration still holds a key of the Camunda 8
 * adapter of VanillaBP 1 which this version does not read. The message says for each key what
 * to write instead, see {@link Camunda8VersionOneKeys}.
 * <p>
 * Quarkus refuses such a key by itself while the application starts, because no configuration
 * mapping declares it. Its message names the key, but not what became of it. This check runs
 * while the application is built, over what the configuration files say, and answers that
 * question. A key which only an environment variable or a configuration source of the running
 * application brings is left to Quarkus and its own message.
 */
class Camunda8VersionOneKeysProcessor {

  private static final Pattern ADAPTER_SECTION = Pattern.compile("vanillabp\\.adapters\\.([^.]+)\\..+");

  /**
   * Ends the build if the configuration holds a key of version 1 which this version does not
   * read.
   *
   * @param validation Where a defect of the application is collected, so that the build ends
   *          with it rather than with a stack trace of this step
   */
  @BuildStep
  void refuseKeysOfVersionOne(
      final BuildProducer<ValidationErrorBuildItem> validation) {

    final var config = ConfigProvider.getConfig();
    try {
      Camunda8VersionOneKeys.refuseKeysOfVersionOne(camunda8AdapterIds(config), config.getPropertyNames());
    } catch (final IllegalStateException e) {
      validation.produce(new ValidationErrorBuildItem(e));
    }

  }

  /**
   * The ids of the Camunda 8 adapters, read from the configuration of the build. The platform
   * resolves them only when the application starts, by the same three rules: a section whose
   * type is <code>camunda8</code>, or which has no type and is named <code>camunda8</code>; the
   * id <code>camunda8</code> in <code>prioritized-adapters</code>; and the id
   * <code>camunda8</code> where nothing else names one. The last two always give the id which
   * is the type.
   */
  private static Set<String> camunda8AdapterIds(
      final Config config) {

    final var adapterType = Camunda8DeploymentService.ADAPTER_TYPE;
    final var ids = new TreeSet<String>();
    for (final var name : config.getPropertyNames()) {
      final var section = ADAPTER_SECTION.matcher(name);
      if (section.matches() && adapterType.equals(typeOf(config, section.group(1)).orElse(section.group(1)))) {
        ids.add(section.group(1));
      }
    }
    final var typeOfTheIdNamedAfterTheType = typeOf(config, adapterType).orElse(adapterType);
    if (adapterType.equals(typeOfTheIdNamedAfterTheType)) {
      final var prioritized = config
          .getOptionalValues("vanillabp.prioritized-adapters", String.class)
          .orElse(List.of());
      if (prioritized.contains(adapterType) || ids.isEmpty()) {
        ids.add(adapterType);
      }
    }
    return ids;

  }

  private static Optional<String> typeOf(
      final Config config,
      final String adapterId) {

    return config.getOptionalValue("vanillabp.adapters.%s.type".formatted(adapterId), String.class);

  }

}
