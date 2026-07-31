package io.vanillabp.camunda8.springboot.smoke;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;

import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Boot tests of the Camunda 8 startup validation (story 26c): configuration is
 * validated AT STARTUP, never first at runtime.
 * <ul>
 *   <li>entirely unconfigured connection → the application BOOTS and a guiding WARN
 *       names the exact property keys to add;</li>
 *   <li>inconsistent connection config of a first-priority adapter → the boot FAILS
 *       naming the missing keys;</li>
 *   <li>inconsistent config of a nowhere-first adapter with policy 'warn' → the
 *       application boots DEGRADED with a warning;</li>
 *   <li>fully configured → no warning, and the configured secret NEVER appears in
 *       the log (messages name keys, not values).</li>
 * </ul>
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8StartupValidationBootTest {

  private static final String DEPLOYMENT_EXCLUDE = "spring.autoconfigure.exclude=io.vanillabp.integration.deployment.DeploymentAutoConfiguration";

  private org.springframework.context.ConfigurableApplicationContext run(
      final String... properties) {

    // command-line arguments (highest precedence) so the scenarios override the
    // application.yaml defaults
    final var args = java.util.stream.Stream
        .concat(
            java.util.stream.Stream.of(DEPLOYMENT_EXCLUDE), java.util.stream.Stream.of(properties))
        .map("--%s"::formatted)
        .toArray(String[]::new);
    return new SpringApplicationBuilder(SmokeTestApplication.class)
        .web(org.springframework.boot.WebApplicationType.NONE)
        .run(args);

  }

  @Test
  public void unconfiguredAdapterBootsWithGuidingWarning(
      final CapturedOutput output) {

    final var before = output.getAll().length();

    // application.yaml configures adapter 'c8' WITHOUT any connection property
    try (var context = run()) {
      Assertions.assertTrue(context.isActive());
    }

    final var log = output.getAll().substring(before);
    Assertions.assertTrue(
        log.contains("Camunda 8 adapter 'c8' has no connection configuration yet"),
        "expected the guiding warning but got: "
            + log);
    Assertions.assertTrue(log.contains("vanillabp.adapters.c8.mode"));
    Assertions.assertTrue(log.contains("vanillabp.adapters.c8.rest-address"));
    Assertions.assertTrue(log.contains("vanillabp.adapters.c8.client-secret"));

  }

  @Test
  public void inconsistentFirstPriorityAdapterFailsTheBoot() {

    // 'c8' is first priority and configured inconsistently: saas without any
    // credential - a genuine defect has to fail the boot naming the missing keys
    final var exception = Assertions.assertThrows(
        Exception.class,
        () -> run("vanillabp.adapters.c8.mode=saas").close());

    final var failure = rootMessage(exception);
    Assertions.assertTrue(
        failure.contains("Camunda 8 adapter 'c8' is configured inconsistently"),
        "expected the guiding failure but got: "
            + failure);
    Assertions.assertTrue(failure.contains("vanillabp.adapters.c8.cluster-id"));
    Assertions.assertTrue(failure.contains("vanillabp.adapters.c8.region"));
    Assertions.assertTrue(failure.contains("vanillabp.adapters.c8.client-id"));
    Assertions.assertTrue(failure.contains("vanillabp.adapters.c8.client-secret"));
    Assertions.assertTrue(failure.contains("vanillabp.adapters.c8.deployment-failure"));

  }

  @Test
  public void inconsistentNowhereFirstAdapterWithWarnPolicyBootsDegraded(
      final CapturedOutput output) {

    final var before = output.getAll().length();

    // 'c8-two' is nowhere first ('c8' leads everywhere) and its policy is 'warn':
    // the inconsistent config degrades to a warning instead of failing the boot
    try (var context = run(
        "vanillabp.prioritized-adapters=c8,c8-two",
        "vanillabp.adapters.c8.rest-address=http://localhost:8080",
        "vanillabp.adapters.c8-two.type=camunda8",
        "vanillabp.adapters.c8-two.deployment-failure=warn",
        "vanillabp.adapters.c8-two.mode=saas",
        "vanillabp.workflow-modules.test-app.adapters.c8-two.resources-location=classpath*:test-app/processes-two")) {
      Assertions.assertTrue(context.isActive());
    }

    final var log = output.getAll().substring(before);
    Assertions.assertTrue(
        log.contains("Camunda 8 adapter 'c8-two' is configured inconsistently"),
        "expected the degraded-boot warning but got: "
            + log);
    Assertions.assertTrue(log.contains("boots DEGRADED"));
    Assertions.assertTrue(log.contains("vanillabp.adapters.c8-two.cluster-id"));

  }

  @Test
  public void fullyConfiguredAdapterBootsWithoutWarningAndWithoutEchoingSecrets(
      final CapturedOutput output) {

    final var secret = "super-secret-credential-4711";
    final var before = output.getAll().length();

    try (var context = run(
        "vanillabp.adapters.c8.mode=saas",
        "vanillabp.adapters.c8.cluster-id=my-cluster",
        "vanillabp.adapters.c8.region=bru-2",
        "vanillabp.adapters.c8.client-id=my-client",
        "vanillabp.adapters.c8.client-secret="
            + secret)) {
      Assertions.assertTrue(context.isActive());
    }

    final var log = output.getAll().substring(before);
    Assertions.assertFalse(
        log.contains("no connection configuration yet"),
        "no warning expected for a fully configured adapter but got: "
            + log);
    Assertions.assertFalse(
        log.contains("configured inconsistently"),
        "no warning expected for a fully configured adapter but got: "
            + log);
    // hard rule: values - especially credentials - are never echoed
    Assertions.assertFalse(
        log.contains(secret),
        "the configured client-secret must never appear in the log");

  }

  private static String rootMessage(
      final Throwable throwable) {

    var cause = throwable;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return String.valueOf(cause.getMessage());

  }

}
