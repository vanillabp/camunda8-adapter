package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * A key of VanillaBP 1 which this version does not read ends the start. The message has one line
 * per such key, which names what to write instead. A key this version reads gets no line, and
 * neither does a key of another adapter.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8VersionOneKeysTest {

  /**
   * The lines of the message which ends the start, one per key of version 1, or none where the
   * start goes on.
   */
  private static List<String> linesFor(
      final String... propertyNames) {

    try {
      Camunda8VersionOneKeys.refuseKeysOfVersionOne(Set.of("camunda8"), List.of(propertyNames));
      return List.of();
    } catch (final IllegalStateException e) {
      final var lines = e
          .getMessage()
          .lines()
          .filter(line -> line.startsWith("  - "))
          .toList();
      assertTrue(e.getMessage().startsWith("The Camunda 8 adapter does not read these keys of VanillaBP 1"),
          e::getMessage);
      assertFalse(lines.isEmpty(), e::getMessage);
      return lines;
    }

  }

  @Test
  @DisplayName("task-timeout names job-timeout at the same level")
  public void taskTimeoutNamesJobTimeout() {

    final var lines = linesFor(
        "vanillabp.workflow-modules.Demo.workflows.Loan.tasks.check.adapters.camunda8.task-timeout");

    assertEquals(1, lines.size(), lines::toString);
    assertTrue(
        lines
            .getFirst()
            .contains(
                "Write 'vanillabp.workflow-modules.Demo.workflows.Loan.tasks.check.adapters.camunda8.job-timeout'"),
        lines.getFirst());

  }

  @Test
  @DisplayName("A worker key below the adapter level names the adapter level, and its new name")
  public void aWorkerKeyNamesTheAdapterLevel() {

    final var lines = linesFor(
        "vanillabp.workflow-modules.Demo.adapters.camunda8.poll-request-timeout",
        "vanillabp.workflow-modules.Demo.workflows.Loan.adapters.camunda8.stream-enabled",
        "vanillabp.workflow-modules.Demo.adapters.camunda8.pollInterval",
        "vanillabp.workflow-modules.Demo.adapters.camunda8.stream-timeout");

    assertEquals(4, lines.size(), lines::toString);
    assertTrue(lines.get(0).contains("'vanillabp.adapters.camunda8.request-timeout'"), lines.get(0));
    assertTrue(lines.get(1).contains("'vanillabp.adapters.camunda8.stream-enabled'"), lines.get(1));
    assertTrue(lines.get(2).contains("'vanillabp.adapters.camunda8.poll-interval'"), lines.get(2));
    assertTrue(lines.get(3).contains("'vanillabp.adapters.camunda8.stream-timeout'"), lines.get(3));

  }

  @Test
  @DisplayName("use-tenants, task-id-as-hex-string and the old places of allow-connectors are named")
  public void theRemovedAndMovedKeysAreNamed() {

    final var lines = linesFor(
        "vanillabp.workflow-modules.Demo.adapters.camunda8.use-tenants",
        "vanillabp.workflow-modules.Demo.adapters.camunda8.task-id-as-hex-string",
        "vanillabp.allow-connectors",
        "vanillabp.workflow-modules.Demo.allow-connectors",
        "vanillabp.workflow-modules.Demo.workflows.Loan.allow-connectors");

    assertEquals(5, lines.size(), lines::toString);
    assertTrue(lines.get(0).contains("name-clash-avoidance: none"), lines.get(0));
    assertTrue(lines.get(1).contains("always decimal"), lines.get(1));
    assertTrue(lines.get(2).contains("'vanillabp.adapters.<id>.allow-connectors'"), lines.get(2));
    assertTrue(
        lines.get(3).contains("'vanillabp.workflow-modules.Demo.adapters.<id>.allow-connectors'"),
        lines.get(3));
    assertTrue(
        lines.get(4).contains("'vanillabp.workflow-modules.Demo.workflows.Loan.adapters.<id>.allow-connectors'"),
        lines.get(4));

  }

  @Test
  @DisplayName("Keys this version reads, and keys of another adapter, do not end the start")
  public void keysThisVersionReadsAreLeftAlone() {

    final var lines = linesFor(
        "vanillabp.adapters.camunda8.poll-interval",
        "vanillabp.adapters.camunda8.stream-timeout",
        "vanillabp.adapters.camunda8.allow-connectors",
        "vanillabp.workflow-modules.Demo.adapters.camunda8.allow-connectors",
        "vanillabp.workflow-modules.Demo.adapters.camunda8.retry-backoff",
        "vanillabp.workflow-modules.Demo.adapters.camunda8.tenant-id",
        "vanillabp.workflow-modules.Demo.adapters.camunda8.report-errors-as-stack-trace",
        "vanillabp.workflow-modules.Demo.adapters.camunda7.task-timeout",
        "camunda.client.worker.defaults.timeout",
        "VANILLABP_WORKFLOWMODULES_DEMO_ADAPTERS_CAMUNDA8_TASKTIMEOUT");

    assertEquals(List.of(), lines);

  }

  @Test
  @DisplayName("A key found in two property sources gets one line")
  public void aKeyIsReportedOnce() {

    final var key = "vanillabp.workflow-modules.Demo.adapters.camunda8.task-timeout";

    assertEquals(1, linesFor(key, key).size());

  }

  @Test
  @DisplayName("An application without a Camunda 8 adapter id is not looked at")
  public void anApplicationWithoutCamunda8AdapterIsNotLookedAt() {

    Camunda8VersionOneKeys
        .refuseKeysOfVersionOne(Set.of(), List.of("vanillabp.workflow-modules.Demo.adapters.camunda8.task-timeout"));

  }

}
