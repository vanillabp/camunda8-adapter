package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The window an open asynchronous task's job lock is renewed in, and what the
 * startup refuses about it.
 * <p>
 * Two configurations are silent at runtime and expensive in production, so both end the
 * boot: the key which used to carry a horizon of fourteen days, and a window which does
 * not fit below <code>vanillabp.delivery.retention</code>. It is that retention and not
 * the outbox one: the two were one property until they were told apart, and this check
 * always argued about the delivery half. The record is what answers the
 * redelivery which renews the lock, so a window outliving it lets the
 * <code>&#64;WorkflowTask</code> method run a second time - the very defect the window
 * exists to fix.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8AsyncTaskLockRenewalTest {

  @Test
  @DisplayName("The renewal window defaults to one hour")
  public void theWindowDefaultsToAnHour() {

    assertEquals(
        Duration.ofHours(1),
        new Camunda8AdapterConfiguration().resolvedAsyncTaskLockRenewal());
    assertEquals(
        Duration.ofMinutes(5),
        configuration(Duration.ofMinutes(5)).resolvedAsyncTaskLockRenewal());

  }

  @Test
  @DisplayName("The removed key async-task-timeout ends the boot naming its successor")
  public void theRemovedKeyEndsTheBoot() {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setAsyncTaskTimeout(Duration.ofDays(14));

    final var exception = assertThrows(
        IllegalStateException.class,
        () -> configuration.validateAsyncTaskLockRenewal("c8", Duration.ofDays(7)));

    final var message = exception.getMessage();
    assertTrue(message.contains("vanillabp.adapters.c8.async-task-timeout"), "names the removed key");
    assertTrue(message.contains("vanillabp.adapters.c8.async-task-lock-renewal"), "names the new one");
    assertTrue(message.contains("PT1H"), "names the default of the new one");

  }

  @Test
  @DisplayName("A window which does not fit below the retention ends the boot, naming both values")
  public void aWindowAboveTheRetentionEndsTheBoot() {

    final var exception = assertThrows(
        IllegalStateException.class,
        () -> configuration(Duration.ofDays(14)).validateAsyncTaskLockRenewal("c8", Duration.ofDays(7)));

    final var message = exception.getMessage();
    assertTrue(message.contains("vanillabp.adapters.c8.async-task-lock-renewal"), "names the window");
    assertTrue(message.contains("PT336H"), "names the window's value");
    assertTrue(message.contains("vanillabp.delivery.retention"), "names the retention it argues about");
    assertTrue(
        message.contains("vanillabp.outbox.retention"),
        "and the one that retention follows where it is not set itself");
    assertTrue(message.contains("PT168H"), "names the retention's value");
    assertTrue(message.contains("PT16H48M"), "recommends at most a tenth of the retention");

    // equal is not below either: the record would expire exactly when the renewal is due
    assertThrows(
        IllegalStateException.class,
        () -> configuration(Duration.ofDays(7)).validateAsyncTaskLockRenewal("c8", Duration.ofDays(7)));

  }

  @Test
  @DisplayName("The default window fits below the default retention")
  public void theDefaultsFitTogether() {

    assertDoesNotThrow(
        () -> new Camunda8AdapterConfiguration().validateAsyncTaskLockRenewal("c8", Duration.ofDays(7)));
    // a platform which does not report a retention at all leaves the window alone
    assertDoesNotThrow(
        () -> new Camunda8AdapterConfiguration().validateAsyncTaskLockRenewal("c8", null));

  }

  @Test
  @DisplayName("The reaction to an overdue task is report unless configured otherwise")
  public void theActionDefaultsToReport() {

    assertEquals(
        Camunda8AdapterConfiguration.AsyncTaskMaxAgeAction.REPORT,
        new Camunda8AdapterConfiguration().getAsyncTaskMaxAgeAction());

  }

  private static Camunda8AdapterConfiguration configuration(
      final Duration renewal) {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setAsyncTaskLockRenewal(renewal);
    return configuration;

  }

}
