package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.client.CamundaClientBuilder;
import io.camunda.client.CamundaClientConfiguration;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The client of the 8.8 line has no mutual TLS, so every key of the block ends the start.
 * <p>
 * It is asserted per line because this differs from <code>job-lease</code>, which such a line
 * accepts and ignores. Ignoring a certificate would connect without it.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8MtlsTest {

  private static final String PASSWORD = "do-not-print-4711";

  @Test
  @DisplayName("this line has no mutual TLS")
  public void thisLineHasNoMutualTls() {

    assertFalse(Camunda8Mtls.supportedByThisLine());

  }

  @Test
  @DisplayName("an adapter without the block starts as before, and the builder is left alone")
  public void anEmptyBlockChangesNothing() {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setRestAddress("https://localhost:8443");
    assertDoesNotThrow(() -> configuration.validateMtls("c8"));

    final var builder = mock(CamundaClientBuilder.class, RETURNS_SELF);
    Camunda8Mtls.apply("c8", builder, configuration.getMtls());
    assertTrue(mockingDetails(builder).getInvocations().isEmpty());
    assertTrue(Camunda8Mtls
        .environmentOverrides("c8", configuration.getMtls(), mock(CamundaClientConfiguration.class), name -> "set")
        .isEmpty());

  }

  @Test
  @DisplayName("any key of the block ends the start, naming the key and the line it needs")
  public void anyKeyEndsTheStart() {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setRestAddress("https://localhost:8443");
    configuration.getMtls().setKeystorePassword(PASSWORD);

    final var message = assertThrows(
        IllegalStateException.class,
        () -> configuration.validateMtls("c8"))
        .getMessage();
    assertTrue(message.contains("configures mutual TLS"), message);
    assertTrue(message.contains("vanillabp.adapters.c8.mtls.keystore-password"), message);
    assertTrue(message.contains("from 8.9.29 on, so they need the 8.9 line of this adapter or a newer one"), message);
    assertFalse(message.contains(PASSWORD), "a password never reaches a message");

  }

  @Test
  @DisplayName("a factory built without the start refuses the block as well")
  public void theFactoryRefusesTheBlock() {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setRestAddress("https://localhost:8443");
    configuration.getMtls().setCaPath("/certs/ca.pem");

    final var message = assertThrows(
        IllegalStateException.class,
        () -> new Camunda8ClientFactory("c8", configuration).close())
        .getMessage();
    assertTrue(message.contains("vanillabp.adapters.c8.mtls.ca-path"), message);

    final var builder = mock(CamundaClientBuilder.class, RETURNS_SELF);
    final var applied = assertThrows(
        IllegalStateException.class,
        () -> Camunda8Mtls.apply("c8", builder, configuration.getMtls()))
        .getMessage();
    assertTrue(applied.contains("release line 8.8"), applied);
    assertTrue(mockingDetails(builder).getInvocations().isEmpty());

  }

}
