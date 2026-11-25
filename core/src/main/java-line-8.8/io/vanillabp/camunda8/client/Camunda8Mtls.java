package io.vanillabp.camunda8.client;

import java.util.List;
import java.util.function.Function;

import io.camunda.client.CamundaClientBuilder;
import io.camunda.client.CamundaClientConfiguration;

/**
 * Would hand the client the mutual TLS of this adapter instance. This is the 8.8 variant,
 * and the client of this line has no mutual TLS. The client of the 8.9 line has it from
 * 8.9.29 on, the client of the 8.10 line from 8.10.10 on.
 * <p>
 * The start refuses every key of the block on this line, see
 * {@link Camunda8AdapterConfiguration#validateMtls(String)}. This class repeats the refusal
 * where the client is built. So a factory built without the start, as a test does it, cannot
 * connect without the certificate it was given either.
 */
final class Camunda8Mtls {

  private Camunda8Mtls() {
  }

  /**
   * Whether the client of this release line has mutual TLS.
   *
   * @return <code>false</code>: the client this line is compiled against has none
   */
  static boolean supportedByThisLine() {

    return false;

  }

  /**
   * Refuses a block which configures anything, and does nothing otherwise.
   *
   * @param adapterId The adapter id
   * @param builder The builder of the client of this adapter instance, left alone
   * @param mtls The mtls block
   * @throws IllegalStateException If a key of the block is set
   */
  static void apply(
      final String adapterId,
      final CamundaClientBuilder builder,
      final Camunda8MtlsConfiguration mtls) {

    if (mtls.isConfigured()) {
      throw mtls.notOnThisLine(adapterId, "8.8");
    }

  }

  /**
   * The client of this line reads no <code>CAMUNDA_MTLS_*</code> variable, so none can
   * replace anything.
   *
   * @param adapterId The adapter id
   * @param mtls What the adapter configured
   * @param clientConfiguration What the built client reports
   * @param environment Reads an environment variable
   * @return Nothing, always
   */
  static List<Camunda8EnvironmentOverrides.Override> environmentOverrides(
      final String adapterId,
      final Camunda8MtlsConfiguration mtls,
      final CamundaClientConfiguration clientConfiguration,
      final Function<String, String> environment) {

    return List.of();

  }

}
