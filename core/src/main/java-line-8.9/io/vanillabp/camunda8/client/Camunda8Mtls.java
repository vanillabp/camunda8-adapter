package io.vanillabp.camunda8.client;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import io.camunda.client.CamundaClientBuilder;
import io.camunda.client.CamundaClientConfiguration;

/**
 * Hands the client the mutual TLS of this adapter instance. This is the 8.9 variant.
 * <p>
 * The client of this line has the options from 8.9.29 on, and the line is built against
 * such a client. They are the same seven builder methods the 8.10 line calls, so this class
 * does what the 8.10 variant does. Each pair of keys goes to the builder method which takes
 * that pair. The client keeps the path and the content of a PEM file apart, so a block which
 * names both would make the content win without a word.
 * {@link Camunda8MtlsConfiguration#validate} refuses that before this class runs.
 */
final class Camunda8Mtls {

  private Camunda8Mtls() {
  }

  /**
   * Whether the client of this release line has mutual TLS.
   *
   * @return <code>true</code>: the client has it from 8.9.29 on
   */
  static boolean supportedByThisLine() {

    return true;

  }

  /**
   * Sets on the builder what the block configures, and nothing else. A key which is not set
   * leaves the client's own default alone, and that default is no mutual TLS.
   *
   * @param adapterId The adapter id
   * @param builder The builder of the client of this adapter instance
   * @param mtls The validated mtls block
   */
  static void apply(
      final String adapterId,
      final CamundaClientBuilder builder,
      final Camunda8MtlsConfiguration mtls) {

    if (hasText(mtls.getCertPath())) {
      builder.clientCertificate(mtls.getCertPath(), mtls.getKeyPath());
    }
    if (hasText(mtls.getCert())) {
      builder.clientCertificateContent(mtls.getCert(), mtls.getKey());
    }
    if (mtls.getKeyPassphrase() != null) {
      builder.clientKeyPassphrase(mtls.getKeyPassphrase());
    }
    if (hasText(mtls.getKeystorePath())) {
      builder.clientKeystore(
          mtls.getKeystorePath(), mtls.getKeystorePassword(), mtls.getKeystoreKeyPassword());
    }
    if (hasText(mtls.getCaPath())) {
      builder.mtlsCaCertificatePath(mtls.getCaPath());
    }
    if (hasText(mtls.getCa())) {
      builder.mtlsCaCertificateContent(mtls.getCa());
    }
    if (hasText(mtls.getTruststorePath())) {
      builder.mtlsTruststore(mtls.getTruststorePath(), mtls.getTruststorePassword());
    }

  }

  /**
   * The files of the block which a <code>CAMUNDA_MTLS_*</code> variable replaced. Only the
   * paths are compared. The content of a private key and every password are secrets, and
   * the comparison would end up in a log line.
   *
   * @param adapterId The adapter id
   * @param mtls What the adapter configured
   * @param clientConfiguration What the built client reports
   * @param environment Reads an environment variable
   * @return Every path an environment variable changed
   */
  static List<Camunda8EnvironmentOverrides.Override> environmentOverrides(
      final String adapterId,
      final Camunda8MtlsConfiguration mtls,
      final CamundaClientConfiguration clientConfiguration,
      final Function<String, String> environment) {

    final var overrides = new LinkedList<Camunda8EnvironmentOverrides.Override>();
    Camunda8EnvironmentOverrides.compare(
        overrides, environment, "CAMUNDA_MTLS_CERT_PATH", null,
        Camunda8AdapterConfiguration.propertyKey(adapterId, "mtls.cert-path"),
        mtls.getCertPath(), clientConfiguration.getMtlsCertPath());
    Camunda8EnvironmentOverrides.compare(
        overrides, environment, "CAMUNDA_MTLS_KEY_PATH", null,
        Camunda8AdapterConfiguration.propertyKey(adapterId, "mtls.key-path"),
        mtls.getKeyPath(), clientConfiguration.getMtlsKeyPath());
    Camunda8EnvironmentOverrides.compare(
        overrides, environment, "CAMUNDA_MTLS_KEYSTORE_PATH", null,
        Camunda8AdapterConfiguration.propertyKey(adapterId, "mtls.keystore-path"),
        mtls.getKeystorePath(), clientConfiguration.getMtlsKeystorePath());
    Camunda8EnvironmentOverrides.compare(
        overrides, environment, "CAMUNDA_MTLS_CA_PATH", null,
        Camunda8AdapterConfiguration.propertyKey(adapterId, "mtls.ca-path"),
        mtls.getCaPath(), clientConfiguration.getMtlsCaPath());
    Camunda8EnvironmentOverrides.compare(
        overrides, environment, "CAMUNDA_MTLS_TRUSTSTORE_PATH", null,
        Camunda8AdapterConfiguration.propertyKey(adapterId, "mtls.truststore-path"),
        mtls.getTruststorePath(), clientConfiguration.getMtlsTruststorePath());
    return overrides;

  }

  private static boolean hasText(
      final String value) {

    return (value != null) && !value.isBlank();

  }

}
