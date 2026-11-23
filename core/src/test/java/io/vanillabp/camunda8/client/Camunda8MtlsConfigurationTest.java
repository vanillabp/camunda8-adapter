package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The rules of the <code>mtls</code> block, which hold on every release line. Whether a line
 * applies the block at all is asserted per line, in <code>Camunda8MtlsTest</code>.
 * <p>
 * Each rule is one the client checks only while it builds its connection, or not at all, and
 * then in the words of its own environment variables. So each test asserts that the message
 * names the keys of this adapter, and where a secret is involved, that it does not name the
 * secret.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8MtlsConfigurationTest {

  private static final List<String> TLS_ADDRESSES = List.of("https://camunda:8443", "https://camunda:26500");

  private static final String PEM = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";

  private static final String SECRET = "do-not-print-4711";

  @TempDir
  Path directory;

  private String file(
      final String name) throws Exception {

    return Files
        .writeString(directory.resolve(name), PEM)
        .toString();

  }

  private static String failure(
      final Camunda8MtlsConfiguration mtls,
      final String legacyCaCertificatePath,
      final List<String> addresses) {

    return assertThrows(
        IllegalStateException.class,
        () -> mtls.validate("c8", legacyCaCertificatePath, addresses))
        .getMessage();

  }

  @Test
  @DisplayName("an empty block is not configured and passes, whatever the addresses")
  public void anEmptyBlockPasses() {

    final var mtls = new Camunda8MtlsConfiguration();
    assertFalse(mtls.isConfigured());
    assertTrue(mtls.configuredKeys().isEmpty());
    assertDoesNotThrow(() -> mtls.validate("c8", "/some/ca.pem", List.of("http://localhost:8080")));

  }

  @Test
  @DisplayName("a client certificate by path, a CA by path and a passphrase pass")
  public void pathsPass() throws Exception {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCertPath(file("client.pem"));
    mtls.setKeyPath(file("client.key"));
    mtls.setKeyPassphrase(SECRET);
    mtls.setCaPath(file("ca.pem"));
    assertDoesNotThrow(() -> mtls.validate("c8", null, TLS_ADDRESSES));
    assertEquals(
        List.of("mtls.cert-path", "mtls.key-path", "mtls.key-passphrase", "mtls.ca-path"),
        mtls.configuredKeys());

  }

  @Test
  @DisplayName("a client certificate as content and a CA as content pass")
  public void contentPasses() {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCert(PEM);
    mtls.setKey("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n");
    mtls.setCa(PEM);
    assertDoesNotThrow(() -> mtls.validate("c8", null, TLS_ADDRESSES));

  }

  @Test
  @DisplayName("a keystore with its passwords and a truststore pass")
  public void storesPass() throws Exception {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setKeystorePath(file("client.p12"));
    mtls.setKeystorePassword("");
    mtls.setKeystoreKeyPassword(SECRET);
    mtls.setTruststorePath(file("trust.jks"));
    mtls.setTruststorePassword(SECRET);
    assertDoesNotThrow(() -> mtls.validate("c8", null, TLS_ADDRESSES));
    assertTrue(
        mtls.configuredKeys().contains("mtls.keystore-password"),
        "an empty password is a password");

  }

  @Test
  @DisplayName("a certificate without its key names both keys")
  public void aCertificateNeedsItsKey() throws Exception {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCertPath(file("client.pem"));
    final var message = failure(mtls, null, TLS_ADDRESSES);
    assertTrue(
        message.contains("sets 'vanillabp.adapters.c8.mtls.cert-path' but not 'vanillabp.adapters.c8.mtls.key-path'"),
        message);

    final var content = new Camunda8MtlsConfiguration();
    content.setKey(PEM);
    final var contentMessage = failure(content, null, TLS_ADDRESSES);
    assertTrue(
        contentMessage.contains("sets 'vanillabp.adapters.c8.mtls.key' but not 'vanillabp.adapters.c8.mtls.cert'"),
        contentMessage);

  }

  @Test
  @DisplayName("a certificate by path and a key as content do not mix")
  public void pathAndContentDoNotMix() throws Exception {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCertPath(file("client.pem"));
    mtls.setKey(PEM);
    final var message = failure(mtls, null, TLS_ADDRESSES);
    assertTrue(message.contains("given the same way"), message);

  }

  @Test
  @DisplayName("two client identities are refused, and each is named")
  public void oneIdentityOnly() throws Exception {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCertPath(file("client.pem"));
    mtls.setKeyPath(file("client.key"));
    mtls.setKeystorePath(file("client.p12"));
    mtls.setKeystorePassword(SECRET);
    final var message = failure(mtls, null, TLS_ADDRESSES);
    assertTrue(message.contains("gives the client certificate for mutual TLS more than once"), message);
    assertTrue(message.contains("'vanillabp.adapters.c8.mtls.cert-path' with 'vanillabp.adapters.c8.mtls.key-path'"),
        message);
    assertTrue(message.contains("'vanillabp.adapters.c8.mtls.keystore-path'"), message);
    assertFalse(message.contains(SECRET), "a password never reaches a message");

    final var pemTwice = new Camunda8MtlsConfiguration();
    pemTwice.setCertPath(file("client.pem"));
    pemTwice.setKeyPath(file("client.key"));
    pemTwice.setCert(PEM);
    pemTwice.setKey(PEM);
    assertTrue(failure(pemTwice, null, TLS_ADDRESSES).contains("more than once"));

  }

  @Test
  @DisplayName("a passphrase without a PEM key is refused")
  public void aPassphraseNeedsAPemKey() throws Exception {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setKeystorePath(file("client.p12"));
    mtls.setKeystorePassword(SECRET);
    mtls.setKeyPassphrase(SECRET);
    final var message = failure(mtls, null, TLS_ADDRESSES);
    assertTrue(message.contains("sets 'vanillabp.adapters.c8.mtls.key-passphrase', but no private key in PEM format"),
        message);
    assertTrue(message.contains("vanillabp.adapters.c8.mtls.keystore-key-password"), message);
    assertFalse(message.contains(SECRET));

  }

  @Test
  @DisplayName("a keystore needs its password, and a password needs its store")
  public void storesAndPasswordsGoTogether() throws Exception {

    final var keystore = new Camunda8MtlsConfiguration();
    keystore.setKeystorePath(file("client.p12"));
    final var message = failure(keystore, null, TLS_ADDRESSES);
    assertTrue(
        message.contains(
            "sets 'vanillabp.adapters.c8.mtls.keystore-path' but not 'vanillabp.adapters.c8.mtls.keystore-password'"),
        message);

    final var keyPassword = new Camunda8MtlsConfiguration();
    keyPassword.setKeystoreKeyPassword(SECRET);
    final var keyPasswordMessage = failure(keyPassword, null, TLS_ADDRESSES);
    assertTrue(keyPasswordMessage.contains(
        "sets a password of a keystore, but not 'vanillabp.adapters.c8.mtls.keystore-path'"), keyPasswordMessage);
    assertFalse(keyPasswordMessage.contains(SECRET));

    final var truststorePassword = new Camunda8MtlsConfiguration();
    truststorePassword.setTruststorePassword(SECRET);
    final var truststoreMessage = failure(truststorePassword, null, TLS_ADDRESSES);
    assertTrue(truststoreMessage.contains(
        "sets 'vanillabp.adapters.c8.mtls.truststore-password', but not 'vanillabp.adapters.c8.mtls.truststore-path'"),
        truststoreMessage);
    assertFalse(truststoreMessage.contains(SECRET));

  }

  @Test
  @DisplayName("the certificate authorities of the cluster come from one source only")
  public void oneSourceOfAuthorities() throws Exception {

    final var twoInBlock = new Camunda8MtlsConfiguration();
    twoInBlock.setCaPath(file("ca.pem"));
    twoInBlock.setTruststorePath(file("trust.jks"));
    final var message = failure(twoInBlock, null, TLS_ADDRESSES);
    assertTrue(message.contains("names the certificate authorities of the cluster more than once"), message);
    assertTrue(message.contains("vanillabp.adapters.c8.mtls.ca-path"), message);
    assertTrue(message.contains("vanillabp.adapters.c8.mtls.truststore-path"), message);

    final var withLegacy = new Camunda8MtlsConfiguration();
    withLegacy.setCa(PEM);
    final var legacyMessage = failure(withLegacy, "/some/ca.pem", TLS_ADDRESSES);
    assertTrue(legacyMessage.contains("vanillabp.adapters.c8.mtls.ca\n"), legacyMessage);
    assertTrue(legacyMessage.contains("vanillabp.adapters.c8.auth.ca-certificate-path"), legacyMessage);

  }

  @Test
  @DisplayName("the legacy CA certificate goes together with a client certificate")
  public void theLegacyCaCertificateServesAClientCertificate() throws Exception {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCertPath(file("client.pem"));
    mtls.setKeyPath(file("client.key"));
    assertDoesNotThrow(() -> mtls.validate("c8", "/some/ca.pem", TLS_ADDRESSES));

  }

  @Test
  @DisplayName("content which is not PEM is refused without repeating it")
  public void contentMustBePem() {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCert(PEM);
    mtls.setKey("/run/secrets/"
        + SECRET);
    final var message = failure(mtls, null, TLS_ADDRESSES);
    assertTrue(message.contains("sets 'vanillabp.adapters.c8.mtls.key', but its value is not PEM content"), message);
    assertTrue(message.contains("use 'vanillabp.adapters.c8.mtls.key-path' instead"), message);
    assertFalse(message.contains(SECRET), "the value may be a key, so it is not repeated");

  }

  @Test
  @DisplayName("a file which is not there is named with its key and its path")
  public void filesMustBeReadable() throws Exception {

    final var missing = directory
        .resolve("not-mounted.pem")
        .toString();
    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCertPath(file("client.pem"));
    mtls.setKeyPath(missing);
    final var message = failure(mtls, null, TLS_ADDRESSES);
    assertTrue(
        message.contains(
            "names 'vanillabp.adapters.c8.mtls.key-path: %s', but no readable file is there".formatted(missing)),
        message);

  }

  @Test
  @DisplayName("mutual TLS needs at least one address with TLS, and SaaS always has it")
  public void mutualTlsNeedsTls() throws Exception {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCaPath(file("ca.pem"));
    final var message = failure(mtls, null, List.of("http://camunda:8080", "grpc://camunda:26500"));
    assertTrue(message.contains("talks to the cluster without TLS: http://camunda:8080 and grpc://camunda:26500"),
        message);
    assertTrue(message.contains("vanillabp.adapters.c8.rest-address"), message);

    assertDoesNotThrow(() -> mtls.validate("c8", null, List.of("http://camunda:8080", "grpcs://camunda:26500")));
    assertDoesNotThrow(() -> mtls.validate("c8", null, null), "SaaS");

  }

  @Test
  @DisplayName("the refusal of a line without mutual TLS names every key and the line")
  public void theRefusalOfAnOlderLine() {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCa(PEM);
    mtls.setTruststorePassword(SECRET);
    final var message = mtls
        .notOnThisLine("c8", "8.9")
        .getMessage();
    assertTrue(message.contains("the Camunda client of the release line 8.9 has no option for it"), message);
    assertTrue(message.contains("vanillabp.adapters.c8.mtls.ca\n"), message);
    assertTrue(message.contains("vanillabp.adapters.c8.mtls.truststore-password"), message);
    assertTrue(message.contains("from 8.10.10 on"), message);
    assertFalse(message.contains(SECRET));

  }

}
