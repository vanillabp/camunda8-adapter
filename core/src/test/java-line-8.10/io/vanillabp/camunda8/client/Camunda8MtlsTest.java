package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.camunda.client.CamundaClientBuilder;
import io.camunda.client.CamundaClientConfiguration;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Mutual TLS reaches the client of the 8.10 line, whose client has it from 8.10.10 on.
 * <p>
 * Each pair of keys has to reach the one builder method which takes that pair. A mocked
 * builder shows which calls the adapter makes. A real client, built from a certificate made
 * for the test, shows that the client takes what the adapter hands it. Building the client
 * reads the certificates, because the client prepares its TLS connections right away. It
 * does not contact a cluster. No test here talks to a cluster with mutual TLS. This
 * repository has no such cluster under test.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8MtlsTest {

  private static final String PASSWORD = "do-not-print-4711";

  private static final String HTTPS_REST = "https://localhost:8443";

  private static final String HTTPS_GRPC = "https://localhost:26500";

  private static KeyPair keyPair;

  private static X509Certificate certificate;

  @TempDir
  static Path directory;

  @BeforeAll
  public static void makeACertificate() throws Exception {

    final var generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    keyPair = generator.generateKeyPair();
    final var name = new X500Name("CN=vanillabp-test");
    final var now = Instant.now();
    final var holder = new JcaX509v3CertificateBuilder(
        name, BigInteger.ONE, Date.from(now.minus(Duration.ofDays(1))), Date
            .from(now.plus(Duration.ofDays(1))), name, keyPair.getPublic())
        .build(new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate()));
    certificate = new JcaX509CertificateConverter().getCertificate(holder);

  }

  private static String pem(
      final Object object) throws Exception {

    final var text = new StringWriter();
    try (var writer = new JcaPEMWriter(text)) {
      writer.writeObject(object);
    }
    return text.toString();

  }

  private static String write(
      final String name,
      final String content) throws Exception {

    return Files
        .writeString(directory.resolve(name), content)
        .toString();

  }

  private static String store(
      final String name,
      final boolean withKey) throws Exception {

    final var store = KeyStore.getInstance("PKCS12");
    store.load(null, null);
    if (withKey) {
      store.setKeyEntry("client", keyPair.getPrivate(), PASSWORD.toCharArray(), new Certificate[]{
          certificate
      });
    } else {
      store.setCertificateEntry("ca", certificate);
    }
    final var path = directory.resolve(name);
    try (var out = Files.newOutputStream(path)) {
      store.store(out, PASSWORD.toCharArray());
    }
    return path.toString();

  }

  private static Camunda8AdapterConfiguration overTls() {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setRestAddress(HTTPS_REST);
    configuration.setGrpcAddress(HTTPS_GRPC);
    return configuration;

  }

  @Test
  @DisplayName("this line has mutual TLS")
  public void thisLineHasMutualTls() {

    assertTrue(Camunda8Mtls.supportedByThisLine());

  }

  @Test
  @DisplayName("an empty block asks nothing of the builder")
  public void anEmptyBlockLeavesTheBuilderAlone() {

    final var builder = mock(CamundaClientBuilder.class, RETURNS_SELF);
    Camunda8Mtls.apply("c8", builder, new Camunda8MtlsConfiguration());
    assertTrue(mockingDetails(builder).getInvocations().isEmpty());

  }

  @Test
  @DisplayName("PEM files, passphrase and a CA file reach their builder methods")
  public void pemFilesReachTheBuilder() {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCertPath("/certs/client.pem");
    mtls.setKeyPath("/certs/client.key");
    mtls.setKeyPassphrase(PASSWORD);
    mtls.setCaPath("/certs/ca.pem");
    final var builder = mock(CamundaClientBuilder.class, RETURNS_SELF);

    Camunda8Mtls.apply("c8", builder, mtls);

    verify(builder).clientCertificate("/certs/client.pem", "/certs/client.key");
    verify(builder).clientKeyPassphrase(PASSWORD);
    verify(builder).mtlsCaCertificatePath("/certs/ca.pem");
    verifyNoMoreInteractions(builder);

  }

  @Test
  @DisplayName("PEM content and a CA as content reach their builder methods")
  public void pemContentReachesTheBuilder() {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCert("cert-pem");
    mtls.setKey("key-pem");
    mtls.setCa("ca-pem");
    final var builder = mock(CamundaClientBuilder.class, RETURNS_SELF);

    Camunda8Mtls.apply("c8", builder, mtls);

    verify(builder).clientCertificateContent("cert-pem", "key-pem");
    verify(builder).mtlsCaCertificateContent("ca-pem");
    verifyNoMoreInteractions(builder);

  }

  @Test
  @DisplayName("a keystore and a truststore reach their builder methods with their passwords")
  public void storesReachTheBuilder() {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setKeystorePath("/certs/client.p12");
    mtls.setKeystorePassword("store");
    mtls.setKeystoreKeyPassword("key");
    mtls.setTruststorePath("/certs/trust.jks");
    final var builder = mock(CamundaClientBuilder.class, RETURNS_SELF);

    Camunda8Mtls.apply("c8", builder, mtls);

    verify(builder).clientKeystore("/certs/client.p12", "store", "key");
    verify(builder).mtlsTruststore("/certs/trust.jks", null);
    verifyNoMoreInteractions(builder);

  }

  @Test
  @DisplayName("a client built from PEM files carries them")
  public void aClientIsBuiltFromPemFiles() throws Exception {

    final var configuration = overTls();
    final var mtls = configuration.getMtls();
    mtls.setCertPath(write("client.pem", pem(certificate)));
    mtls.setKeyPath(write("client.key", pem(keyPair.getPrivate())));
    mtls.setCaPath(write("ca.pem", pem(certificate)));

    configuration.validateMtls("c8");
    try (var factory = new Camunda8ClientFactory("c8", configuration)) {

      final var client = factory.getClient().getConfiguration();
      assertEquals(mtls.getCertPath(), client.getMtlsCertPath());
      assertEquals(mtls.getKeyPath(), client.getMtlsKeyPath());
      assertEquals(mtls.getCaPath(), client.getMtlsCaPath());
    }

  }

  @Test
  @DisplayName("a client built from a keystore and a truststore carries both")
  public void aClientIsBuiltFromStores() throws Exception {

    final var configuration = overTls();
    final var mtls = configuration.getMtls();
    mtls.setKeystorePath(store("client.p12", true));
    mtls.setKeystorePassword(PASSWORD);
    mtls.setTruststorePath(store("trust.p12", false));
    mtls.setTruststorePassword(PASSWORD);

    configuration.validateMtls("c8");
    try (var factory = new Camunda8ClientFactory("c8", configuration)) {

      final var client = factory.getClient().getConfiguration();
      assertEquals(mtls.getKeystorePath(), client.getMtlsKeystorePath());
      assertEquals(mtls.getTruststorePath(), client.getMtlsTruststorePath());
    }

  }

  @Test
  @DisplayName("the factory refuses a block the start would refuse")
  public void theFactoryValidatesTheBlock() {

    final var configuration = overTls();
    configuration.getMtls().setCertPath(directory.resolve("not-mounted.pem").toString());
    configuration.getMtls().setKeyPath(directory.resolve("not-mounted.key").toString());

    final var message = assertThrows(
        IllegalStateException.class,
        () -> new Camunda8ClientFactory("c8", configuration).close())
        .getMessage();
    assertTrue(message.contains("vanillabp.adapters.c8.mtls.cert-path"), message);

  }

  @Test
  @DisplayName("a CAMUNDA_MTLS_* variable which replaced a configured file is named")
  public void anEnvironmentVariableWhichReplacedAFileIsNamed() {

    final var mtls = new Camunda8MtlsConfiguration();
    mtls.setCaPath("/certs/ca.pem");
    final var client = mock(CamundaClientConfiguration.class);
    when(client.getMtlsCaPath()).thenReturn("/elsewhere/ca.pem");
    final var environment = Map.of(
        "CAMUNDA_MTLS_CA_PATH", "/elsewhere/ca.pem",
        "CAMUNDA_MTLS_KEY_PASSPHRASE", PASSWORD);

    final var overrides = Camunda8Mtls.environmentOverrides("c8", mtls, client, environment::get);

    assertEquals(1, overrides.size(), overrides.toString());
    assertEquals("CAMUNDA_MTLS_CA_PATH", overrides.getFirst().variable());
    assertEquals("vanillabp.adapters.c8.mtls.ca-path", overrides.getFirst().propertyKey());
    assertEquals("/elsewhere/ca.pem", overrides.getFirst().effective());
    assertFalse(overrides.toString().contains(PASSWORD), "secrets are not compared");

  }

}
