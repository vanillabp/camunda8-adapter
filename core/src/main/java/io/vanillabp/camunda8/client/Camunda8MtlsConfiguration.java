package io.vanillabp.camunda8.client;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Mutual TLS (mTLS) for the connection to the cluster. This is the block
 * <code>vanillabp.adapters.&lt;id&gt;.mtls.*</code>. With mTLS, the cluster checks a
 * certificate of the client as well, not only the client the certificate of the cluster.
 * <p>
 * The block holds two independent things, and each has alternatives:
 * <ul>
 * <li>The identity of the client, which the cluster checks. It is a certificate chain with
 * its private key, both PEM encoded, given as files (<code>cert-path</code> and
 * <code>key-path</code>) or as content (<code>cert</code> and <code>key</code>). An encrypted
 * key needs <code>key-passphrase</code>. Instead of PEM, a Java keystore can hold the
 * identity (<code>keystore-path</code>, <code>keystore-password</code> and, where the key has
 * a password of its own, <code>keystore-key-password</code>).</li>
 * <li>The certificate authorities which check the certificate of the cluster. They are a PEM
 * bundle, given as a file (<code>ca-path</code>) or as content (<code>ca</code>), or a Java
 * truststore (<code>truststore-path</code> and <code>truststore-password</code>). Whatever is
 * given replaces the truststore of the JVM for the cluster connection.</li>
 * </ul>
 * The keys use the words of Camunda's Spring Boot starter, below
 * <code>camunda.client.mtls</code>. So an application which moves from the starter to
 * VanillaBP finds the same names.
 * <p>
 * The client of the 8.9 line knows these options from 8.9.29 on, the client of the 8.10 line
 * from 8.10.10 on. On the 8.8 line every key of the block ends the start. Ignoring the keys
 * would connect without the certificate the configuration names. Each line's
 * <code>Camunda8Mtls</code> applies the block to the builder.
 * <p>
 * Messages built here name keys, and for files the path. They never name a password, a
 * passphrase or the content of a key.
 * <p>
 * Why these keys exist, why they live in a block of their own and why an older line refuses them
 * is decision 108 in the repository's DECISIONS.md.
 */
public class Camunda8MtlsConfiguration {

  /**
   * The line a PEM block starts with. Content which lacks it is most likely a path written
   * into the key for the content.
   */
  private static final String PEM_BEGIN = "-----BEGIN ";

  /**
   * Opens the mtls block empty. Every adapter section has one, whether the application wrote
   * any of its keys or not.
   */
  public Camunda8MtlsConfiguration() {
  }

  /**
   * The PEM file with the certificate chain of the client. It goes together with
   * {@link #keyPath}.
   */
  private String certPath;

  /**
   * The PEM file with the certificate chain of the client.
   *
   * @return The path, or <code>null</code> where none is configured
   */
  public String getCertPath() {

    return certPath;

  }

  /**
   * The PEM file with the certificate chain of the client.
   *
   * @param certPath The path, or <code>null</code> where none is configured
   */
  public void setCertPath(
      final String certPath) {

    this.certPath = certPath;

  }

  /**
   * The certificate chain of the client as PEM content. It goes together with {@link #key}.
   */
  private String cert;

  /**
   * The certificate chain of the client as PEM content.
   *
   * @return The content, or <code>null</code> where none is configured
   */
  public String getCert() {

    return cert;

  }

  /**
   * The certificate chain of the client as PEM content.
   *
   * @param cert The content, or <code>null</code> where none is configured
   */
  public void setCert(
      final String cert) {

    this.cert = cert;

  }

  /**
   * The PEM file with the private key of the client. The client reads PKCS#8, encrypted or
   * not, PKCS#1 and EC keys.
   */
  private String keyPath;

  /**
   * The PEM file with the private key of the client.
   *
   * @return The path, or <code>null</code> where none is configured
   */
  public String getKeyPath() {

    return keyPath;

  }

  /**
   * The PEM file with the private key of the client.
   *
   * @param keyPath The path, or <code>null</code> where none is configured
   */
  public void setKeyPath(
      final String keyPath) {

    this.keyPath = keyPath;

  }

  /**
   * The private key of the client as PEM content. It is a secret and never logged.
   */
  private String key;

  /**
   * The private key of the client as PEM content.
   *
   * @return The content, or <code>null</code> where none is configured
   */
  public String getKey() {

    return key;

  }

  /**
   * The private key of the client as PEM content.
   *
   * @param key The content, or <code>null</code> where none is configured
   */
  public void setKey(
      final String key) {

    this.key = key;

  }

  /**
   * The passphrase of an encrypted private key, given by file or as content. It is a secret
   * and never logged.
   */
  private String keyPassphrase;

  /**
   * The passphrase of an encrypted private key.
   *
   * @return The passphrase, or <code>null</code> where none is configured
   */
  public String getKeyPassphrase() {

    return keyPassphrase;

  }

  /**
   * The passphrase of an encrypted private key.
   *
   * @param keyPassphrase The passphrase, or <code>null</code> where none is configured
   */
  public void setKeyPassphrase(
      final String keyPassphrase) {

    this.keyPassphrase = keyPassphrase;

  }

  /**
   * The PEM file with the certificate authorities which check the certificate of the
   * cluster.
   */
  private String caPath;

  /**
   * The PEM file with the certificate authorities which check the certificate of the
   * cluster.
   *
   * @return The path, or <code>null</code> where none is configured
   */
  public String getCaPath() {

    return caPath;

  }

  /**
   * The PEM file with the certificate authorities which check the certificate of the
   * cluster.
   *
   * @param caPath The path, or <code>null</code> where none is configured
   */
  public void setCaPath(
      final String caPath) {

    this.caPath = caPath;

  }

  /**
   * The certificate authorities which check the certificate of the cluster, as PEM content.
   */
  private String ca;

  /**
   * The certificate authorities which check the certificate of the cluster, as PEM content.
   *
   * @return The content, or <code>null</code> where none is configured
   */
  public String getCa() {

    return ca;

  }

  /**
   * The certificate authorities which check the certificate of the cluster, as PEM content.
   *
   * @param ca The content, or <code>null</code> where none is configured
   */
  public void setCa(
      final String ca) {

    this.ca = ca;

  }

  /**
   * The Java keystore which holds the identity of the client. A file ending in
   * <code>.p12</code> or <code>.pfx</code> is read as PKCS#12, every other file as JKS.
   */
  private String keystorePath;

  /**
   * The Java keystore which holds the identity of the client.
   *
   * @return The path, or <code>null</code> where none is configured
   */
  public String getKeystorePath() {

    return keystorePath;

  }

  /**
   * The Java keystore which holds the identity of the client.
   *
   * @param keystorePath The path, or <code>null</code> where none is configured
   */
  public void setKeystorePath(
      final String keystorePath) {

    this.keystorePath = keystorePath;

  }

  /**
   * The password that keystore is opened with. It is a secret and never logged.
   */
  private String keystorePassword;

  /**
   * The password that keystore is opened with.
   *
   * @return The password, or <code>null</code> where none is configured
   */
  public String getKeystorePassword() {

    return keystorePassword;

  }

  /**
   * The password that keystore is opened with.
   *
   * @param keystorePassword The password, or <code>null</code> where none is configured
   */
  public void setKeystorePassword(
      final String keystorePassword) {

    this.keystorePassword = keystorePassword;

  }

  /**
   * The password of the private key inside that keystore. Where it is not set, the client
   * uses the password of the keystore. It is a secret and never logged.
   */
  private String keystoreKeyPassword;

  /**
   * The password of the private key inside that keystore.
   *
   * @return The password, or <code>null</code> where none is configured
   */
  public String getKeystoreKeyPassword() {

    return keystoreKeyPassword;

  }

  /**
   * The password of the private key inside that keystore.
   *
   * @param keystoreKeyPassword The password, or <code>null</code> where none is configured
   */
  public void setKeystoreKeyPassword(
      final String keystoreKeyPassword) {

    this.keystoreKeyPassword = keystoreKeyPassword;

  }

  /**
   * The Java truststore with the certificate authorities which check the certificate of the
   * cluster. A file ending in <code>.p12</code> or <code>.pfx</code> is read as PKCS#12, every
   * other file as JKS.
   */
  private String truststorePath;

  /**
   * The Java truststore with the certificate authorities which check the certificate of the
   * cluster.
   *
   * @return The path, or <code>null</code> where none is configured
   */
  public String getTruststorePath() {

    return truststorePath;

  }

  /**
   * The Java truststore with the certificate authorities which check the certificate of the
   * cluster.
   *
   * @param truststorePath The path, or <code>null</code> where none is configured
   */
  public void setTruststorePath(
      final String truststorePath) {

    this.truststorePath = truststorePath;

  }

  /**
   * The password that truststore is opened with, where it has one. It is a secret and never
   * logged.
   */
  private String truststorePassword;

  /**
   * The password that truststore is opened with.
   *
   * @return The password, or <code>null</code> where none is configured
   */
  public String getTruststorePassword() {

    return truststorePassword;

  }

  /**
   * The password that truststore is opened with.
   *
   * @param truststorePassword The password, or <code>null</code> where none is configured
   */
  public void setTruststorePassword(
      final String truststorePassword) {

    this.truststorePassword = truststorePassword;

  }

  /**
   * The keys of this block which are set, relative to
   * <code>vanillabp.adapters.&lt;id&gt;.</code>, in the order they are documented. A password
   * counts as set even where it is empty, because an empty password is a password.
   *
   * @return The keys which are set, empty where the block is not used
   */
  public List<String> configuredKeys() {

    final var keys = new LinkedList<String>();
    addIfText(keys, "mtls.cert-path", certPath);
    addIfText(keys, "mtls.key-path", keyPath);
    addIfText(keys, "mtls.cert", cert);
    addIfText(keys, "mtls.key", key);
    addIfSet(keys, "mtls.key-passphrase", keyPassphrase);
    addIfText(keys, "mtls.keystore-path", keystorePath);
    addIfSet(keys, "mtls.keystore-password", keystorePassword);
    addIfSet(keys, "mtls.keystore-key-password", keystoreKeyPassword);
    addIfText(keys, "mtls.ca-path", caPath);
    addIfText(keys, "mtls.ca", ca);
    addIfText(keys, "mtls.truststore-path", truststorePath);
    addIfSet(keys, "mtls.truststore-password", truststorePassword);
    return keys;

  }

  /**
   * Whether any key of this block is set.
   *
   * @return Whether the block is used
   */
  public boolean isConfigured() {

    return !configuredKeys().isEmpty();

  }

  /**
   * The start of a release line whose client has no mTLS ends here. Ignoring the keys would
   * connect without the certificate they name, and the cluster would refuse the connection
   * with a TLS error which names nothing of this configuration.
   *
   * @param adapterId The adapter id
   * @param line The release line of this build, e.g. <code>8.8</code>
   * @return The exception which ends the start, naming the keys which are set
   */
  public IllegalStateException notOnThisLine(
      final String adapterId,
      final String line) {

    return new IllegalStateException(
        """
            Camunda 8 adapter '%s' configures mutual TLS, but the Camunda client of the release line %s \
            has no option for it:
              %s
            The client knows these options from 8.9.29 on, so they need the 8.9 line of this adapter or a \
            newer one. Use such a line together with a cluster of the same version, or remove the keys."""
            .formatted(
                adapterId,
                line,
                configuredKeys()
                    .stream()
                    .map(key -> propertyKey(adapterId, key))
                    .collect(Collectors.joining("\n  "))));

  }

  /**
   * Validates the block AT STARTUP. Each rule is one the client checks only when it builds
   * its connection, or not at all. The client's own messages name its environment variables,
   * so an application configured through VanillaBP keys would not learn which key is wrong.
   *
   * @param adapterId The adapter id
   * @param legacyCaCertificatePath The value of <code>auth.ca-certificate-path</code>, which
   *          is the older way to name the certificate authority of the cluster
   * @param addressesInUse The addresses the client talks to, REST and gRPC, or
   *          <code>null</code> for Camunda 8 SaaS, which the client always reaches with TLS
   * @throws IllegalStateException Naming the keys which do not fit together and the way out
   */
  public void validate(
      final String adapterId,
      final String legacyCaCertificatePath,
      final List<String> addressesInUse) {

    if (!isConfigured()) {
      return;
    }

    requirePair(adapterId, "mtls.cert-path", certPath, "mtls.key-path", keyPath);
    requirePair(adapterId, "mtls.cert", cert, "mtls.key", key);

    final var identities = new LinkedList<String>();
    if (hasText(certPath) || hasText(keyPath)) {
      identities.add("'%s' with '%s'".formatted(
          propertyKey(adapterId, "mtls.cert-path"),
          propertyKey(adapterId, "mtls.key-path")));
    }
    if (hasText(cert) || hasText(key)) {
      identities.add("'%s' with '%s'".formatted(
          propertyKey(adapterId, "mtls.cert"),
          propertyKey(adapterId, "mtls.key")));
    }
    if (hasText(keystorePath)) {
      identities.add("'%s'".formatted(propertyKey(adapterId, "mtls.keystore-path")));
    }
    if (identities.size() > 1) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' gives the client certificate for mutual TLS more than once:
                %s
              The client presents one identity. Keep one of them and remove the others."""
              .formatted(adapterId, String.join("\n  ", identities)));
    }

    if ((keyPassphrase != null) && !hasText(keyPath) && !hasText(key)) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' sets '%s', but no private key in PEM format which it could open. \
              The passphrase belongs to the key in '%s' or '%s'. A key in a keystore is opened with \
              '%s' instead."""
              .formatted(
                  adapterId,
                  propertyKey(adapterId, "mtls.key-passphrase"),
                  propertyKey(adapterId, "mtls.key-path"),
                  propertyKey(adapterId, "mtls.key"),
                  propertyKey(adapterId, "mtls.keystore-key-password")));
    }

    if (hasText(keystorePath) && (keystorePassword == null)) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' sets '%s' but not '%s'. Without the password the client cannot read \
              the private key out of the keystore. Add it, and add '%s' as well where the key has a \
              password of its own."""
              .formatted(
                  adapterId,
                  propertyKey(adapterId, "mtls.keystore-path"),
                  propertyKey(adapterId, "mtls.keystore-password"),
                  propertyKey(adapterId, "mtls.keystore-key-password")));
    }
    if (!hasText(keystorePath) && ((keystorePassword != null) || (keystoreKeyPassword != null))) {
      throw new IllegalStateException(
          "Camunda 8 adapter '%s' sets a password of a keystore, but not '%s'. Add the path of the keystore, or remove the password."
              .formatted(adapterId, propertyKey(adapterId, "mtls.keystore-path")));
    }
    if (!hasText(truststorePath) && (truststorePassword != null)) {
      throw new IllegalStateException(
          "Camunda 8 adapter '%s' sets '%s', but not '%s'. Add the path of the truststore, or remove the password."
              .formatted(
                  adapterId,
                  propertyKey(adapterId, "mtls.truststore-password"),
                  propertyKey(adapterId, "mtls.truststore-path")));
    }

    final var authorities = new LinkedList<String>();
    if (hasText(caPath)) {
      authorities.add(propertyKey(adapterId, "mtls.ca-path"));
    }
    if (hasText(ca)) {
      authorities.add(propertyKey(adapterId, "mtls.ca"));
    }
    if (hasText(truststorePath)) {
      authorities.add(propertyKey(adapterId, "mtls.truststore-path"));
    }
    if (!authorities.isEmpty() && hasText(legacyCaCertificatePath)) {
      authorities.add(propertyKey(adapterId, "auth.ca-certificate-path"));
    }
    if (authorities.size() > 1) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' names the certificate authorities of the cluster more than once:
                %s
              The client takes one source for them. Keep one and remove the others. '%s' does the same \
              as '%s' and is kept for configurations written before the mtls block existed."""
              .formatted(
                  adapterId,
                  String.join("\n  ", authorities),
                  propertyKey(adapterId, "auth.ca-certificate-path"),
                  propertyKey(adapterId, "mtls.ca-path")));
    }

    requirePem(adapterId, "mtls.cert", cert);
    requirePem(adapterId, "mtls.key", key);
    requirePem(adapterId, "mtls.ca", ca);

    requireReadableFile(adapterId, "mtls.cert-path", certPath);
    requireReadableFile(adapterId, "mtls.key-path", keyPath);
    requireReadableFile(adapterId, "mtls.keystore-path", keystorePath);
    requireReadableFile(adapterId, "mtls.ca-path", caPath);
    requireReadableFile(adapterId, "mtls.truststore-path", truststorePath);

    if ((addressesInUse != null) && addressesInUse
        .stream()
        .noneMatch(Camunda8MtlsConfiguration::usesTls)) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' configures mutual TLS, but talks to the cluster without TLS: %s. \
              Write the addresses with 'https://' in '%s' and '%s'."""
              .formatted(
                  adapterId,
                  String.join(" and ", addressesInUse),
                  propertyKey(adapterId, "rest-address"),
                  propertyKey(adapterId, "grpc-address")));
    }

  }

  /**
   * Whether the client talks TLS to this address. It reads the scheme the way the client
   * does. An address the client cannot read counts as TLS here, because the client fails on
   * it with a message of its own.
   */
  private static boolean usesTls(
      final String address) {

    final String scheme;
    try {
      scheme = URI
          .create(address)
          .getScheme();
    } catch (IllegalArgumentException e) {
      return true;
    }
    return (scheme == null) || !List
        .of("http", "grpc")
        .contains(scheme.toLowerCase(Locale.ROOT));

  }

  private static void requirePair(
      final String adapterId,
      final String certificateKey,
      final String certificate,
      final String privateKeyKey,
      final String privateKey) {

    if (hasText(certificate) == hasText(privateKey)) {
      return;
    }
    final var given = hasText(certificate)
        ? certificateKey
        : privateKeyKey;
    final var missing = hasText(certificate)
        ? privateKeyKey
        : certificateKey;
    throw new IllegalStateException(
        """
            Camunda 8 adapter '%s' sets '%s' but not '%s'. The client certificate for mutual TLS needs \
            the certificate and its private key, given the same way: '%s' with '%s', or '%s' with '%s'."""
            .formatted(
                adapterId,
                propertyKey(adapterId, given),
                propertyKey(adapterId, missing),
                propertyKey(adapterId, "mtls.cert-path"),
                propertyKey(adapterId, "mtls.key-path"),
                propertyKey(adapterId, "mtls.cert"),
                propertyKey(adapterId, "mtls.key")));

  }

  private static void requirePem(
      final String adapterId,
      final String key,
      final String content) {

    if (!hasText(content) || content.contains(PEM_BEGIN)) {
      return;
    }
    // the value is not repeated, it may be a private key
    throw new IllegalStateException(
        """
            Camunda 8 adapter '%s' sets '%s', but its value is not PEM content: there is no line \
            starting with '%s'. To name a file, use '%s-path' instead."""
            .formatted(
                adapterId,
                propertyKey(adapterId, key),
                PEM_BEGIN.trim(),
                propertyKey(adapterId, key)));

  }

  private static void requireReadableFile(
      final String adapterId,
      final String key,
      final String path) {

    if (!hasText(path) || Files.isReadable(Paths.get(path))) {
      return;
    }
    throw new IllegalStateException(
        "Camunda 8 adapter '%s' names '%s: %s', but no readable file is there. Check the path, and on a container check that the secret or config map is mounted before the application starts."
            .formatted(adapterId, propertyKey(adapterId, key), path));

  }

  private static void addIfText(
      final List<String> keys,
      final String key,
      final String value) {

    if (hasText(value)) {
      keys.add(key);
    }

  }

  private static void addIfSet(
      final List<String> keys,
      final String key,
      final String value) {

    if (value != null) {
      keys.add(key);
    }

  }

  private static String propertyKey(
      final String adapterId,
      final String key) {

    return Camunda8AdapterConfiguration.propertyKey(adapterId, key);

  }

  private static boolean hasText(
      final String value) {

    return (value != null) && !value.isBlank();

  }

}
