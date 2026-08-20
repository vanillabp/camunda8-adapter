package io.vanillabp.camunda8.client;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.Setter;

/**
 * How one Camunda 8 adapter instance proves who it is - the block
 * <code>vanillabp.adapters.&lt;id&gt;.auth.*</code> (story 88).
 * <p>
 * A credential belongs to the connection and not to a workflow, so this block lives at
 * the adapter level only; there is nothing to resolve per workflow module, workflow or
 * task.
 * <p>
 * Three methods, the same three the Camunda client knows: {@link Method#NONE},
 * {@link Method#BASIC} and {@link Method#OIDC}. An absent {@link #method} is detected
 * from the keys which are set, and the detection is REPORTED, because a silent fallback
 * to <code>none</code> against a cluster which wants credentials is precisely the
 * failure this block exists for.
 * <p>
 * Client certificates for the cluster connection are not among the methods: the Camunda
 * client has no keystore of its own for gRPC or REST on any supported release line -
 * neither a builder method, nor a client property, nor an environment variable. The
 * keystore and truststore keys here are the ones the client does have, and they belong
 * to the token request against the authorization server. What the cluster connection
 * offers is {@link #caCertificatePath} and the adapter's <code>override-authority</code>.
 * <p>
 * Messages built here name property KEYS and never values: no secret and no length of
 * one ever reaches a log line.
 */
@Getter
@Setter
public class Camunda8AuthConfiguration {

  /**
   * How an adapter instance authenticates against its cluster.
   */
  public enum Method {
    /** No credentials are sent. */
    NONE,
    /** Username and password, sent as an HTTP basic authentication header. */
    BASIC,
    /** An OIDC client fetching a token, cached and refreshed by the client. */
    OIDC
  }

  /**
   * The audience a Camunda 8 SaaS cluster expects, and the token endpoint of Camunda's
   * login service. A SaaS adapter is an OIDC adapter with these two presets, which is
   * why both modes share one code path - see {@link Camunda8Authentication}.
   */
  public static final String SAAS_AUDIENCE = "zeebe.camunda.io";

  /**
   * @see #SAAS_AUDIENCE
   */
  public static final String SAAS_AUTHORIZATION_SERVER_URL = "https://login.cloud.camunda.io/oauth/token";

  /**
   * The method to use, or <code>null</code> for auto-detection from the keys which are
   * set (see {@link #resolveMethod(Camunda8AdapterConfiguration.Mode)}).
   */
  private Method method;

  private String username;

  private String password;

  private String clientId;

  private String clientSecret;

  private String authorizationServerUrl;

  private String audience;

  private String scope;

  /**
   * Where the client keeps the tokens it fetched, so a restart does not fetch a new one
   * while the old is still valid. Default: the client's
   * <code>${user.home}/.camunda/credentials</code>.
   */
  private String credentialsCachePath;

  /**
   * How long connecting to the authorization server may take. Default: the client's 5
   * seconds.
   */
  private java.time.Duration connectTimeout;

  /**
   * How long reading the token response may take. Default: the client's 5 seconds.
   */
  private java.time.Duration readTimeout;

  /**
   * The keystore holding the client certificate the AUTHORIZATION SERVER asks for (not
   * the cluster - see the class comment).
   */
  private String keystorePath;

  private String keystorePassword;

  private String keystoreKeyPassword;

  /**
   * The truststore the AUTHORIZATION SERVER's certificate is verified against (not the
   * cluster's - see the class comment).
   */
  private String truststorePath;

  private String truststorePassword;

  /**
   * The certificate authority the CLUSTER's TLS certificate is verified against, for a
   * gateway with a certificate the JVM's truststore does not know. Independent of the
   * method: a cluster may want TLS without wanting credentials, and the other way round.
   */
  private String caCertificatePath;

  /**
   * Whether NO authentication key is set at all - an adapter configured before this
   * block existed.
   *
   * @return Whether the authentication configuration is entirely absent
   */
  public boolean isAbsent() {

    return (method == null) && !isConfigured();

  }

  /**
   * Whether any key of this block other than {@link #method} carries a value.
   *
   * @return Whether something is configured
   */
  public boolean isConfigured() {

    return hasCredentialKeys() || hasText(caCertificatePath);

  }

  /**
   * Whether a key is set which only a credential-sending method uses. The CA certificate
   * is deliberately not among them: verifying the cluster's TLS certificate is a question
   * of the transport, and a cluster may want TLS without wanting credentials.
   *
   * @return Whether a credential key carries a value
   */
  private boolean hasCredentialKeys() {

    return hasText(username) || hasText(password) || hasText(clientId) || hasText(clientSecret) || hasText(
        authorizationServerUrl) || hasText(audience) || hasText(scope) || hasText(
            credentialsCachePath) || (connectTimeout != null) || (readTimeout != null) || hasText(
                keystorePath) || hasText(keystorePassword) || hasText(keystoreKeyPassword) || hasText(
                    truststorePath) || hasText(truststorePassword);

  }

  /**
   * Whether the method was named explicitly rather than detected.
   *
   * @return Whether <code>auth.method</code> is set
   */
  public boolean isMethodExplicit() {

    return method != null;

  }

  /**
   * The method this adapter instance uses: the configured one, or the one its keys
   * imply.
   * <p>
   * Detection order, most specific first: a basic key names basic, an OIDC key names
   * oidc, a SaaS adapter is oidc through its presets, and everything else is
   * <code>none</code>. Two methods configured at once are not detectable and fail the
   * boot (see {@link #validate(String, Camunda8AdapterConfiguration.Mode)}).
   *
   * @param mode The adapter's connection mode
   * @return The method in use, never <code>null</code>
   */
  public Method resolveMethod(
      final Camunda8AdapterConfiguration.Mode mode) {

    if (method != null) {
      return method;
    }
    if (hasBasicKeys()) {
      return Method.BASIC;
    }
    if (hasOidcKeys() || (mode == Camunda8AdapterConfiguration.Mode.SAAS)) {
      return Method.OIDC;
    }
    return Method.NONE;

  }

  /**
   * The keys of the resolved method which are missing (relative to
   * <code>vanillabp.adapters.&lt;id&gt;.auth.</code>). An empty list means the method
   * can be built.
   *
   * @param saas Whether the adapter runs in SaaS mode, where the connection keys supply
   *          the OIDC client and the presets supply audience and authorization server
   * @return The missing keys, in the order they belong into a configuration file
   */
  public List<String> missingProperties(
      final boolean saas) {

    final var mode = saas
        ? Camunda8AdapterConfiguration.Mode.SAAS
        : Camunda8AdapterConfiguration.Mode.SELF_MANAGED;
    final var missing = new LinkedList<String>();
    switch (resolveMethod(mode)) {
      case BASIC -> {
        if (!hasText(username)) {
          missing.add("auth.username");
        }
        if (!hasText(password)) {
          missing.add("auth.password");
        }
      }
      case OIDC -> {
        // in SaaS mode the connection keys ARE the OIDC client, so nothing is missing
        // here which the connection validation does not already report
        if (!saas) {
          if (!hasText(clientId)) {
            missing.add("auth.client-id");
          }
          if (!hasText(clientSecret)) {
            missing.add("auth.client-secret");
          }
          if (!hasText(authorizationServerUrl)) {
            // without one the client silently points at Camunda's SaaS login, which a
            // self-managed setup never means
            missing.add("auth.authorization-server-url");
          }
          if (!hasText(audience)) {
            missing.add("auth.audience");
          }
        }
      }
      case NONE -> {
      }
    }
    return missing;

  }

  /**
   * Validates the authentication block of one adapter instance AT STARTUP. Every defect
   * it reports is one a boot cannot recover from: a method whose credentials are
   * incomplete cannot be built, and a credential which will never be sent is a key
   * somebody wrote for nothing.
   *
   * @param adapterId The adapter id
   * @param mode The adapter's connection mode
   * @throws IllegalStateException Naming the method, the missing keys with their full
   *           paths and the YAML which completes them
   */
  public void validate(
      final String adapterId,
      final Camunda8AdapterConfiguration.Mode mode) {

    final var saas = mode == Camunda8AdapterConfiguration.Mode.SAAS;

    if ((method == null) && hasBasicKeys() && hasOidcKeys()) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' has both basic and OIDC credentials configured, so the method \
              cannot be detected. Name it, and remove the keys of the other one:
                %s: basic   # or oidc"""
              .formatted(adapterId, propertyKey(adapterId, "auth.method")));
    }

    final var resolved = resolveMethod(mode);

    if ((resolved == Method.NONE) && hasCredentialKeys()) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' authenticates with 'none' while credentials are configured below \
              '%s' - they would never be sent. Either name the method they belong to:
                %s: basic   # or oidc
              or remove the keys."""
              .formatted(
                  adapterId,
                  propertyKey(adapterId, "auth"),
                  propertyKey(adapterId, "auth.method")));
    }

    if ((resolved == Method.BASIC) && saas) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' runs in mode 'saas' and would authenticate with 'basic'. Camunda 8 \
              SaaS accepts an OIDC client only, configured with the connection keys '%s' and '%s'. \
              Remove '%s', or switch the adapter to 'mode: self-managed'."""
              .formatted(
                  adapterId,
                  propertyKey(adapterId, "client-id"),
                  propertyKey(adapterId, "client-secret"),
                  propertyKey(adapterId, "auth.method")));
    }

    final var missing = missingProperties(saas);
    if (!missing.isEmpty()) {
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' authenticates with '%s' but these properties are missing:
                %s
              Complete the block:
              %s"""
              .formatted(
                  adapterId,
                  resolved.name().toLowerCase(),
                  missing
                      .stream()
                      .map(key -> propertyKey(adapterId, key))
                      .collect(Collectors.joining("\n  ")),
                  exampleYaml(adapterId, resolved)));
    }

    requireReadableFile(adapterId, "auth.keystore-path", keystorePath);
    requireReadableFile(adapterId, "auth.truststore-path", truststorePath);
    requireReadableFile(adapterId, "auth.ca-certificate-path", caCertificatePath);

  }

  /**
   * The minimal YAML of a method, indented for a message. It is the second half of every
   * complaint this class makes: naming what is missing without showing where it goes
   * sends the reader to the documentation, which is what these messages are here to
   * avoid.
   *
   * @param adapterId The adapter id
   * @param method The method
   * @return The YAML block
   */
  public static String exampleYaml(
      final String adapterId,
      final Method method) {

    return switch (method) {
      case BASIC -> """
          vanillabp:
            adapters:
              %s:
                auth:
                  method: basic
                  username: <the cluster's user>
                  password: <its password>"""
          .formatted(adapterId);
      case OIDC -> """
          vanillabp:
            adapters:
              %s:
                auth:
                  method: oidc
                  client-id: <the OIDC client>
                  client-secret: <its secret>
                  authorization-server-url: https://<your identity provider>/protocol/openid-connect/token
                  audience: <what the cluster expects, e.g. zeebe-api>"""
          .formatted(adapterId);
      case NONE -> """
          vanillabp:
            adapters:
              %s:
                auth:
                  method: none"""
          .formatted(adapterId);
    };

  }

  /**
   * Who this adapter instance authenticates as - a user name or a client id, never a
   * secret. Used where two adapter ids on ONE cluster have to be told apart
   * ({@link Camunda8InstanceIdentity}) and in the startup line.
   *
   * @param configuration The adapter's connection configuration (SaaS keys serve as the
   *          OIDC client)
   * @return The principal or <code>null</code> where there is none
   */
  public String principal(
      final Camunda8AdapterConfiguration configuration) {

    return switch (resolveMethod(configuration.getMode())) {
      case BASIC -> username;
      case OIDC -> hasText(clientId)
          ? clientId
          : configuration.getClientId();
      case NONE -> null;
    };

  }

  private boolean hasBasicKeys() {

    return hasText(username) || hasText(password);

  }

  private boolean hasOidcKeys() {

    return hasText(clientId) || hasText(clientSecret) || hasText(authorizationServerUrl) || hasText(
        audience) || hasText(scope);

  }

  private static void requireReadableFile(
      final String adapterId,
      final String key,
      final String path) {

    if (!hasText(path)) {
      return;
    }
    final var file = java.nio.file.Paths.get(path);
    if (java.nio.file.Files.isReadable(file)) {
      return;
    }
    throw new IllegalStateException(
        "Camunda 8 adapter '%s' names '%s: %s', but no readable file is there. Check the path, and on a container check that the secret or config map is mounted before the application starts."
            .formatted(adapterId, propertyKey(adapterId, key), path));

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
