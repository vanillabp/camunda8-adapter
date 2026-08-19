package io.vanillabp.camunda8;

import java.io.IOException;
import java.util.Properties;

/**
 * The release line this build of the adapter belongs to.
 * <p>
 * The adapter is published once per line, e.g. <code>2.1.0-8.9</code>, because Camunda
 * promises a client only against clusters of its own version and newer: the client a build
 * was compiled against is the lowest cluster version that build accepts. A line therefore
 * carries every VanillaBP fix to users who cannot upgrade their cluster yet, and the newest
 * line carries what only the newest cluster can do.
 * <p>
 * The line reaches the runtime for MESSAGES, not for behaviour. What the adapter does
 * follows what the cluster answers, never what the build believes, which is why nothing
 * here decides anything. A message saying "this cluster cannot do X" is easier to act on
 * when it also names the line the application is running.
 * <p>
 * The values are filtered into the adapter's version descriptor
 * <code>META-INF/vanillabp/adapter-camunda8.properties</code> by the Maven build, from the
 * properties the active line profile selects. Where they cannot be read the answer is
 * <code>unknown</code>: this must never break an application, a custom build included.
 */
public final class Camunda8ReleaseLine {

  private static final String DESCRIPTOR = "/META-INF/vanillabp/adapter-camunda8.properties";

  private static final String UNKNOWN = "unknown";

  private static final Properties DESCRIPTOR_PROPERTIES = read();

  private Camunda8ReleaseLine() {
    // static helper
  }

  /**
   * @return The Camunda 8 minor this build belongs to, e.g. <code>8.9</code>, which is the
   *         lowest cluster version it accepts, or <code>unknown</code>.
   */
  public static String id() {

    return DESCRIPTOR_PROPERTIES.getProperty("camunda8.line", UNKNOWN);

  }

  /**
   * @return The exact Camunda client this build was compiled against, e.g.
   *         <code>8.9.16</code>, or <code>unknown</code>.
   */
  public static String clientVersion() {

    return DESCRIPTOR_PROPERTIES.getProperty("camunda8.client", UNKNOWN);

  }

  private static Properties read() {

    final var properties = new Properties();
    try (var descriptor = Camunda8ReleaseLine.class.getResourceAsStream(DESCRIPTOR)) {
      if (descriptor != null) {
        properties.load(descriptor);
      }
    } catch (IOException e) {
      // an unreadable descriptor means unknown, never a failure: the line is used in
      // messages only
    }
    return properties;

  }

}
