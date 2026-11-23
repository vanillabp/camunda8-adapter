package io.vanillabp.camunda8.client;

import java.util.Optional;
import java.util.regex.Pattern;

import io.vanillabp.camunda8.Camunda8ReleaseLine;
import lombok.extern.slf4j.Slf4j;

/**
 * Compares the version of the cluster with the client this build was compiled against. It
 * does so once per start and writes one line about the pair.
 * <p>
 * Only the minor version counts. A patch level does not change what Camunda support accepts,
 * so client 8.10.5 against cluster 8.10.0 is a match.
 * <ul>
 * <li>Same minor version: INFO.</li>
 * <li>One minor version apart, in either direction: WARN. Camunda support accepts a client one
 * minor version older than its cluster. That is the state during an upgrade, and an upgrade
 * should end. So the operator reads the line at every start.</li>
 * <li>More than one minor version apart: ERROR, because then some calls may fail.</li>
 * </ul>
 * Nothing here ends the start. Otherwise production could stand still only because somebody
 * forgot to update a client. Every line names the release line of the adapter which matches the
 * cluster, so the operator knows what to change.
 * <p>
 * The version comes from the topology request with which the start waits for its cluster
 * ({@link Camunda8ClusterWait}). So the check costs no request of its own.
 * <p>
 * Why the pair is compared and never refused is decision 90 in the repository's DECISIONS.md.
 * <p>
 * The same answer says one more thing. An adapter which leases its jobs on a cluster of 8.10
 * older than {@value #FIRST_8_10_WITHOUT_THE_LEASE_STALL} gets a WARN. The gateway of such a
 * cluster can strand a leased job for one job timeout. Why the advice depends on the version of
 * the cluster is {@code DECISIONS.pending/2194.md}.
 */
@Slf4j
public final class Camunda8ClusterVersionCheck {

  /**
   * Major and minor at the start of a version. What follows them, a patch level or a
   * pre-release like <code>-alpha2</code>, does not count.
   */
  private static final Pattern MAJOR_AND_MINOR = Pattern.compile("^\\s*(\\d+)\\.(\\d+)(?:[.\\-+].*)?\\s*$");

  /**
   * Major, minor and patch at the start of a version. A pre-release like
   * <code>8.10.0-alpha5</code> counts as its patch level, so it is older than any release
   * with a higher patch.
   */
  private static final Pattern MAJOR_MINOR_AND_PATCH = Pattern
      .compile("^\\s*(\\d+)\\.(\\d+)\\.(\\d+)(?:[\\-+].*)?\\s*$");

  /**
   * The first cluster of the 8.10 line whose gateway gives a leased job back when it could not
   * hand it out. Every earlier 8.10 cluster leaves such a job locked for nobody until its lock
   * runs out.
   */
  public static final String FIRST_8_10_WITHOUT_THE_LEASE_STALL = "8.10.10";

  /** The patch level of {@link #FIRST_8_10_WITHOUT_THE_LEASE_STALL}. */
  private static final int FIRST_PATCH_WITHOUT_THE_LEASE_STALL = 10;

  private Camunda8ClusterVersionCheck() {
  }

  /**
   * How serious a pair of versions is. It is the log level of the line.
   */
  enum Severity {
    INFO,
    WARN,
    ERROR
  }

  /**
   * What the check says about a pair of versions.
   *
   * @param severity The log level of the line
   * @param message The line itself
   */
  record Verdict(
                 Severity severity,
                 String message) {
  }

  /**
   * Writes the verdict about the cluster which just answered.
   *
   * @param adapterId The adapter instance
   * @param clusterVersion The gateway version the cluster named in its topology
   */
  public static void logTheVerdict(
      final String adapterId,
      final String clusterVersion) {

    final var verdict = judge(
        adapterId,
        clusterVersion,
        Camunda8ReleaseLine.clientVersion(),
        Camunda8ReleaseLine.id());
    switch (verdict.severity()) {
      case INFO -> log.info(verdict.message());
      case WARN -> log.warn(verdict.message());
      case ERROR -> log.error(verdict.message());
    }

  }

  /**
   * Writes a WARN where this adapter leases its jobs on a cluster whose gateway can strand a
   * leased job. It writes nothing otherwise.
   *
   * @param adapterId The adapter instance
   * @param clusterVersion The gateway version the cluster named in its topology
   * @param leasesItsJobs Whether this adapter leases, see
   *          {@link Camunda8AdapterConfiguration#leasesItsJobs()}
   */
  public static void logTheLeaseAdvice(
      final String adapterId,
      final String clusterVersion,
      final boolean leasesItsJobs) {

    leaseAdvice(adapterId, clusterVersion, leasesItsJobs).ifPresent(log::warn);

  }

  /**
   * What the start says about a lease on this cluster. It reads nothing but its arguments,
   * so a test can name any version.
   *
   * @param adapterId The adapter instance
   * @param clusterVersion The gateway version the cluster named, may be <code>null</code>
   * @param leasesItsJobs Whether this adapter leases its jobs
   * @return The WARN line, or nothing where a lease costs nothing on this cluster or the
   *         adapter does not lease
   */
  static Optional<String> leaseAdvice(
      final String adapterId,
      final String clusterVersion,
      final boolean leasesItsJobs) {

    if (!leasesItsJobs || !strandsALeasedJob(clusterVersion)) {
      return Optional.empty();
    }
    return Optional.of(
        ("Camunda8[%s]: '%s' is 'use', and cluster %s is older than %s. The gateway of such a cluster sometimes "
            + "activates a leased job for a long poll which has just ended. It cannot give that job back, so the "
            + "job waits one '%s' for nobody. Upgrade the cluster to %s or later. Switching to 'do-not-use' "
            + "instead only helps an application which has not leased a job yet, because a leased job never "
            + "reaches a worker which does not lease.")
            .formatted(
                adapterId,
                Camunda8AdapterConfiguration.propertyKey(adapterId, "job-lease"),
                clusterVersion,
                FIRST_8_10_WITHOUT_THE_LEASE_STALL,
                Camunda8AdapterConfiguration.propertyKey(adapterId, "job-timeout"),
                FIRST_8_10_WITHOUT_THE_LEASE_STALL));

  }

  /**
   * Whether the version names a cluster of 8.10 older than
   * {@link #FIRST_8_10_WITHOUT_THE_LEASE_STALL}. A version which cannot be read says nothing,
   * so it is <code>false</code>.
   */
  private static boolean strandsALeasedJob(
      final String clusterVersion) {

    if (clusterVersion == null) {
      return false;
    }
    final var matcher = MAJOR_MINOR_AND_PATCH.matcher(clusterVersion);
    if (!matcher.matches()) {
      return false;
    }
    try {
      return (Integer.parseInt(matcher.group(1)) == 8) && (Integer.parseInt(matcher.group(2)) == 10) && (Integer
          .parseInt(matcher.group(3)) < FIRST_PATCH_WITHOUT_THE_LEASE_STALL);
    } catch (final NumberFormatException e) {
      // digits beyond the range of an int are no version anyone ships
      return false;
    }

  }

  /**
   * The line for a start which does not ask the cluster anything before it goes on. That is
   * a start with <code>startup-wait: PT0S</code>. It still says what the cluster should be.
   *
   * @param adapterId The adapter instance
   */
  public static void logThatNothingWasCompared(
      final String adapterId) {

    log.info(
        "Camunda8[{}]: '{}' is PT0S, so the start does not ask the cluster for its version. Client {} "
            + "of adapter release line {} expects a cluster {}.x. The health check shows the version of the "
            + "cluster as 'gatewayVersion'.",
        adapterId,
        Camunda8AdapterConfiguration.propertyKey(adapterId, "startup-wait"),
        Camunda8ReleaseLine.clientVersion(),
        Camunda8ReleaseLine.id(),
        Camunda8ReleaseLine.id());

  }

  /**
   * The verdict about one pair of versions. It reads nothing but its arguments, so a test
   * can name any pair.
   *
   * @param adapterId The adapter instance
   * @param clusterVersion The gateway version the cluster named, may be <code>null</code>
   * @param clientVersion The client this build was compiled against, may be
   *          <code>unknown</code>
   * @param line The release line of this build, e.g. <code>8.10</code>
   * @return The verdict, never <code>null</code>
   */
  static Verdict judge(
      final String adapterId,
      final String clusterVersion,
      final String clientVersion,
      final String line) {

    final var cluster = majorAndMinor(clusterVersion);
    // the client version is exact. The line is the fallback for a build whose descriptor
    // names no client, and it has the same minor anyway
    final var client = majorAndMinor(clientVersion) != null
        ? majorAndMinor(clientVersion)
        : majorAndMinor(line);
    if ((cluster == null) || (client == null)) {
      return new Verdict(
          Severity.INFO, ("Camunda8[%s]: cannot compare cluster version '%s' with client '%s' of adapter release line "
              + "'%s', so it cannot say whether Camunda supports this pair. Use the adapter release line "
              + "with the minor version of the cluster.")
              .formatted(adapterId, clusterVersion, clientVersion, line));
    }

    final var clusterMinor = cluster[0]
        + "."
        + cluster[1];
    final var clientMinor = client[0]
        + "."
        + client[1];
    final var matchingLine = "use adapter release line %s (versions ending in '-%s')"
        .formatted(clusterMinor, clusterMinor);
    // a different major is never "one minor apart"
    final var distance = cluster[0] == client[0]
        ? cluster[1] - client[1]
        : Integer.MAX_VALUE;

    if (distance == 0) {
      return new Verdict(
          Severity.INFO, "Camunda8[%s]: cluster %s and client %s of adapter release line %s have the same minor version"
              .formatted(adapterId, clusterVersion, clientVersion, line));
    }
    if (distance == 1) {
      return new Verdict(
          Severity.WARN, ("Camunda8[%s]: cluster %s is one minor version newer than client %s of adapter release line "
              + "%s. Camunda support accepts this during a cluster upgrade. When the upgrade is done, %s.")
              .formatted(adapterId, clusterVersion, clientVersion, line, matchingLine));
    }
    if (distance == -1) {
      return new Verdict(
          Severity.WARN, ("Camunda8[%s]: cluster %s is one minor version older than client %s of adapter release line "
              + "%s. Camunda support has not confirmed that a client works against an older cluster, and "
              + "some calls may be refused. Either %s, or upgrade the cluster to %s.")
              .formatted(adapterId, clusterVersion, clientVersion, line, matchingLine, clientMinor));
    }
    final var direction = cluster[0] > client[0] || (cluster[0] == client[0] && distance > 0)
        ? "newer"
        : "older";
    return new Verdict(
        Severity.ERROR, ("Camunda8[%s]: cluster %s is more than one minor version %s than client %s of adapter release "
            + "line %s. Camunda does not support this pair, so some calls may fail. The start goes on "
            + "anyway. %s%s.")
            .formatted(
                adapterId,
                clusterVersion,
                direction,
                clientVersion,
                line,
                "older".equals(direction) ? "Either " : "To fix it, ",
                "older".equals(direction)
                    ? matchingLine
                        + ", or upgrade the cluster to "
                        + clientMinor
                    : matchingLine));

  }

  /**
   * Major and minor of a version, or <code>null</code> where the text does not start with them.
   */
  private static int[] majorAndMinor(
      final String version) {

    if (version == null) {
      return null;
    }
    final var matcher = MAJOR_AND_MINOR.matcher(version);
    if (!matcher.matches()) {
      return null;
    }
    try {
      return new int[]{
          Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2))
      };
    } catch (final NumberFormatException e) {
      // digits beyond the range of an int are no version anyone ships
      return null;
    }

  }

}
