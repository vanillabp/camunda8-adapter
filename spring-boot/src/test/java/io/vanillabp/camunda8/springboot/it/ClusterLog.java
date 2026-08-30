package io.vanillabp.camunda8.springboot.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.testcontainers.containers.output.OutputFrame;

/**
 * What the Camunda 8 containers of this module printed, written into a file below
 * {@code target}.
 * <p>
 * Every integration test class here brings a cluster of its own, and a red build
 * otherwise leaves nothing of any of them: the container is removed with the run and the
 * runner's disk with it, so a test which timed out waiting for a notification and a
 * cluster which never handed the job out look the same afterwards. The file is what the
 * build uploads next to the test reports when it fails, and its name carries
 * {@code application} because that is the shape of file the upload collects.
 * <p>
 * All containers of the module append to the SAME file, and every line names the
 * container which wrote it and the time it did. That pair is what relates a line to the
 * test which was running - the containers are created while their classes are loaded, so
 * there is no test name to ask for at that point.
 * <p>
 * The Quarkus integration tests carry a writer of their own. The two modules share no
 * test classpath and this repository publishes no test jar, so a class used by both would
 * have to be a module of its own for forty lines.
 */
final class ClusterLog {

  /**
   * Where the containers of this module write. Relative to the module, so a run from the
   * IDE and a run from Maven produce the same file.
   */
  static final Path FILE = Path
      .of("target", "c8-cluster-application.log")
      .toAbsolutePath();

  /**
   * Numbers the containers apart. Several classes run one after the other in the same
   * fork, and every one of them starts a broker with the same image and the same purpose.
   */
  private static final AtomicInteger CONTAINERS = new AtomicInteger();

  private static Writer writer;

  private ClusterLog() {
    // static helper
  }

  /**
   * @param kind What the container is, e.g. {@code broker}
   * @return A log consumer for one container, to be handed to
   *         {@code withLogConsumer(...)}
   */
  static Consumer<OutputFrame> of(
      final String kind) {

    final var container = "%s-%d".formatted(kind, CONTAINERS.incrementAndGet());
    return frame -> append(container, frame);

  }

  private static synchronized void append(
      final String container,
      final OutputFrame frame) {

    final var line = frame.getUtf8StringWithoutLineEnding();
    if (line.isEmpty()) {
      return;
    }
    try {
      if (writer == null) {
        Files.createDirectories(FILE.getParent());
        // truncating: the file belongs to this run, and a build reading it wants the
        // lines of the run which just failed rather than of every run since the last
        // 'clean'
        writer = Files
            .newBufferedWriter(
                FILE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
      }
      // the runner's own clock and only milliseconds of it: what a reader relates a line
      // to is the build log and the test report, and both are written by this clock
      writer.write("%s %s %s%n".formatted(LocalTime.now().truncatedTo(ChronoUnit.MILLIS), container, line));
      // flushed per line: nothing closes this writer, and the interesting lines are the
      // last ones before a run ends
      writer.flush();
    } catch (final IOException e) {
      throw new UncheckedIOException("Cannot write '%s'".formatted(FILE), e);
    }

  }

}
