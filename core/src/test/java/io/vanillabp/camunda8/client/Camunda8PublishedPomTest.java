package io.vanillabp.camunda8.client;

import java.nio.file.Files;
import java.nio.file.Path;

import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Element;

import io.vanillabp.camunda8.Camunda8ReleaseLine;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The POM this line publishes asks for this line's Camunda client, and for nothing an
 * application of another line would have to take.
 * <p>
 * A release line is a promise about the cluster an application may run: the client a build
 * was compiled against is the lowest cluster version that build accepts. The promise is
 * kept by the POM rather than by the jar, because that POM is what puts the client on an
 * application's classpath. It used to say something else. The version lived in a property
 * the line profile sets, the published POM was a copy of the source POM, and a consumer
 * activates none of our profiles, so every line asked for the client of the current GA
 * line. That is what the flatten plugin's 'oss' mode ended, and this test is what keeps it
 * ended.
 * <p>
 * The published POM is read from disk rather than derived, because deriving it would repeat
 * the reasoning the mistake was made in. It is the file the flatten plugin writes during
 * 'process-resources' and Maven installs and deploys in place of the source POM.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8PublishedPomTest {

  /** The POM which is installed and deployed for this module, written by the flatten plugin. */
  private static final Path PUBLISHED_POM = Path.of(".flattened-pom.xml");

  @Test
  @DisplayName("the published POM asks for the client of this release line")
  public void thePublishedPomAsksForTheClientOfThisLine() throws Exception {

    final var published = read();
    final var client = dependencyVersion(published, "io.camunda", "camunda-client-java");

    if (Camunda8ReleaseLine.clientVersion().equals(client)) {
      return;
    }
    throw new AssertionError(
        ("The POM published for release line %s asks for Camunda client %s, but this build was "
            + "compiled against %s. An application takes its client from that POM, so it would run "
            + "code of one client against another one. A version reaches the published POM only "
            + "when the flatten plugin resolves it: check that <flattenMode> in the parent pom.xml "
            + "is still 'oss'.")
            .formatted(
                Camunda8ReleaseLine.id(),
                client == null
                    ? "no version at all"
                    : client,
                Camunda8ReleaseLine.clientVersion()));

  }

  @Test
  @DisplayName("the published POM leaves an application nothing of ours to inherit")
  public void thePublishedPomHasNoParentAndNoDependencyManagement() throws Exception {

    final var published = read();
    if ((published.getElementsByTagName("parent").getLength() == 0) && (published
        .getElementsByTagName("dependencyManagement").getLength() == 0)) {
      return;
    }
    throw new AssertionError(
        "The POM published for release line %s carries a parent or a dependencyManagement. "
            .formatted(Camunda8ReleaseLine.id())
            + "Both are read when an application resolves its dependencies, and both would hand it "
            + "versions this repository picked for the newest line, the protobuf pin of the parent "
            + "pom.xml above all. What one line needs is no business of another line's users, see "
            + "decision 39 in the repository's DECISIONS.md.");

  }

  private Element read() throws Exception {

    if (!Files.isRegularFile(PUBLISHED_POM)) {
      throw new AssertionError(
          "The published POM is missing at '%s'. The flatten plugin writes it in the phase "
              .formatted(PUBLISHED_POM.toAbsolutePath())
              + "'process-resources', so this test cannot run from an IDE which skipped it.");
    }
    final var factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(false);
    return factory
        .newDocumentBuilder()
        .parse(PUBLISHED_POM.toFile())
        .getDocumentElement();

  }

  /**
   * The version the published POM declares for one dependency, or {@code null} where it
   * declares the dependency without one.
   */
  private String dependencyVersion(
      final Element project,
      final String groupId,
      final String artifactId) {

    final var dependencies = project.getElementsByTagName("dependency");
    for (var i = 0; i < dependencies.getLength(); i++) {
      final var dependency = (Element) dependencies.item(i);
      if (groupId.equals(textOf(dependency, "groupId")) && artifactId
          .equals(textOf(dependency, "artifactId"))) {
        return textOf(dependency, "version");
      }
    }
    throw new AssertionError(
        ("The POM published for release line %s declares no dependency on %s:%s. Either this "
            + "module stopped using the Camunda client, and then the release lines are about "
            + "something else than they were, or this test reads the wrong file.")
            .formatted(Camunda8ReleaseLine.id(), groupId, artifactId));

  }

  private static String textOf(
      final Element dependency,
      final String tagName) {

    final var elements = dependency.getElementsByTagName(tagName);
    return elements.getLength() == 0
        ? null
        : elements
            .item(0)
            .getTextContent()
            .trim();

  }

}
