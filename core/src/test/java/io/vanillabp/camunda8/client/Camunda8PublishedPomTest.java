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
 * <p>
 * The same file also says where the artifact comes from and, until September 2026, where we
 * deploy it. An address which opens and a silence about our registry are cheap to check here,
 * and both are the kind of thing which comes back without anybody noticing.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8PublishedPomTest {

  /** The POM which is installed and deployed for this module, written by the flatten plugin. */
  private static final Path PUBLISHED_POM = Path.of(".flattened-pom.xml");

  /** The one address every artifact of this repository names, whichever module it is. */
  private static final String REPOSITORY = "https://github.com/vanillabp/camunda8-adapter";

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

  @Test
  @DisplayName("the published POM names an address which opens")
  public void thePublishedPomNamesAnAddressWhichOpens() throws Exception {

    final var published = read();
    final var wrong = new StringBuilder();
    appendWhereWrong(wrong, "url", childText(published, "url"), REPOSITORY);
    final var scm = child(published, "scm");
    appendWhereWrong(wrong, "scm/connection", childText(scm, "connection"), "scm:git:"
        + REPOSITORY
        + ".git");
    appendWhereWrong(
        wrong,
        "scm/developerConnection",
        childText(scm, "developerConnection"),
        "scm:git:"
            + REPOSITORY
            + ".git");
    appendWhereWrong(wrong, "scm/url", childText(scm, "url"), REPOSITORY
        + "/tree/main");

    if (wrong.isEmpty()) {
      return;
    }
    throw new AssertionError(
        ("The POM published for release line %s names addresses we did not write:%s"
            + "%nMaven appends the artifact's name to the URL and to all three scm elements of a "
            + "child unless the four 'inherit.append.path' attributes in the parent pom.xml say "
            + "otherwise, and an address with a module name in it is no page. Every artifact of "
            + "this repository names the repository root.")
            .formatted(Camunda8ReleaseLine.id(), wrong));

  }

  @Test
  @DisplayName("the published POM says nothing about where we deploy")
  public void thePublishedPomSaysNothingAboutWhereWeDeploy() throws Exception {

    final var published = read();
    if (published.getElementsByTagName("distributionManagement").getLength() == 0) {
      return;
    }
    throw new AssertionError(
        ("The POM published for release line %s carries a distributionManagement. It names the "
            + "registry WE deploy to, which a user of the artifact can neither use nor act on, and "
            + "on an artifact sitting on Maven Central it points a reader at GitHub Packages. The "
            + "source pom.xml keeps it because the deploy reads it from there, and the flatten "
            + "plugin takes it out of what we publish: check <pomElements> in the parent pom.xml.")
            .formatted(Camunda8ReleaseLine.id()));

  }

  /**
   * Notes one address which is not the one we wrote, so a run reports all of them at once
   * rather than the first.
   */
  private static void appendWhereWrong(
      final StringBuilder wrong,
      final String element,
      final String actual,
      final String expected) {

    if (expected.equals(actual)) {
      return;
    }
    wrong
        .append("%n  <%s> is %s, expected %s".formatted(element, actual == null
            ? "absent"
            : "'"
                + actual
                + "'",
            "'"
                + expected
                + "'"));

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

  /**
   * A direct child element of the given one, or {@code null} where it has none of that
   * name. Direct, because a POM repeats names: 'url' stands under the project, under every
   * license and inside 'scm'.
   */
  private static Element child(
      final Element parent,
      final String tagName) {

    if (parent == null) {
      return null;
    }
    final var children = parent.getChildNodes();
    for (var i = 0; i < children.getLength(); i++) {
      final var node = children.item(i);
      if ((node instanceof final Element element) && tagName.equals(element.getTagName())) {
        return element;
      }
    }
    return null;

  }

  /** The text of a direct child element, or {@code null} where there is none. */
  private static String childText(
      final Element parent,
      final String tagName) {

    final var element = child(parent, tagName);
    return element == null
        ? null
        : element
            .getTextContent()
            .trim();

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
