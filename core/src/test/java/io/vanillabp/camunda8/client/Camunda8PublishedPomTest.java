package io.vanillabp.camunda8.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.camunda8.Camunda8ReleaseLine;
import io.vanillabp.camunda8.test.PublishedPom;
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
 * ended. See decision 39 in the repository's DECISIONS.md.
 * <p>
 * The same POM also says where the artifact comes from, and it used to say where we deploy
 * it. Decision 40 is about that half.
 * <p>
 * What the assertions know sits in {@link PublishedPom} of the module 'test-support',
 * because the Business Cockpit's Camunda 8 adapter is built the same way and checks the same
 * promise. This test is the caller which names the artifact and the versions expected of it.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8PublishedPomTest {

  /** The one address every artifact of this repository names, whichever module it is. */
  private static final String REPOSITORY = "https://github.com/vanillabp/camunda8-adapter";

  @Test
  @DisplayName("the published POM asks for the client of this release line")
  public void thePublishedPomAsksForTheClientOfThisLine() {

    PublishedPom
        .ofTheModuleUnderTest()
        .asksFor("io.camunda", "camunda-client-java", Camunda8ReleaseLine.clientVersion());

  }

  @Test
  @DisplayName("the published POM leaves an application nothing of ours to inherit")
  public void thePublishedPomHasNoParentAndNoDependencyManagement() {

    PublishedPom
        .ofTheModuleUnderTest()
        .handsAnApplicationNothingToInherit();

  }

  @Test
  @DisplayName("the published POM names an address which opens")
  public void thePublishedPomNamesAnAddressWhichOpens() {

    PublishedPom
        .ofTheModuleUnderTest()
        .pointsAt(REPOSITORY);

  }

  @Test
  @DisplayName("the published POM says nothing about where we deploy")
  public void thePublishedPomSaysNothingAboutWhereWeDeploy() {

    PublishedPom
        .ofTheModuleUnderTest()
        .saysNothingAboutWhereWeDeploy();

  }

}
