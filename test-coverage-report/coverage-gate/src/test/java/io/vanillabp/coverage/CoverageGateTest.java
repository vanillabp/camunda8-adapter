package io.vanillabp.coverage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.integration.test.utils.CoverageGate;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The gate of story 49: it breaks the build when a platform's aggregated coverage
 * drops below the threshold, so a drop is noticed while it happens instead of a year
 * later.
 * <p>
 * JaCoCo's own <code>check</code> goal cannot do this: it judges ONE module's classes
 * against ONE execution-data file, while both numbers of this repository come from
 * aggregated reports spanning many modules. So the gate reads exactly the report which
 * is published, and report and gate can never disagree.
 * <p>
 * The completeness test comes first for the reason story 76 documented: a threshold
 * checked against an incomplete aggregate fails builds for coverage which exists and
 * is only not counted, and nobody can fix that by writing a test.
 */
@ExtendWith(SuppressOutputExtension.class)
public class CoverageGateTest {

  private static final Path ROOT = CoverageGate.repositoryRoot("coverage.repository.root");


  private static final List<Path> AGGREGATE_POMS = List
      .of(
          ROOT.resolve("test-coverage-report/spring-boot/pom.xml"),
          ROOT.resolve("test-coverage-report/quarkus/pom.xml"));

  /**
   * Modules whose execution data belongs to no coverage report. Each entry is a
   * decision - a module missing here and missing from both aggregates is the defect
   * this test exists for.
   */
  private static final Set<String> DELIBERATELY_NOT_AGGREGATED = Set.of();

  @Test
  @DisplayName("Every module producing coverage data is read by an aggregated report")
  public void everyModuleProducingCoverageDataIsAggregated() {

    final var missing = CoverageGate
        .modulesMissingFromAggregates(ROOT, AGGREGATE_POMS, DELIBERATELY_NOT_AGGREGATED);

    assertTrue(missing.isEmpty(), () -> CoverageGate.describeMissingModules(missing, AGGREGATE_POMS));

  }

  @Test
  @DisplayName("The Spring Boot report is above the coverage threshold")
  public void theSpringBootReportIsAboveTheThreshold() {

    assertAboveThreshold("Spring Boot", 0, "coverage.threshold.spring-boot");

  }

  @Test
  @DisplayName("The Quarkus report is above the coverage threshold")
  public void theQuarkusReportIsAboveTheThreshold() {

    assertAboveThreshold("Quarkus", 1, "coverage.threshold.quarkus");

  }

  /**
   * The threshold is read per platform, because a repository may hold a different line
   * on each of them. Where they differ, the lower one is a floor against regression and
   * the reason belongs into the root POM next to the property.
   */
  private void assertAboveThreshold(
      final String platform,
      final int report,
      final String thresholdProperty) {

    final var threshold = Double.parseDouble(System.getProperty(thresholdProperty));

    final var coverage = CoverageGate
        .read(
            CoverageGate
                .reportsOfBothPlatforms(ROOT)
                .get(report),
            platform,
            CoverageGate.Metric.INSTRUCTIONS);

    assertTrue(
        coverage.percentage() >= threshold,
        () -> """
            %s - below the %s %% VanillaBP measures per platform. Coverage is measured separately \
            per platform, so this platform's own tests have to close the gap: sort the per-package \
            numbers of the report's jacoco.csv by MISSED instructions, not by percentage, and put \
            the test where the uncovered code belongs. Code nobody can reach is dead and gets \
            deleted rather than covered."""
            .formatted(coverage, threshold));

  }

}
