package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;
import io.vanillabp.spi.process.ProcessDefinitionNotFoundException;
import io.vanillabp.spi.process.ProcessService;

/**
 * The viewer/history API (story 26) against a real Camunda 8 cluster running
 * <b>without secondary storage</b> (the standalone broker of all Camunda 8
 * integration tests) - the documented degradation:
 * <ul>
 * <li>process definitions and BPMN XML are served from what THIS application
 * version deployed, carrying the cluster's real process definition key and
 * version;</li>
 * <li>the element history stays unavailable and is reported as <code>null</code>
 * (the SPI's "not supported by the underlying BPMS"), NEVER as an error.</li>
 * </ul>
 * The full history requires the query API - see the README.
 * <p>
 * Runs WITHOUT secondary storage, so the query API is unavailable and the adapter's
 * awareness probe answers optimistically - what this test exercises is that fallback.
 * The query path is covered by {@code Camunda8SecondaryStorageIT}, which brings its own
 * Elasticsearch (story 52).
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
// closed when the class is done: every IT here has a context of its own (its own
// container), Spring would keep them all until the JVM exits, and a context outliving
// its cluster keeps its job workers polling an address nobody answers - which is what
// made the later classes of this module run into their timeouts
@DirtiesContext
public class Camunda8ViewerApiIT {

  @Container
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.standaloneBroker();

  @DynamicPropertySource
  static void camunda8Properties(
      final DynamicPropertyRegistry registry) {

    registry.add("vanillabp.adapters.c8.rest-address",
        () -> "http://"
            + CAMUNDA.getHost()
            + ":"
            + CAMUNDA.getMappedPort(8080));
    registry.add("vanillabp.adapters.c8.grpc-address",
        () -> "http://"
            + CAMUNDA.getHost()
            + ":"
            + CAMUNDA.getMappedPort(26500));

  }

  @Autowired
  private ProcessService<DockerAggregate> processService;

  @Autowired
  private DockerAggregateRepository aggregateRepository;

  @Autowired
  private TransactionTemplate transactionTemplate;

  private DockerAggregate startWorkflow() {

    return transactionTemplate.execute(status -> {
      final var aggregate = new DockerAggregate();
      aggregate.setContent("viewer");
      return processService.startWorkflow(aggregateRepository.save(aggregate));
    });

  }

  @Test
  @DisplayName("Definitions and BPMN XML are served from what this application version deployed")
  public void definitionsAndXmlComeFromTheDeployment() throws Exception {

    final var aggregate = startWorkflow();

    final var definitions = processService.getProcessDefinitions(aggregate, null);

    assertEquals(1, definitions.size(), () -> "expected the deployed definition but got: "
        + definitions);
    final var definition = definitions.getFirst();
    assertTrue(
        definition
            .id()
            .startsWith("c8#"),
        () -> "process definition ids are namespaced per adapter id but got: "
            + definition.id());
    assertEquals("TestProcess", definition.bpmnProcessId());
    // the version is the one the CLUSTER assigned at deployment
    assertNotNull(definition.version());
    assertNull(definition.usedByElements());

    try (var xml = processService.getBpmnXml(definition.id())) {
      final var deployedXml = new String(xml.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(
          deployedXml.contains("TestProcess"),
          () -> "the BPMN XML has to contain the process but got: "
              + deployedXml);
      assertTrue(
          deployedXml.contains("test-job"),
          "the XML is the model AS DEPLOYED (VanillaBP's wiring included)");
    }

  }

  @Test
  @DisplayName("Without secondary storage the history reports no elements instead of failing")
  public void historyDegradesWithoutSecondaryStorage() {

    final var aggregate = startWorkflow();

    final var history = processService.getWorkflowHistory(aggregate, null);

    assertNotNull(history.processDefinitionId());
    assertNull(
        history.elementsHistory(),
        "without the query API the element history is reported as 'not supported', never as an error");

  }

  @Test
  @DisplayName("An unknown process definition raises the SPI's guiding exception")
  public void unknownProcessDefinitionRaisesGuidingError() {

    final var exception = assertThrows(
        ProcessDefinitionNotFoundException.class,
        () -> processService.getBpmnXml("c8#123456789"));

    assertTrue(
        exception
            .getMessage()
            .contains("123456789"),
        () -> "expected a guiding message but got: "
            + exception.getMessage());

  }

}
