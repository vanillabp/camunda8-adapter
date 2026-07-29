package io.vanillabp.camunda8.deployment;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.integration.adapter.spi.BpmnParseException;

/**
 * Unit tests of {@link Camunda8DeploymentService} not requiring a cluster: BPMN parsing
 * (executable-process extraction, parse errors) and that deployment of an empty module
 * does not touch the client.
 */
public class Camunda8DeploymentServiceTest {

  private static final String TWO_PROCESSES_ONE_EXECUTABLE = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          id="Definitions_1" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:process id="ExecutableProcess" isExecutable="true">
          <bpmn:startEvent id="start"/>
        </bpmn:process>
        <bpmn:process id="NonExecutableProcess" isExecutable="false">
          <bpmn:startEvent id="start2"/>
        </bpmn:process>
      </bpmn:definitions>
      """;

  private Camunda8DeploymentService newDeploymentService() {

    // an unconfigured factory: getClient() would throw if ever called
    return new Camunda8DeploymentService(
        "c8", new Camunda8ClientFactory("c8", new Camunda8AdapterConfiguration()));

  }

  private static InputStream bpmn(
      final String bpmnProcessId) {

    final var model = Bpmn
        .createExecutableProcess(bpmnProcessId)
        .startEvent()
        .endEvent()
        .done();
    return new ByteArrayInputStream(Bpmn.convertToString(model).getBytes(UTF_8));

  }

  @Test
  @DisplayName("readBpmn returns one entry per executable process")
  public void readBpmnReturnsExecutableProcess() {

    final var service = newDeploymentService();

    final var result = service.readBpmn("module", "test.bpmn", bpmn("MyProcess"), true);

    assertEquals(1, result.size());
    assertEquals("MyProcess", result.get(0).getKey());

  }

  @Test
  @DisplayName("readBpmn ignores non-executable processes of a multi-process file")
  public void readBpmnIgnoresNonExecutableProcesses() {

    final var service = newDeploymentService();

    final var result = service.readBpmn(
        "module",
        "two.bpmn",
        new ByteArrayInputStream(TWO_PROCESSES_ONE_EXECUTABLE.getBytes(UTF_8)),
        true);

    assertEquals(1, result.size());
    assertEquals("ExecutableProcess", result.get(0).getKey());

  }

  @Test
  @DisplayName("readBpmn wraps parse errors in BpmnParseException")
  public void readBpmnWrapsParseErrors() {

    final var service = newDeploymentService();

    final var exception = assertThrows(
        BpmnParseException.class,
        () -> service.readBpmn(
            "module",
            "broken.bpmn",
            new ByteArrayInputStream("this is not valid bpmn xml".getBytes(UTF_8)),
            true));
    assertTrue(exception.getMessage().contains("broken.bpmn"));

  }

  @Test
  @DisplayName("prepareBpmn accumulates resources and process IDs into the context")
  public void prepareBpmnAccumulates() {

    final var service = newDeploymentService();
    final var model = Bpmn
        .createExecutableProcess("P1")
        .startEvent()
        .endEvent()
        .done();

    final Camunda8ProcessingContext context = service.prepareBpmn("module", null, "a.bpmn", "P1", model);
    // a second executable process of the same file reuses the context and deduplicates
    // the resource by filename
    service.prepareBpmn("module", context, "a.bpmn", "P2", model);

    assertEquals(1, context.getResources().size());

  }

  @Test
  @DisplayName("deploying an empty module does not touch the (unconfigured) client")
  public void deployEmptyModuleDoesNotTouchClient() {

    final var service = newDeploymentService();

    // null context (no BPMN files at all) and empty context must not build a client
    assertDoesNotThrow(() -> service.deployResources("module", null));
    assertDoesNotThrow(() -> service.deployResources("module", new Camunda8ProcessingContext("module")));

  }

}
