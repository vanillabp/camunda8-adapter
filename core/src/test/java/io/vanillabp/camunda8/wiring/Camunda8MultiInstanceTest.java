package io.vanillabp.camunda8.wiring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.Activity;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeIoMapping;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What the adapter has to do to a model before Camunda 8 can report an iteration
 * (story 62): every multi-instance element gets input mappings named after itself,
 * and which iterations enclose which element is remembered for dispatch.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8MultiInstanceTest {

  private static final String NESTED = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:process id="MiProcess" isExecutable="true">
          <bpmn:serviceTask id="Flat">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="flat" />
            </bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>
          </bpmn:serviceTask>
          <bpmn:subProcess id="Outer">
            <bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=groups" inputElement="group" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:serviceTask id="Inner">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="inner" />
              </bpmn:extensionElements>
              <bpmn:multiInstanceLoopCharacteristics>
                <bpmn:extensionElements>
                  <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
                </bpmn:extensionElements>
              </bpmn:multiInstanceLoopCharacteristics>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="Plain">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="plain" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:subProcess>
        </bpmn:process>
      </bpmn:definitions>
      """;

  private static BpmnModelInstance model(
      final String xml) {

    return Bpmn.readModelFromStream(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

  }

  private static List<String> inputsOf(
      final BpmnModelInstance model,
      final String elementId) {

    final var activity = (Activity) model.getModelElementById(elementId);
    final var ioMapping = activity.getSingleExtensionElement(ZeebeIoMapping.class);
    if (ioMapping == null) {
      return List.of();
    }
    return ioMapping
        .getInputs()
        .stream()
        .map(input -> input.getTarget()
            + "="
            + input.getSource())
        .toList();

  }

  @Test
  @DisplayName("every multi-instance element gets index, total and element mappings named after itself")
  public void mappingsAreInjected() {

    final var model = model(NESTED);

    Camunda8MultiInstance.wire(model, "MiProcess", new Camunda8MultiInstance.Registry());

    assertEquals(
        List
            .of(
                "vanillabpMiIndex_Flat==loopCounter",
                "vanillabpMiTotal_Flat==count(items)",
                "vanillabpMiElement_Flat==item"),
        inputsOf(model, "Flat"));
    assertEquals(
        List
            .of(
                "vanillabpMiIndex_Outer==loopCounter",
                "vanillabpMiTotal_Outer==count(groups)",
                "vanillabpMiElement_Outer==group"),
        inputsOf(model, "Outer"));
    assertTrue(
        inputsOf(model, "Plain").isEmpty(),
        "a task which is not multi-instance is left alone");

  }

  @Test
  @DisplayName("wiring the same model twice adds nothing - a redeployment must not change the BPMN")
  public void injectionIsIdempotent() {

    final var model = model(NESTED);
    final var registry = new Camunda8MultiInstance.Registry();

    Camunda8MultiInstance.wire(model, "MiProcess", registry);
    final var afterFirst = inputsOf(model, "Flat");
    Camunda8MultiInstance.wire(model, "MiProcess", registry);

    assertEquals(afterFirst, inputsOf(model, "Flat"));

  }

  @Test
  @DisplayName("the chain of a nested task is outermost first, and holds every iteration around it")
  public void theChainIsRecorded() {

    final var model = model(NESTED);
    final var registry = new Camunda8MultiInstance.Registry();

    Camunda8MultiInstance.wire(model, "MiProcess", registry);

    assertEquals(
        List.of("Outer", "Inner"),
        registry
            .chainOf("MiProcess", "Inner")
            .stream()
            .map(Camunda8MultiInstance.MultiInstanceElement::elementId)
            .toList(),
        "the subprocess encloses the task, so it comes first");
    assertEquals(
        List.of("Flat"),
        registry
            .chainOf("MiProcess", "Flat")
            .stream()
            .map(Camunda8MultiInstance.MultiInstanceElement::elementId)
            .toList());
    assertEquals(
        List.of("Outer"),
        registry
            .chainOf("MiProcess", "Plain")
            .stream()
            .map(Camunda8MultiInstance.MultiInstanceElement::elementId)
            .toList(),
        "a plain task inside a multi-instance subprocess still runs in that iteration");
    assertTrue(
        registry.chainOf("MiProcess", "Unknown").isEmpty());

  }

  @Test
  @DisplayName("the index the SPI reports counts from 0, although Camunda 8 counts from 1")
  public void valuesAreTranslated() {

    final var model = model(NESTED);
    final var registry = new Camunda8MultiInstance.Registry();
    Camunda8MultiInstance.wire(model, "MiProcess", registry);

    final var values = Camunda8MultiInstance
        .valuesOf(
            registry.chainOf("MiProcess", "Inner"),
            Map
                .of(
                    "vanillabpMiIndex_Outer", 2,
                    "vanillabpMiTotal_Outer", 2,
                    "vanillabpMiElement_Outer", "g2",
                    "vanillabpMiIndex_Inner", 1,
                    "vanillabpMiTotal_Inner", 3,
                    "vanillabpMiElement_Inner", "x"));

    assertEquals(List.of("Outer", "Inner"), List.copyOf(values.keySet()), "outermost first");
    assertEquals(1, values.get("Outer").index());
    assertEquals(2, values.get("Outer").total());
    assertEquals("g2", values.get("Outer").element());
    assertEquals(0, values.get("Inner").index());
    assertEquals(3, values.get("Inner").total());
    assertEquals("x", values.get("Inner").element());

  }

  @Test
  @DisplayName("a model deployed before this adapter knew about multi-instance reports nothing")
  public void missingVariablesAreSkipped() {

    final var model = model(NESTED);
    final var registry = new Camunda8MultiInstance.Registry();
    Camunda8MultiInstance.wire(model, "MiProcess", registry);

    final var values = Camunda8MultiInstance
        .valuesOf(registry.chainOf("MiProcess", "Flat"), Map.of("items", List.of("a")));

    assertTrue(values.isEmpty(), "the core then names what was supplied, which is nothing");

  }

  @Test
  @DisplayName("a process without multi-instance is not touched at all")
  public void nothingHappensWithoutMultiInstance() {

    final var model = model(
        """
            <?xml version="1.0" encoding="UTF-8"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
              <bpmn:process id="Plain" isExecutable="true">
                <bpmn:serviceTask id="Task">
                  <bpmn:extensionElements>
                    <zeebe:taskDefinition type="task" />
                  </bpmn:extensionElements>
                </bpmn:serviceTask>
              </bpmn:process>
            </bpmn:definitions>
            """);
    final var registry = new Camunda8MultiInstance.Registry();

    Camunda8MultiInstance.wire(model, "Plain", registry);

    assertTrue(inputsOf(model, "Task").isEmpty());
    assertTrue(registry.chainOf("Plain", "Task").isEmpty());
    // a process which is not in the model at all is not an error either
    Camunda8MultiInstance.wire(model, "Absent", registry);

  }

  @Test
  @DisplayName("an element without an input element reports no element, and one without a collection no total")
  public void whatTheModelDoesNotSayIsNotInvented() {

    final var model = model(
        """
            <?xml version="1.0" encoding="UTF-8"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
              <bpmn:process id="Sparse" isExecutable="true">
                <bpmn:serviceTask id="Task">
                  <bpmn:extensionElements>
                    <zeebe:taskDefinition type="task" />
                  </bpmn:extensionElements>
                  <bpmn:multiInstanceLoopCharacteristics>
                    <bpmn:extensionElements>
                      <zeebe:loopCharacteristics inputCollection="=items" />
                    </bpmn:extensionElements>
                  </bpmn:multiInstanceLoopCharacteristics>
                </bpmn:serviceTask>
              </bpmn:process>
            </bpmn:definitions>
            """);
    final var registry = new Camunda8MultiInstance.Registry();

    Camunda8MultiInstance.wire(model, "Sparse", registry);

    assertEquals(
        List.of("vanillabpMiIndex_Task==loopCounter", "vanillabpMiTotal_Task==count(items)"),
        inputsOf(model, "Task"));
    assertNull(
        registry
            .chainOf("Sparse", "Task")
            .getFirst()
            .elementVariable());

    final var values = Camunda8MultiInstance
        .valuesOf(
            registry.chainOf("Sparse", "Task"),
            Map.of("vanillabpMiIndex_Task", 1, "vanillabpMiTotal_Task", 4));
    assertNull(values.get("Task").element());
    assertEquals(0, values.get("Task").index());

  }

  @Test
  @DisplayName("two elements whose IDs differ only in characters a variable name cannot hold are rejected")
  public void ambiguousElementIdsFailTheDeployment() {

    final var model = model(
        """
            <?xml version="1.0" encoding="UTF-8"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
              <bpmn:process id="Clash" isExecutable="true">
                <bpmn:serviceTask id="my-task">
                  <bpmn:extensionElements>
                    <zeebe:taskDefinition type="a" />
                  </bpmn:extensionElements>
                  <bpmn:multiInstanceLoopCharacteristics>
                    <bpmn:extensionElements>
                      <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
                    </bpmn:extensionElements>
                  </bpmn:multiInstanceLoopCharacteristics>
                </bpmn:serviceTask>
                <bpmn:serviceTask id="my.task">
                  <bpmn:extensionElements>
                    <zeebe:taskDefinition type="b" />
                  </bpmn:extensionElements>
                  <bpmn:multiInstanceLoopCharacteristics>
                    <bpmn:extensionElements>
                      <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
                    </bpmn:extensionElements>
                  </bpmn:multiInstanceLoopCharacteristics>
                </bpmn:serviceTask>
              </bpmn:process>
            </bpmn:definitions>
            """);

    final var exception = assertThrows(
        IllegalStateException.class,
        () -> Camunda8MultiInstance.wire(model, "Clash", new Camunda8MultiInstance.Registry()));

    assertTrue(exception.getMessage().contains("my-task"), exception.getMessage());
    assertTrue(exception.getMessage().contains("my.task"), exception.getMessage());
    assertTrue(exception.getMessage().contains("Rename one of them"), exception.getMessage());

  }

}
