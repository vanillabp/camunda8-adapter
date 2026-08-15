package io.vanillabp.camunda8.wiring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Story 59: which elements of a model can put a SECOND token into a running workflow -
 * the finding this adapter reports to the core, which turns it into the hint about two
 * writers on one workflow aggregate.
 * <p>
 * Every construct is asserted together with the variant that does NOT produce a second
 * token, since a hint given for a sequential model would be worse than none.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8ConcurrentTokensTest {

  private static BpmnModelInstance model(
      final String processContent) {

    final var xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
          <bpmn:process id="TestProcess" isExecutable="true">
        %s
          </bpmn:process>
          <bpmn:process id="OtherProcess" isExecutable="true">
            <bpmn:parallelGateway id="OtherFork">
              <bpmn:outgoing>OtherFlow_1</bpmn:outgoing>
              <bpmn:outgoing>OtherFlow_2</bpmn:outgoing>
            </bpmn:parallelGateway>
            <bpmn:task id="OtherTask_1" />
            <bpmn:task id="OtherTask_2" />
            <bpmn:sequenceFlow id="OtherFlow_1" sourceRef="OtherFork" targetRef="OtherTask_1" />
            <bpmn:sequenceFlow id="OtherFlow_2" sourceRef="OtherFork" targetRef="OtherTask_2" />
          </bpmn:process>
        </bpmn:definitions>
        """
        .formatted(processContent);
    return Bpmn.readModelFromStream(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

  }

  private static List<String> elementsOf(
      final String processContent) {

    return Camunda8TaskWiring.concurrentTokenElementIdsOf(model(processContent), "TestProcess");

  }

  @Test
  @DisplayName("A boundary event which does not cancel its activity produces a second token")
  public void nonInterruptingBoundaryEvent() {

    final var found = elementsOf("""
            <bpmn:serviceTask id="Activity_Approve" />
            <bpmn:boundaryEvent id="Event_Reminder" cancelActivity="false" attachedToRef="Activity_Approve">
              <bpmn:timerEventDefinition id="Timer_1" />
            </bpmn:boundaryEvent>
            <bpmn:boundaryEvent id="Event_Timeout" attachedToRef="Activity_Approve">
              <bpmn:timerEventDefinition id="Timer_2" />
            </bpmn:boundaryEvent>
        """);

    assertEquals(List.of("Event_Reminder"), found);

  }

  @Test
  @DisplayName("A parallel or inclusive gateway counts when it forks, not when it joins")
  public void forkingGateways() {

    final var found = elementsOf("""
            <bpmn:parallelGateway id="Gateway_Fork">
              <bpmn:outgoing>Flow_1</bpmn:outgoing>
              <bpmn:outgoing>Flow_2</bpmn:outgoing>
            </bpmn:parallelGateway>
            <bpmn:inclusiveGateway id="Gateway_Inclusive">
              <bpmn:outgoing>Flow_3</bpmn:outgoing>
              <bpmn:outgoing>Flow_4</bpmn:outgoing>
            </bpmn:inclusiveGateway>
            <bpmn:parallelGateway id="Gateway_Join">
              <bpmn:incoming>Flow_1</bpmn:incoming>
              <bpmn:incoming>Flow_2</bpmn:incoming>
              <bpmn:outgoing>Flow_5</bpmn:outgoing>
            </bpmn:parallelGateway>
            <bpmn:task id="Task_1" />
            <bpmn:task id="Task_2" />
            <bpmn:task id="Task_3" />
            <bpmn:sequenceFlow id="Flow_1" sourceRef="Gateway_Fork" targetRef="Gateway_Join" />
            <bpmn:sequenceFlow id="Flow_2" sourceRef="Gateway_Fork" targetRef="Gateway_Join" />
            <bpmn:sequenceFlow id="Flow_3" sourceRef="Gateway_Inclusive" targetRef="Task_1" />
            <bpmn:sequenceFlow id="Flow_4" sourceRef="Gateway_Inclusive" targetRef="Task_2" />
            <bpmn:sequenceFlow id="Flow_5" sourceRef="Gateway_Join" targetRef="Task_3" />
        """);

    assertEquals(List.of("Gateway_Fork", "Gateway_Inclusive"), found);

  }

  @Test
  @DisplayName("A multi-instance activity counts when it is parallel, not when it is sequential")
  public void parallelMultiInstance() {

    final var found = elementsOf("""
            <bpmn:serviceTask id="Activity_Parallel">
              <bpmn:multiInstanceLoopCharacteristics isSequential="false" />
            </bpmn:serviceTask>
            <bpmn:serviceTask id="Activity_Sequential">
              <bpmn:multiInstanceLoopCharacteristics isSequential="true" />
            </bpmn:serviceTask>
        """);

    assertEquals(List.of("Activity_Parallel"), found);

  }

  @Test
  @DisplayName("An event subprocess counts when its start event does not interrupt the process")
  public void nonInterruptingEventSubProcess() {

    final var found = elementsOf("""
            <bpmn:subProcess id="SubProcess_Reminder" triggeredByEvent="true">
              <bpmn:startEvent id="Start_Reminder" isInterrupting="false">
                <bpmn:messageEventDefinition id="Message_1" />
              </bpmn:startEvent>
            </bpmn:subProcess>
            <bpmn:subProcess id="SubProcess_Cancel" triggeredByEvent="true">
              <bpmn:startEvent id="Start_Cancel">
                <bpmn:messageEventDefinition id="Message_2" />
              </bpmn:startEvent>
            </bpmn:subProcess>
            <bpmn:subProcess id="SubProcess_Embedded">
              <bpmn:startEvent id="Start_Embedded" />
            </bpmn:subProcess>
        """);

    assertEquals(List.of("SubProcess_Reminder"), found);

  }

  @Test
  @DisplayName("A sequential model reports nothing, and another process' elements never leak in")
  public void aSequentialProcessReportsNothing() {

    final var found = elementsOf("""
            <bpmn:startEvent id="Start" />
            <bpmn:exclusiveGateway id="Gateway_Decision">
              <bpmn:outgoing>Flow_1</bpmn:outgoing>
              <bpmn:outgoing>Flow_2</bpmn:outgoing>
            </bpmn:exclusiveGateway>
            <bpmn:task id="Task_1" />
            <bpmn:task id="Task_2" />
            <bpmn:sequenceFlow id="Flow_1" sourceRef="Gateway_Decision" targetRef="Task_1" />
            <bpmn:sequenceFlow id="Flow_2" sourceRef="Gateway_Decision" targetRef="Task_2" />
        """);

    // the forking parallel gateway of 'OtherProcess' belongs to the other process
    assertTrue(found.isEmpty(), found.toString());

  }

}
