package io.vanillabp.camunda8.wiring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.Message;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeSubscription;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The start event of an event subprocess waits for a message like an intermediate catch
 * event does. Camunda 8 opens its subscription when the workflow starts, for that one
 * workflow, and it refuses a model in which the message names no correlation key. So the
 * adapter adds the key there as well. The message start event of the process itself starts a
 * new workflow. It correlates by name, and it gets no key.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8MessageOfAnEventSubprocessTest {

  private static final String PROCESS_ID = "order_approval";

  private static final String A_PROCESS_WITH_MESSAGE_EVENT_SUBPROCESSES = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Definitions_1" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:message id="Msg_orderPlaced" name="OrderPlaced" />
        <bpmn:message id="Msg_customerCalled" name="CustomerCalled" />
        <bpmn:message id="Msg_orderWithdrawn" name="OrderWithdrawn" />
        <bpmn:process id="order_approval" isExecutable="true">
          <bpmn:startEvent id="Event_orderPlaced">
            <bpmn:messageEventDefinition id="Def_orderPlaced" messageRef="Msg_orderPlaced" />
          </bpmn:startEvent>
          <bpmn:subProcess id="Activity_note" triggeredByEvent="true">
            <bpmn:startEvent id="Event_customerCalled" isInterrupting="false">
              <bpmn:messageEventDefinition id="Def_customerCalled" messageRef="Msg_customerCalled" />
            </bpmn:startEvent>
          </bpmn:subProcess>
          <bpmn:subProcess id="Activity_withdraw" triggeredByEvent="true">
            <bpmn:startEvent id="Event_orderWithdrawn" isInterrupting="true">
              <bpmn:messageEventDefinition id="Def_orderWithdrawn" messageRef="Msg_orderWithdrawn" />
            </bpmn:startEvent>
          </bpmn:subProcess>
        </bpmn:process>
      </bpmn:definitions>
      """;

  private static BpmnModelInstance theModel() {

    return Bpmn
        .readModelFromStream(
            new ByteArrayInputStream(
                A_PROCESS_WITH_MESSAGE_EVENT_SUBPROCESSES.getBytes(StandardCharsets.UTF_8)));

  }

  private static String correlationKeyOf(
      final BpmnModelInstance model,
      final String messageId) {

    final Message message = model.getModelElementById(messageId);
    final var subscription = message.getSingleExtensionElement(ZeebeSubscription.class);
    return subscription == null
        ? null
        : subscription.getCorrelationKey();

  }

  @Test
  @DisplayName("Both event subprocesses wait for a message, and the start event of the process does not")
  public void theStartEventsOfTheEventSubprocessesWaitForAMessage() {

    final var waiting = Camunda8TaskWiring
        .messageCatchElementsOf(theModel(), PROCESS_ID)
        .map(Camunda8TaskWiring.MessageCatchElement::elementId)
        .toList();

    assertEquals(
        List.of("Event_customerCalled", "Event_orderWithdrawn"),
        waiting,
        () -> "an event subprocess waits inside a running workflow: "
            + waiting);

  }

  @Test
  @DisplayName("The messages of both event subprocesses get the aggregate ID as their correlation key")
  public void theMessagesOfTheEventSubprocessesGetTheCorrelationKey() {

    final var model = theModel();

    Camunda8TaskWiring.wireMessageSubscriptions(model, PROCESS_ID, () -> "orderId");

    assertEquals("=orderId", correlationKeyOf(model, "Msg_customerCalled"),
        "the event subprocess which runs next to the workflow");
    assertEquals("=orderId", correlationKeyOf(model, "Msg_orderWithdrawn"),
        "the event subprocess which takes the workflow over");
    assertNull(correlationKeyOf(model, "Msg_orderPlaced"),
        "a message which starts a new workflow correlates by name only");

  }

  @Test
  @DisplayName("A process nobody serves owes the cluster the keys of its event subprocesses")
  public void theEventSubprocessesAreNamedAsMissingAKey() {

    final var missing = Camunda8TaskWiring.messagesWithoutACorrelationKey(theModel(), PROCESS_ID);

    assertEquals(
        List
            .of(
                new Camunda8TaskWiring.MessageWithoutACorrelationKey(
                    "Event_customerCalled", "CustomerCalled"),
                new Camunda8TaskWiring.MessageWithoutACorrelationKey(
                    "Event_orderWithdrawn", "OrderWithdrawn")),
        missing);

  }

}
