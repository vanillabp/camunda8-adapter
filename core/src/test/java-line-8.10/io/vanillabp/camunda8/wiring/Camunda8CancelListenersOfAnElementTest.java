package io.vanillabp.camunda8.wiring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.zeebe.model.bpmn.instance.ServiceTask;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListenerEventType;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What a served listener on an element other than a Camunda-managed user task gets on release
 * line <code>8.10</code>: a cancel execution listener, which this line is the first one to have.
 * <p>
 * The sibling of this test in the directories of the older lines asserts the opposite, which is
 * the whole reason the two live per line.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8CancelListenersOfAnElementTest {

  @Test
  @DisplayName("A served listener of a service task gets a cancel execution listener")
  public void aServiceTaskIsTold() {

    final var model = Camunda8CancelListenersFixture.aServiceTaskWithAStartListener();

    final var withoutACancellation = Camunda8CancelListenersFixture.writeTheCancelListeners(model);

    assertTrue(
        withoutACancellation.isEmpty(),
        () -> "this line can tell any element: "
            + withoutACancellation);
    final var written = List
        .copyOf(
            ((ServiceTask) model.getModelElementById(Camunda8CancelListenersFixture.ELEMENT))
                .getSingleExtensionElement(ZeebeExecutionListeners.class)
                .getExecutionListeners());
    assertEquals(2, written.size(), () -> "the modelled listener and one of VanillaBP's: "
        + written);
    final var cancelListener = written.get(1);
    assertEquals(
        ZeebeExecutionListenerEventType.cancel,
        cancelListener.getEventType(),
        "the listener VanillaBP writes waits for the cancellation");
    assertEquals(
        Camunda8CancelListenersFixture.SCOPED_JOB_TYPE,
        cancelListener.getType(),
        "and carries the job type the CLUSTER knows, so the same worker gets it");
    assertEquals(
        Camunda8Listeners.CANCEL_LISTENER_RETRIES,
        cancelListener.getRetries(),
        "a cancellation is not a place to retry");

  }

}
