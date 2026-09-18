package io.vanillabp.camunda8.wiring;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.zeebe.model.bpmn.instance.ServiceTask;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What a served listener on an element other than a Camunda-managed user task gets on release
 * line <code>8.8</code>: nothing, because a cancel execution listener arrived with 8.10.
 * <p>
 * The listener is returned instead, and the startup report names it. The sibling of this test in
 * the 8.10 directory asserts the opposite, which is the whole reason the two live per line.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8CancelListenersOfAnElementTest {

  @Test
  @DisplayName("A served listener of a service task is reported as hearing no cancellation")
  public void aServiceTaskCannotBeTold() {

    final var model = Camunda8CancelListenersFixture.aServiceTaskWithAStartListener();

    final var withoutACancellation = Camunda8CancelListenersFixture.writeTheCancelListeners(model);

    assertEquals(
        1,
        withoutACancellation.size(),
        () -> "this line cannot tell a service task, and says so rather than staying silent: "
            + withoutACancellation);
    assertEquals(
        Camunda8CancelListenersFixture.ELEMENT,
        withoutACancellation.getFirst().elementId(),
        "named by the element a modeller finds");
    final var written = ((ServiceTask) model.getModelElementById(Camunda8CancelListenersFixture.ELEMENT))
        .getSingleExtensionElement(ZeebeExecutionListeners.class)
        .getExecutionListeners();
    // counted rather than searched by job type: the cancel listener would carry the job type of
    // the modelled one, so only the event tells the two apart, and this line has no such event
    assertEquals(
        1,
        written.size(),
        () -> "and nothing of VanillaBP's reached the model: "
            + written);

  }

}
