package io.vanillabp.camunda8.wiring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.UserTask;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskListenerEventType;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskListeners;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The cancel listener VanillaBP writes beside a listener somebody modelled, and the listeners
 * which get none.
 * <p>
 * What a Camunda-managed user task gets is the same on every release line, which is why it is
 * asserted here. What every OTHER element gets depends on the line, so it is asserted in the
 * per-line test directories, by {@code Camunda8CancelListenersOfAnElementTest}.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8CancelListenersTest {

  private static final String PROCESS = "TestProcess";

  /**
   * The job type is prefixed in a workflow module which avoids name clashes that way, and the
   * record of a listener carries the plain one. This is the shortest stand-in for that step.
   */
  private static final UnaryOperator<String> PREFIXED = taskDefinition -> "test-module_"
      + taskDefinition;

  private static BpmnModelInstance model(
      final String processContent) {

    final var xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
          <bpmn:process id="%s" isExecutable="true">
        %s
          </bpmn:process>
        </bpmn:definitions>
        """
        .formatted(PROCESS, processContent);
    return Bpmn.readModelFromStream(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

  }

  private static final String A_USER_TASK_WITH_A_MODELLED_LISTENER = """
          <bpmn:userTask id="Activity_Approve">
            <bpmn:extensionElements>
              <zeebe:userTask />
              <zeebe:formDefinition externalReference="approve" />
              <zeebe:taskListeners>
                <zeebe:taskListener eventType="creating" type="test-module_auditTheApproval" />
              </zeebe:taskListeners>
            </bpmn:extensionElements>
          </bpmn:userTask>
      """;

  private static final String A_USER_TASK_WHOSE_LISTENER_IS_THE_CANCELLATION = """
          <bpmn:userTask id="Activity_Approve">
            <bpmn:extensionElements>
              <zeebe:userTask />
              <zeebe:formDefinition externalReference="approve" />
              <zeebe:taskListeners>
                <zeebe:taskListener eventType="canceling" type="test-module_auditTheApproval" />
              </zeebe:taskListeners>
            </bpmn:extensionElements>
          </bpmn:userTask>
      """;

  private static List<Camunda8Listeners.ModelledListener> writeTheCancelListeners(
      final BpmnModelInstance model,
      final Camunda8Listeners.Kind kind,
      final String event) {

    return Camunda8TaskWiring
        .addCancelListenersFor(
            model,
            List.of(
                new Camunda8Listeners.ModelledListener(
                    PROCESS, "Activity_Approve", kind, event, "auditTheApproval")),
            PREFIXED);

  }

  private static List<io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskListener> taskListenersOf(
      final BpmnModelInstance model) {

    return List
        .copyOf(
            ((UserTask) model.getModelElementById("Activity_Approve"))
                .getSingleExtensionElement(ZeebeTaskListeners.class)
                .getTaskListeners());

  }

  @Test
  @DisplayName("A served listener of a user task gets a canceling listener carrying its own job type")
  public void aUserTaskListenerGetsACancellation() {

    final var model = model(A_USER_TASK_WITH_A_MODELLED_LISTENER);

    final var withoutACancellation = writeTheCancelListeners(
        model, Camunda8Listeners.Kind.TASK_LISTENER, "creating");

    assertTrue(
        withoutACancellation.isEmpty(),
        () -> "a user task can be told on every release line: "
            + withoutACancellation);
    final var written = taskListenersOf(model);
    assertEquals(2, written.size(), () -> "the modelled listener and one of VanillaBP's: "
        + written);
    final var cancelListener = written.get(1);
    assertEquals(
        ZeebeTaskListenerEventType.canceling,
        cancelListener.getEventType(),
        "the listener VanillaBP writes waits for the cancellation");
    assertEquals(
        "test-module_auditTheApproval",
        cancelListener.getType(),
        "and carries the job type the CLUSTER knows, so the same worker gets it");
    assertEquals(
        Camunda8Listeners.CANCEL_LISTENER_RETRIES,
        cancelListener.getRetries(),
        "a cancellation is not a place to retry");

  }

  @Test
  @DisplayName("A listener the modeller put on the cancellation gets none of VanillaBP's own")
  public void aModelledCancellationGetsNothing() {

    final var model = model(A_USER_TASK_WHOSE_LISTENER_IS_THE_CANCELLATION);

    final var withoutACancellation = writeTheCancelListeners(
        model, Camunda8Listeners.Kind.TASK_LISTENER, "canceling");

    assertTrue(
        withoutACancellation.isEmpty(),
        () -> "such a listener hears the moment through itself, so nothing is missing: "
            + withoutACancellation);
    assertEquals(
        1,
        taskListenersOf(model).size(),
        "a second listener would report the same moment to the same method twice");

  }

  @Test
  @DisplayName("Wiring a model a second time writes no second cancel listener")
  public void aSecondWiringAddsNothing() {

    final var model = model(A_USER_TASK_WITH_A_MODELLED_LISTENER);

    writeTheCancelListeners(model, Camunda8Listeners.Kind.TASK_LISTENER, "creating");
    writeTheCancelListeners(model, Camunda8Listeners.Kind.TASK_LISTENER, "creating");

    assertEquals(
        2,
        taskListenersOf(model).size(),
        "a model which already carries the cancellation is left as it is");

  }

  @Test
  @DisplayName("A listener of an element the model does not declare is passed over")
  public void anElementWhichIsNotInTheModelIsPassedOver() {

    final var model = model(A_USER_TASK_WITH_A_MODELLED_LISTENER);

    final var withoutACancellation = Camunda8TaskWiring
        .addCancelListenersFor(
            model,
            List.of(
                new Camunda8Listeners.ModelledListener(
                    PROCESS, "Activity_WhichIsGone", Camunda8Listeners.Kind.EXECUTION_LISTENER, "start", "auditIt")),
            PREFIXED);

    assertTrue(
        withoutACancellation.isEmpty(),
        () -> "nothing can be written and nothing is missing either: "
            + withoutACancellation);

  }

  @Test
  @DisplayName("Which event of which kind counts as a cancellation")
  public void whatCountsAsACancellation() {

    assertTrue(
        Camunda8Listeners.isACancellation(
            new Camunda8Listeners.ModelledListener(
                PROCESS, "Activity_Approve", Camunda8Listeners.Kind.TASK_LISTENER, "canceling", "auditIt")),
        "a task listener on 'canceling' is the cancellation of a user task");
    assertTrue(
        Camunda8Listeners.isACancellation(
            new Camunda8Listeners.ModelledListener(
                PROCESS, "Activity_Approve", Camunda8Listeners.Kind.EXECUTION_LISTENER, "cancel", "auditIt")),
        "an execution listener on 'cancel' is the cancellation of any element");
    assertEquals(
        false,
        Camunda8Listeners.isACancellation(
            new Camunda8Listeners.ModelledListener(
                PROCESS, "Activity_Approve", Camunda8Listeners.Kind.TASK_LISTENER, "completing", "auditIt")),
        "a completing listener waits for the task to finish, and hears that it never will");
    assertEquals(
        false,
        Camunda8Listeners.isACancellation(
            new Camunda8Listeners.ModelledListener(
                PROCESS, "Activity_Approve", Camunda8Listeners.Kind.EXECUTION_LISTENER, "end", "auditIt")),
        "an end execution listener of this cluster does not fire on a cancellation");

  }

}
