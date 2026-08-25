package io.vanillabp.camunda8.wiring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.instance.UserTask;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskListener;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskListenerEventType;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskListeners;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The V1-COMPATIBILITY contract of the user-task listener injection:
 * listener job types keep the V1 prefix, retries stay "0" and the insertion order
 * is EXACTLY V1's (VanillaBP "creating" FIRST, custom listeners in between,
 * VanillaBP "canceling" LAST) - upgrading a V1 application must produce a
 * byte-identical BPMN so no new process version is deployed.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8UserTaskWiringTest {

  private static io.camunda.zeebe.model.bpmn.BpmnModelInstance model(
      final String userTaskExtensions) {

    final var xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
          <bpmn:process id="UTProcess" isExecutable="true">
            <bpmn:userTask id="ut">
              <bpmn:extensionElements>
        %s
              </bpmn:extensionElements>
            </bpmn:userTask>
          </bpmn:process>
        </bpmn:definitions>
        """
        .formatted(userTaskExtensions);
    return Bpmn.readModelFromStream(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

  }

  private static List<ZeebeTaskListener> listenersOf(
      final io.camunda.zeebe.model.bpmn.BpmnModelInstance model) {

    return List.copyOf(model
        .getModelElementsByType(UserTask.class)
        .iterator()
        .next()
        .getSingleExtensionElement(ZeebeTaskListeners.class)
        .getTaskListeners());

  }

  @Test
  @DisplayName("V1 order: VanillaBP creating FIRST, custom listeners in between, VanillaBP canceling LAST")
  public void listenersInsertedInV1Order() {

    final var bpmn = model("""
        <zeebe:userTask />
        <zeebe:formDefinition externalReference="approve" />
        <zeebe:taskListeners>
          <zeebe:taskListener eventType="creating" type="custom-listener" />
        </zeebe:taskListeners>""");

    final var userTasks = Camunda8TaskWiring.userTasksOf(bpmn, "UTProcess", "mod", "x.bpmn");

    assertEquals(1, userTasks.size());
    assertEquals("approve", userTasks.get(0).externalFormReference());
    assertEquals("io.vanillabp.userTask:approve", userTasks.get(0).listenerJobType());

    final var listeners = listenersOf(bpmn);
    assertEquals(3, listeners.size());
    // VanillaBP creating FIRST
    assertEquals(ZeebeTaskListenerEventType.creating, listeners.get(0).getEventType());
    assertEquals("io.vanillabp.userTask:approve", listeners.get(0).getType());
    assertEquals("0", listeners.get(0).getRetries());
    // custom listener stays in between
    assertEquals("custom-listener", listeners.get(1).getType());
    // VanillaBP canceling LAST
    assertEquals(ZeebeTaskListenerEventType.canceling, listeners.get(2).getEventType());
    assertEquals("io.vanillabp.userTask:approve", listeners.get(2).getType());
    assertEquals("0", listeners.get(2).getRetries());

  }

  @Test
  @DisplayName("Re-wiring an already-processed model does not duplicate the listeners")
  public void rewiringDoesNotDuplicate() {

    final var bpmn = model("""
        <zeebe:userTask />
        <zeebe:formDefinition externalReference="approve" />""");

    Camunda8TaskWiring.userTasksOf(bpmn, "UTProcess", "mod", "x.bpmn");
    Camunda8TaskWiring.userTasksOf(bpmn, "UTProcess", "mod", "x.bpmn");

    assertEquals(2, listenersOf(bpmn).size(), "creating + canceling exactly once");

  }

  @Test
  @DisplayName("A user task without an external form reference fails with a guiding message")
  public void missingFormReferenceFailsGuiding() {

    final var bpmn = model("""
        <zeebe:userTask />""");

    final var failure = assertThrows(
        IllegalStateException.class,
        () -> Camunda8TaskWiring.userTasksOf(bpmn, "UTProcess", "mod", "x.bpmn"));
    assertTrue(failure.getMessage().contains("External form reference"));

  }

  @Test
  @DisplayName("Worker-based user tasks (no zeebe:userTask marker) are ignored here")
  public void workerBasedUserTasksIgnored() {

    final var bpmn = model("""
        <zeebe:taskDefinition type="legacyUserTaskJob" />""");

    assertEquals(0, Camunda8TaskWiring.userTasksOf(bpmn, "UTProcess", "mod", "x.bpmn").size());

  }

  @Test
  @DisplayName("A user task as VanillaBP 1 modelled it up to 1.6.3 is found, so it can be reported")
  public void legacyUserTaskIsFound() {

    final var bpmn = model("""
        <zeebe:formDefinition formKey="camunda-forms:bpmn:approve" />""");

    assertEquals(
        List.of("ut"),
        Camunda8TaskWiring.legacyUserTaskIdsOf(bpmn, "UTProcess"),
        "a plain user task whose form definition names a formKey is version 1's construction");
    assertEquals(
        0,
        Camunda8TaskWiring.userTasksOf(bpmn, "UTProcess", "mod", "x.bpmn").size(),
        "and nothing else sees it, which is why it has to be reported on its own");

  }

  @Test
  @DisplayName("A Camunda-managed user task is not version 1's construction, whatever else it carries")
  public void camundaManagedUserTaskIsNotLegacy() {

    final var bpmn = model("""
        <zeebe:userTask />
        <zeebe:formDefinition externalReference="approve" formKey="camunda-forms:bpmn:approve" />""");

    assertEquals(
        List.of(),
        Camunda8TaskWiring.legacyUserTaskIdsOf(bpmn, "UTProcess"),
        "the zeebe:userTask marker decides, and a leftover formKey next to it changes nothing");

  }

  @Test
  @DisplayName("A user task served by an own job worker is not version 1's construction either")
  public void ownJobWorkerUserTaskIsNotLegacy() {

    final var bpmn = model("""
        <zeebe:taskDefinition type="ownWorker" />
        <zeebe:formDefinition formKey="camunda-forms:bpmn:approve" />""");

    assertEquals(
        List.of(),
        Camunda8TaskWiring.legacyUserTaskIdsOf(bpmn, "UTProcess"),
        "a task definition of its own says the application serves this itself");

  }

  @Test
  @DisplayName("A user task without any form definition is nobody's business here")
  public void userTaskWithoutFormDefinitionIsNotLegacy() {

    final var bpmn = model("""
        <zeebe:formDefinition formId="approve" />""");

    assertEquals(List.of(), Camunda8TaskWiring.legacyUserTaskIdsOf(bpmn, "UTProcess"));

  }

  @Test
  @DisplayName("A user task of ANOTHER process is not counted")
  public void userTaskOfAnotherProcessIsNotCounted() {

    final var bpmn = model("""
        <zeebe:formDefinition formKey="camunda-forms:bpmn:approve" />""");

    assertEquals(List.of(), Camunda8TaskWiring.legacyUserTaskIdsOf(bpmn, "SomeOtherProcess"));

  }

}
