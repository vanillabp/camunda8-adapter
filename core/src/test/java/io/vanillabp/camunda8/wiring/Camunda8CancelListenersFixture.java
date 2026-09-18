package io.vanillabp.camunda8.wiring;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;

/**
 * The one model the per-line tests of the cancel listener work on, so the lines differ in what
 * they assert and in nothing else.
 */
final class Camunda8CancelListenersFixture {

  private Camunda8CancelListenersFixture() {
  }

  static final String PROCESS = "TestProcess";

  /**
   * The element the served listener sits on.
   */
  static final String ELEMENT = "Activity_Ship";

  /**
   * The job type as the CLUSTER knows it, which is the prefixed one in a workflow module which
   * avoids name clashes that way.
   */
  static final String SCOPED_JOB_TYPE = "test-module_auditTheShipping";

  /**
   * @return A service task carrying one execution listener somebody modelled on {@code start}
   */
  static BpmnModelInstance aServiceTaskWithAStartListener() {

    final var xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
          <bpmn:process id="%s" isExecutable="true">
            <bpmn:serviceTask id="%s">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="test-module_shipTheOrder" />
                <zeebe:executionListeners>
                  <zeebe:executionListener eventType="start" type="%s" />
                </zeebe:executionListeners>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:process>
        </bpmn:definitions>
        """
        .formatted(PROCESS, ELEMENT, SCOPED_JOB_TYPE);
    return Bpmn.readModelFromStream(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

  }

  /**
   * Writes what this release line can write beside the served listener of that model.
   *
   * @param model The model, modified in place
   * @return The listeners which hear no cancellation on this line
   */
  static List<Camunda8Listeners.ModelledListener> writeTheCancelListeners(
      final BpmnModelInstance model) {

    return Camunda8TaskWiring
        .addCancelListenersFor(
            model,
            List.of(
                new Camunda8Listeners.ModelledListener(
                    PROCESS, ELEMENT, Camunda8Listeners.Kind.EXECUTION_LISTENER, "start", "auditTheShipping")),
            taskDefinition -> "test-module_"
                + taskDefinition);

  }

}
