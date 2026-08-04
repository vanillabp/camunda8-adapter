package io.vanillabp.camunda8.wiring;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import org.camunda.bpm.model.xml.instance.ModelElementInstance;

import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.BusinessRuleTask;
import io.camunda.zeebe.model.bpmn.instance.FlowElement;
import io.camunda.zeebe.model.bpmn.instance.Process;
import io.camunda.zeebe.model.bpmn.instance.ScriptTask;
import io.camunda.zeebe.model.bpmn.instance.SendTask;
import io.camunda.zeebe.model.bpmn.instance.ServiceTask;
import io.camunda.zeebe.model.bpmn.instance.Task;
import io.camunda.zeebe.model.bpmn.instance.UserTask;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeFormDefinition;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskDefinition;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskListener;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskListenerEventType;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskListeners;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeUserTask;
import io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec;

/**
 * Extracts the job-worker tasks of an executable BPMN process from the Camunda 8
 * model: service-like tasks carrying a <code>zeebe:taskDefinition</code> - its
 * <code>type</code> IS the VanillaBP task definition (job type). Used during
 * <code>wireBpmn</code> for the wiring validation and to know which job workers to
 * open per workflow module.
 */
public final class Camunda8TaskWiring {

  private Camunda8TaskWiring() {
  }

  /**
   * One task to be served by a job worker.
   *
   * @param bpmnProcessId The BPMN process ID
   * @param activityId The BPMN activity ID
   * @param taskDefinition The <code>zeebe:taskDefinition</code> type (= job type)
   */
  public record Camunda8TaskToWire(
                                   String bpmnProcessId,
                                   String activityId,
                                   String taskDefinition) {

    public BpmnTaskSpec toSpec() {

      return new BpmnTaskSpec(activityId, taskDefinition);

    }

  }

  /**
   * The job-worker tasks of the given executable process (including tasks inside
   * embedded subprocesses). Tasks without a <code>zeebe:taskDefinition</code> get a
   * <code>null</code> task definition - reported by the wiring validation with a
   * guiding message.
   */
  public static List<Camunda8TaskToWire> tasksOf(
      final BpmnModelInstance model,
      final String bpmnProcessId) {

    final var tasks = new LinkedList<Camunda8TaskToWire>();
    Stream
        .of(ServiceTask.class, SendTask.class, BusinessRuleTask.class, ScriptTask.class)
        .flatMap(type -> model.getModelElementsByType(type).stream())
        .map(Task.class::cast)
        .filter(task -> bpmnProcessId.equals(owningProcessId(task)))
        .forEach(task -> {
          final var taskDefinition = task.getSingleExtensionElement(ZeebeTaskDefinition.class);
          tasks.add(new Camunda8TaskToWire(
              bpmnProcessId, task.getId(), taskDefinition != null
                  ? taskDefinition.getType()
                  : null));
        });
    return tasks;

  }

  /**
   * The V1-compatible job-type prefix of user-task listeners: the listener type is
   * this prefix plus the user task's external form reference. MUST NOT change -
   * upgrading a V1 application has to produce a byte-identical BPMN so the
   * deployment does not create a new process version.
   */
  public static final String TASKDEFINITION_USERTASK_ZEEBE = "io.vanillabp.userTask:";

  /**
   * One user task to be served by listener-job workers (story 24).
   *
   * @param bpmnProcessId The BPMN process ID
   * @param activityId The BPMN activity ID
   * @param externalFormReference The <code>zeebe:formDefinition</code> external
   *          reference (= the VanillaBP task definition of the user task)
   */
  public record Camunda8UserTaskToWire(
                                       String bpmnProcessId,
                                       String activityId,
                                       String externalFormReference) {

    public BpmnTaskSpec toSpec() {

      return BpmnTaskSpec.userTask(activityId, externalFormReference);

    }

    /**
     * The job type of this user task's lifecycle listeners.
     */
    public String listenerJobType() {

      return TASKDEFINITION_USERTASK_ZEEBE + externalFormReference;

    }

  }

  /**
   * The Camunda-managed user tasks (<code>zeebe:userTask</code>) of the given
   * executable process AND - for NEW models - adds the V1-compatible lifecycle
   * task listeners to the model: per user task a <code>creating</code> listener as
   * the FIRST and a <code>canceling</code> listener as the LAST listener (custom
   * modeller-defined listeners stay in between), both with <code>retries="0"</code>
   * and the type {@link #TASKDEFINITION_USERTASK_ZEEBE} + external form reference.
   * A user task without an external form reference fails with a guiding message
   * (the V1 convention: the external form reference IS the task definition).
   */
  public static List<Camunda8UserTaskToWire> userTasksOf(
      final BpmnModelInstance model,
      final String bpmnProcessId,
      final String workflowModuleId,
      final String filename) {

    final var userTasks = new LinkedList<Camunda8UserTaskToWire>();
    model
        .getModelElementsByType(UserTask.class)
        .stream()
        .filter(task -> bpmnProcessId.equals(owningProcessId(task)))
        // only Camunda-managed user tasks (zeebe:userTask, the 8.8 default);
        // worker-based user tasks (a zeebe:taskDefinition instead) are handled
        // like service tasks by tasksOf
        .filter(task -> task.getSingleExtensionElement(ZeebeUserTask.class) != null)
        .forEach(task -> {
          final var formDefinition = task.getSingleExtensionElement(ZeebeFormDefinition.class);
          final var externalFormReference = formDefinition != null
              ? formDefinition.getExternalReference()
              : null;
          if ((externalFormReference == null) || externalFormReference.isBlank()) {
            throw new IllegalStateException(
                ("User task '%s' of BPMN process '%s' (file '%s', workflow module '%s') has no "
                    + "external form reference! VanillaBP's Camunda 8 convention: the user task's "
                    + "form is referenced externally and the reference IS the task definition - "
                    + "set 'External form reference' in the modeler (zeebe:formDefinition "
                    + "externalReference).")
                    .formatted(task.getId(), bpmnProcessId, filename, workflowModuleId));
          }
          addUserTaskListeners(task, externalFormReference);
          userTasks.add(new Camunda8UserTaskToWire(bpmnProcessId, task.getId(), externalFormReference));
        });
    return userTasks;

  }

  /**
   * V1 listener order per element: VanillaBP <code>creating</code> FIRST, any
   * custom listeners in between, VanillaBP <code>canceling</code> LAST. Listeners
   * already carrying the VanillaBP prefix are not duplicated (re-wiring an
   * already-processed model).
   */
  private static void addUserTaskListeners(
      final UserTask task,
      final String externalFormReference) {

    final var listenerJobType = TASKDEFINITION_USERTASK_ZEEBE + externalFormReference;

    final ZeebeTaskListeners taskListeners;
    final boolean isNew;
    if (task.getSingleExtensionElement(ZeebeTaskListeners.class) != null) {
      taskListeners = task.getSingleExtensionElement(ZeebeTaskListeners.class);
      final var alreadyWired = taskListeners
          .getTaskListeners()
          .stream()
          .anyMatch(listener -> listenerJobType.equals(listener.getType()));
      if (alreadyWired) {
        return;
      }
      isNew = false;
    } else {
      taskListeners = task.getExtensionElements().addExtensionElement(ZeebeTaskListeners.class);
      isNew = true;
    }

    final var createListener = task.getModelInstance().newInstance(ZeebeTaskListener.class);
    createListener.setEventType(ZeebeTaskListenerEventType.creating);
    createListener.setType(listenerJobType);
    createListener.setRetries("0");
    taskListeners.insertElementAfter(createListener, null); // first listener

    final var cancelListener = task.getModelInstance().newInstance(ZeebeTaskListener.class);
    cancelListener.setEventType(ZeebeTaskListenerEventType.canceling);
    cancelListener.setType(listenerJobType);
    cancelListener.setRetries("0");
    if (isNew) {
      taskListeners.insertElementAfter(cancelListener, createListener);
    } else {
      final var previousListeners = new LinkedList<>(taskListeners.getTaskListeners());
      taskListeners.insertElementAfter(
          cancelListener, previousListeners.isEmpty()
              ? createListener
              : previousListeners.getLast());
    }

  }

  /**
   * Wires the correlation keys of the given executable process' MESSAGE
   * subscriptions (story 23): publishing with
   * <code>correlationKey = workflow-aggregate ID</code> only correlates if the
   * deployed model's message subscriptions carry a matching
   * <code>zeebe:subscription</code> correlation-key expression. For every message
   * referenced by a catch element of this process WITHOUT such an expression the
   * V2 convention <code>=&lt;aggregate-ID variable&gt;</code> is INJECTED (the
   * engine-specific BPMN modification of the pipeline); an existing expression is
   * left untouched (the modeller may correlate by an own variable, e.g. for
   * correlation-id scenarios - V1 models keep working byte-identically). Message
   * START events need no correlation key and are skipped.
   *
   * @param model The BPMN model (modified in place)
   * @param bpmnProcessId The executable process to wire
   * @param aggregateIdVariableName Supplies the name of the process variable
   *          holding the workflow-aggregate ID - resolved ONLY if an injection is
   *          actually necessary, so a process without message catch elements does
   *          not require the aggregate's ID property to be resolvable
   */
  public static void wireMessageSubscriptions(
      final BpmnModelInstance model,
      final String bpmnProcessId,
      final java.util.function.Supplier<String> aggregateIdVariableName) {

    final var messages = new java.util.LinkedHashSet<io.camunda.zeebe.model.bpmn.instance.Message>();

    // intermediate catch events + boundary events + receive tasks of THIS process
    model
        .getModelElementsByType(io.camunda.zeebe.model.bpmn.instance.CatchEvent.class)
        .stream()
        .filter(event -> bpmnProcessId.equals(owningProcessId(event)))
        // message START events correlate by name only - no correlation key
        .filter(event -> !(event instanceof io.camunda.zeebe.model.bpmn.instance.StartEvent))
        .flatMap(event -> event
            .getEventDefinitions()
            .stream()
            .filter(io.camunda.zeebe.model.bpmn.instance.MessageEventDefinition.class::isInstance)
            .map(io.camunda.zeebe.model.bpmn.instance.MessageEventDefinition.class::cast))
        .map(io.camunda.zeebe.model.bpmn.instance.MessageEventDefinition::getMessage)
        .filter(java.util.Objects::nonNull)
        .forEach(messages::add);
    model
        .getModelElementsByType(io.camunda.zeebe.model.bpmn.instance.ReceiveTask.class)
        .stream()
        .filter(task -> bpmnProcessId.equals(owningProcessId(task)))
        .map(io.camunda.zeebe.model.bpmn.instance.ReceiveTask::getMessage)
        .filter(java.util.Objects::nonNull)
        .forEach(messages::add);

    messages.forEach(message -> {
      final var existing = message
          .getSingleExtensionElement(io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeSubscription.class);
      if (existing != null) {
        // the modeller correlates deliberately (e.g. by an own correlation-id
        // variable) - leave it untouched, V1 models stay byte-identical
        return;
      }
      final var extensionElements = message.getExtensionElements() != null
          ? message.getExtensionElements()
          : (io.camunda.zeebe.model.bpmn.instance.ExtensionElements) message
              .getModelInstance()
              .newInstance(io.camunda.zeebe.model.bpmn.instance.ExtensionElements.class);
      if (message.getExtensionElements() == null) {
        message.addChildElement(extensionElements);
      }
      final var subscription = extensionElements
          .addExtensionElement(io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeSubscription.class);
      subscription.setCorrelationKey("="
          + aggregateIdVariableName.get());
    });

  }

  private static String owningProcessId(
      final FlowElement element) {

    ModelElementInstance current = element;
    while (current != null) {
      if (current instanceof Process process) {
        return process.getId();
      }
      current = current.getParentElement();
    }
    return null;

  }

}
