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
   * The job-type prefix of the start execution listeners VanillaBP attaches to start
   * events the cluster fires on its own: this prefix plus the scoped BPMN
   * process id plus the start event's id. The job type has to be unique across the
   * tenant, hence the process id - and one worker serves exactly one start event.
   */
  public static final String TASKDEFINITION_BPMS_INITIATED_START = "io.vanillabp.bpmsStart:";

  /**
   * The job-type prefix of the end execution listener VanillaBP attaches to a
   * process whose application wants to be told that a workflow ended:
   * this prefix plus the scoped BPMN process id.
   */
  public static final String TASKDEFINITION_WORKFLOW_ENDED = "io.vanillabp.workflowEnd:";

  /**
   * @param scopedBpmnProcessId The BPMN process id the cluster knows
   * @return The job type of the process' end execution listener
   */
  public static String workflowEndedJobTypeOf(
      final String scopedBpmnProcessId) {

    return TASKDEFINITION_WORKFLOW_ENDED + scopedBpmnProcessId;

  }

  /**
   * The version tag the modeller gave the process
   * (<code>zeebe:versionTag</code>) - the name a
   * <code>&#64;WorkflowTask(version = "release-2026")</code> refers to.
   *
   * @param model The BPMN model as deployed
   * @param bpmnProcessId The SCOPED BPMN process id
   * @return The version tag or <code>null</code> if the model carries none
   */
  public static String versionTagOf(
      final BpmnModelInstance model,
      final String bpmnProcessId) {

    final var process = model
        .getModelElementsByType(Process.class)
        .stream()
        .filter(candidate -> bpmnProcessId.equals(candidate.getId()))
        .findFirst()
        .orElse(null);
    if (process == null) {
      return null;
    }
    final var versionTag = process
        .getSingleExtensionElement(io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeVersionTag.class);
    return versionTag == null
        ? null
        : versionTag.getValue();

  }

  /**
   * Attaches an <code>end</code> execution listener to the PROCESS element, which
   * is what tells VanillaBP that a workflow ended. Only called where the
   * application declared a <code>&#64;WorkflowEnded</code> method: a model must not
   * pay for a notification nobody asked for.
   *
   * @param model The BPMN model, already scoped by <code>prepareBpmn</code>
   * @param bpmnProcessId The SCOPED BPMN process id
   * @return Whether a listener was attached (false if the process is not in this
   *         model)
   */
  public static boolean attachWorkflowEndedListener(
      final BpmnModelInstance model,
      final String bpmnProcessId) {

    final var process = model
        .getModelElementsByType(io.camunda.zeebe.model.bpmn.instance.Process.class)
        .stream()
        .filter(candidate -> bpmnProcessId.equals(candidate.getId()))
        .findFirst()
        .orElse(null);
    if (process == null) {
      return false;
    }

    final var jobType = workflowEndedJobTypeOf(bpmnProcessId);
    final io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners listeners;
    if (process
        .getSingleExtensionElement(
            io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners.class) != null) {
      listeners = process
          .getSingleExtensionElement(
              io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners.class);
      final var alreadyWired = listeners
          .getExecutionListeners()
          .stream()
          .anyMatch(listener -> jobType.equals(listener.getType()));
      if (alreadyWired) {
        return true;
      }
    } else {
      if (process.getExtensionElements() == null) {
        process
            .setExtensionElements(
                process
                    .getModelInstance()
                    .newInstance(io.camunda.zeebe.model.bpmn.instance.ExtensionElements.class));
      }
      listeners = process
          .getExtensionElements()
          .addExtensionElement(io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners.class);
    }

    final var listener = process
        .getModelInstance()
        .newInstance(io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListener.class);
    listener
        .setEventType(io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListenerEventType.end);
    listener.setType(jobType);
    // LAST listener: whatever the model itself does at the end of the process runs
    // before the application is told the workflow ended
    listeners.getExecutionListeners().forEach(existing -> {
    });
    listeners.addChildElement(listener);
    return true;

  }

  /**
   * One start event the cluster fires on its own, to be served by a start
   * execution-listener worker.
   *
   * @param bpmnProcessId The SCOPED BPMN process id (what the cluster knows)
   * @param startEventId The BPMN id of the start event
   * @param kind Which kind of start event it is
   * @param signalName The PLAIN signal name for a signal start event
   */
  public record Camunda8BpmsInitiatedStartToWire(
                                                 String bpmnProcessId,
                                                 String startEventId,
                                                 io.vanillabp.spi.service.BpmsStartTrigger.Kind kind,
                                                 String signalName) {

    /**
     * @return The job type of this start event's execution listener
     */
    public String listenerJobType() {

      return listenerJobTypeOf(bpmnProcessId, startEventId);

    }

  }

  /**
   * @param scopedBpmnProcessId The BPMN process id the cluster knows
   * @param startEventId The BPMN id of the start event
   * @return The job type of the start event's execution listener
   */
  public static String listenerJobTypeOf(
      final String scopedBpmnProcessId,
      final String startEventId) {

    return "%s%s:%s".formatted(TASKDEFINITION_BPMS_INITIATED_START, scopedBpmnProcessId, startEventId);

  }

  /**
   * The start events of the given executable process which the CLUSTER fires on its
   * own (timer, signal, conditional) - and attaches a <code>start</code> execution
   * listener to each of them, which is how VanillaBP learns about such a start and
   * gets to build the workflow aggregate before anything else runs.
   * <p>
   * Message start events are not among them: those are triggered by the application
   * through {@code ProcessService#startWorkflowByMessage}, which carries the
   * aggregate. Camunda 8 has no conditional events at all; the kind is part of the
   * model here so an unsupported model fails at the cluster, not silently.
   *
   * @param model The BPMN model, already scoped by <code>prepareBpmn</code>
   * @param bpmnProcessId The SCOPED BPMN process id
   * @param signalNameResolver Turns the scoped signal name of the model into the
   *          plain one the application modelled
   * @return The start events to be wired
   */
  public static List<Camunda8BpmsInitiatedStartToWire> bpmsInitiatedStartsOf(
      final BpmnModelInstance model,
      final String bpmnProcessId,
      final java.util.function.UnaryOperator<String> signalNameResolver) {

    final var startEvents = new LinkedList<Camunda8BpmsInitiatedStartToWire>();
    model
        .getModelElementsByType(io.camunda.zeebe.model.bpmn.instance.StartEvent.class)
        .stream()
        .filter(startEvent -> bpmnProcessId.equals(owningProcessId(startEvent)))
        .forEach(startEvent -> {
          final var definitions = startEvent.getEventDefinitions();
          final var timer = definitions
              .stream()
              .anyMatch(io.camunda.zeebe.model.bpmn.instance.TimerEventDefinition.class::isInstance);
          final var signal = definitions
              .stream()
              .filter(io.camunda.zeebe.model.bpmn.instance.SignalEventDefinition.class::isInstance)
              .map(io.camunda.zeebe.model.bpmn.instance.SignalEventDefinition.class::cast)
              .findFirst();
          final var conditional = definitions
              .stream()
              .anyMatch(io.camunda.zeebe.model.bpmn.instance.ConditionalEventDefinition.class::isInstance);

          final io.vanillabp.spi.service.BpmsStartTrigger.Kind kind;
          final String signalName;
          if (timer) {
            kind = io.vanillabp.spi.service.BpmsStartTrigger.Kind.TIMER;
            signalName = null;
          } else if (signal.isPresent()) {
            kind = io.vanillabp.spi.service.BpmsStartTrigger.Kind.SIGNAL;
            signalName = signal
                .map(definition -> definition.getSignal() == null
                    ? null
                    : definition.getSignal().getName())
                .map(signalNameResolver)
                .orElse(null);
          } else if (conditional) {
            kind = io.vanillabp.spi.service.BpmsStartTrigger.Kind.CONDITIONAL;
            signalName = null;
          } else {
            return;
          }

          addStartExecutionListener(startEvent, listenerJobTypeOf(bpmnProcessId, startEvent.getId()));
          startEvents
              .add(
                  new Camunda8BpmsInitiatedStartToWire(
                      bpmnProcessId, startEvent.getId(), kind, signalName));
        });
    return startEvents;

  }

  /**
   * Attaches a <code>start</code> execution listener to the start event, unless the
   * model already carries it (re-wiring an already-processed model). Retries stay at
   * the Camunda default: unlike the user-task listeners, a failure here means the
   * workflow has no aggregate, which is worth retrying before it becomes an incident.
   */
  private static void addStartExecutionListener(
      final io.camunda.zeebe.model.bpmn.instance.StartEvent startEvent,
      final String listenerJobType) {

    final io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners listeners;
    if (startEvent
        .getSingleExtensionElement(
            io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners.class) != null) {
      listeners = startEvent
          .getSingleExtensionElement(
              io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners.class);
      final var alreadyWired = listeners
          .getExecutionListeners()
          .stream()
          .anyMatch(listener -> listenerJobType.equals(listener.getType()));
      if (alreadyWired) {
        return;
      }
    } else {
      // a start event carrying no extension elements at all: the container has to
      // be created before a listener can be added to it
      if (startEvent.getExtensionElements() == null) {
        startEvent
            .setExtensionElements(
                startEvent
                    .getModelInstance()
                    .newInstance(io.camunda.zeebe.model.bpmn.instance.ExtensionElements.class));
      }
      listeners = startEvent
          .getExtensionElements()
          .addExtensionElement(io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners.class);
    }

    final var listener = startEvent
        .getModelInstance()
        .newInstance(io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListener.class);
    // 'end' on the start event, not 'start': the cluster rejects start execution
    // listeners on start events (8.8), while an end listener still gates the
    // transition - it runs before the flow leaves the start event, so nothing of
    // the process can run before the workflow aggregate exists
    listener
        .setEventType(io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListenerEventType.end);
    listener.setType(listenerJobType);
    // the workflow aggregate is built here: VanillaBP's listener has to run before
    // any listener the model brings along
    listeners.insertElementAfter(listener, null);

  }

  /**
   * One user task to be served by listener-job workers.
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
   * subscriptions: publishing with
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

  /**
   * The IDs of the elements of the given process which can put a SECOND token into a
   * running workflow: a boundary event which does not cancel its activity,
   * a parallel or inclusive gateway forking into more than one sequence flow, an
   * activity marked as a PARALLEL multi-instance, and an event subprocess whose start
   * event does not interrupt the process. Two tokens are two branches writing the
   * same workflow aggregate - what that means is the core's decision, this method
   * only reads the model.
   *
   * @param model The BPMN model
   * @param bpmnProcessId The process' ID as the model knows it (the SCOPED ID)
   * @return The element IDs, possibly empty
   */
  public static List<String> concurrentTokenElementIdsOf(
      final BpmnModelInstance model,
      final String bpmnProcessId) {

    return Stream
        .of(
            elementsOf(model, bpmnProcessId, io.camunda.zeebe.model.bpmn.instance.BoundaryEvent.class)
                .filter(boundaryEvent -> !boundaryEvent.cancelActivity()),
            elementsOf(model, bpmnProcessId, io.camunda.zeebe.model.bpmn.instance.ParallelGateway.class)
                .filter(gateway -> gateway.getOutgoing().size() > 1),
            elementsOf(model, bpmnProcessId, io.camunda.zeebe.model.bpmn.instance.InclusiveGateway.class)
                .filter(gateway -> gateway.getOutgoing().size() > 1),
            elementsOf(model, bpmnProcessId, io.camunda.zeebe.model.bpmn.instance.Activity.class)
                .filter(Camunda8TaskWiring::isParallelMultiInstance),
            elementsOf(model, bpmnProcessId, io.camunda.zeebe.model.bpmn.instance.SubProcess.class)
                .filter(Camunda8TaskWiring::isNonInterruptingEventSubProcess))
        .flatMap(elements -> elements)
        .map(FlowElement::getId)
        .distinct()
        .toList();

  }

  private static <T extends FlowElement> Stream<T> elementsOf(
      final BpmnModelInstance model,
      final String bpmnProcessId,
      final Class<T> type) {

    return model
        .getModelElementsByType(type)
        .stream()
        .filter(element -> bpmnProcessId.equals(owningProcessId(element)));

  }

  private static boolean isParallelMultiInstance(
      final io.camunda.zeebe.model.bpmn.instance.Activity activity) {

    return (activity
        .getLoopCharacteristics() instanceof io.camunda.zeebe.model.bpmn.instance.MultiInstanceLoopCharacteristics loop) && !loop
            .isSequential();

  }

  private static boolean isNonInterruptingEventSubProcess(
      final io.camunda.zeebe.model.bpmn.instance.SubProcess subProcess) {

    return subProcess.triggeredByEvent() && subProcess
        .getChildElementsByType(io.camunda.zeebe.model.bpmn.instance.StartEvent.class)
        .stream()
        .anyMatch(startEvent -> !startEvent.isInterrupting());

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
