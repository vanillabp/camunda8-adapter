package io.vanillabp.camunda8.wiring;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.camunda.bpm.model.xml.instance.ModelElementInstance;

import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.Process;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeCalledDecision;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeIoMapping;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeLoopCharacteristics;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeScript;

/**
 * Which process variables a worker of this adapter asks the cluster for.
 *
 * <p>
 * A Camunda 8 worker which names none gets the COMPLETE variable scope of the process
 * instance with every job, which Camunda describes as "tens or more variables, of
 * arbitrary size" and recommends against. VanillaBP is in a better position than a plain
 * client user: the workflow aggregate is the source of truth, so a handler is served from
 * the application's own database and the job itself has to carry only what the adapter
 * reads out of it.
 * </p>
 *
 * <p>
 * That is a short and computable list, and every entry of it is here:
 * </p>
 * <ul>
 * <li>the <strong>workflow aggregate's ID</strong>, in the variable named after the
 * aggregate's ID attribute
 * ({@code WorkflowTaskInvoker#resolveWorkflowAggregateIdName}). Every one of the four
 * worker kinds starts by reading it, and the name is a property of the BPMN process
 * rather than of the adapter, so a worker serving two processes which disagree carries
 * both names;</li>
 * <li>the <strong>multi-instance context</strong> of the element the job belongs to
 * (story 62): the index, the total and the element of every iteration enclosing it,
 * in the variables {@link Camunda8MultiInstance} injects while the model is deployed.
 * Which ones those are depends on the element, which is why this list is not a
 * constant;</li>
 * <li>every variable name the BPMN process itself DECLARES, see
 * {@link #declaredVariablesOf}: the targets of its input and output mappings, the result
 * variables of its scripts and called decisions, the output collections of its
 * multi-instance elements. The adapter does not read them, but a {@code @TaskParam} may,
 * and the model is the one place where the adapter can see such a name. The workflow-end
 * listener is left out of this one: a {@code @WorkflowEnded} method cannot declare a
 * {@code @TaskParam} at all, so the aggregate's ID is its complete list.</li>
 * </ul>
 *
 * <p>
 * What stays out is therefore what only the aggregate sync wrote into the instance: a copy
 * of the data the handler already holds, which on a workflow aggregate with a few large
 * attributes is the whole of what Camunda warns about.
 * </p>
 *
 * <p>
 * <strong>The list belongs to the WORKER, not to the delivery.</strong> A worker
 * subscribes to a job type and serves every task of the workflow module using it, across
 * BPMN processes, so its list is the union over everything it serves. The list is also
 * part of what the gateway compares when it decides whether two job streams are
 * equivalent (story 74), so it has to be the same on every node and after every restart
 * of one application version: it is therefore sorted, and derived from the deployed
 * models rather than from the iteration order of a hash map.
 * </p>
 *
 * <p>
 * <strong>Two exceptions where the derivation cannot win.</strong> A worker serving the
 * start events the cluster fires itself asks for everything, because VanillaBP copies
 * every variable such a start carries into the workflow aggregate
 * ({@code BpmsInitiatedStartContext#getVariables}) - there is no list to derive. And a
 * {@code @TaskParam} may name a variable which is neither VanillaBP's nor the model's: an
 * attribute of the aggregate, or something written past both of them. That case is the
 * reason for the escape hatch {@code vanillabp.adapters.<id>.fetch-variables: all}, and it
 * fails the delivery with a message naming the property rather than quietly handing the
 * method a <code>null</code>.
 * </p>
 */
public final class Camunda8FetchVariables {

  private Camunda8FetchVariables() {
  }

  /**
   * What {@code vanillabp.adapters.<id>.fetch-variables} may say.
   */
  public enum Mode {
    /**
     * Ask for the variables the adapter derived from the deployed models - the default,
     * and what keeps the payload of a job at what VanillaBP actually reads.
     */
    DERIVED,
    /**
     * Ask for the complete variable scope, which is what a Camunda 8 worker does when
     * nobody says otherwise. The escape hatch for the case the derivation misses: an
     * application reading a process variable with {@code @TaskParam}.
     */
    ALL
  }

  /**
   * What one worker asks for: either the complete scope, or the names below.
   *
   * @param all Whether the worker asks for every variable of the process instance
   * @param names The variable names to fetch, sorted; empty while {@link #all} is
   *          <code>true</code>
   */
  public record Selection(boolean all,
                          List<String> names) {

    /**
     * @return A selection asking for the complete variable scope
     */
    public static Selection everything() {

      return new Selection(true, List.of());

    }

    /**
     * @param names The variable names, in any order
     * @return A selection asking for those names, sorted so it is stable across
     *         restarts
     */
    public static Selection of(
        final Collection<String> names) {

      return new Selection(false, List.copyOf(new TreeSet<>(names)));

    }

    /**
     * @param name A variable name
     * @return Whether a job of this worker carries that variable
     */
    public boolean covers(
        final String name) {

      return all || names.contains(name);

    }

    /**
     * @return What the startup line and the guiding messages call this selection
     */
    public String describe() {

      return all
          ? "all variables of the process instance"
          : names.toString();

    }

  }

  /**
   * The property key of the escape hatch, at the level a reader has to change it.
   *
   * @param adapterId The adapter id
   * @return The full property key
   */
  public static String propertyKey(
      final String adapterId) {

    return io.vanillabp.camunda8.client.Camunda8AdapterConfiguration.propertyKey(adapterId, "fetch-variables");

  }

  /**
   * What a delivery says when the variable holding the workflow aggregate's ID is not
   * there. Until story 93 there was one cause, a workflow started past VanillaBP; since
   * the workers name what they fetch, a list which does not carry the name is the second
   * one, so the message names the list too.
   *
   * @param what What kind of job it is, capitalized ("Job", "The user-task listener job")
   * @param jobKey The job's key
   * @param taskDefinition The task definition, as the core knows it
   * @param bpmnProcessId The BPMN process id, as the core knows it
   * @param aggregateIdName The variable the aggregate's ID was expected in
   * @param adapterId The adapter id, for the property key
   * @param selection What this worker fetches
   * @return The message
   */
  public static String missingAggregateId(
      final String what,
      final long jobKey,
      final String taskDefinition,
      final String bpmnProcessId,
      final String aggregateIdName,
      final String adapterId,
      final Selection selection) {

    return """
        %s '%s' (type '%s') of BPMN process '%s' carries no variable '%s' holding the workflow \
        aggregate's ID! Either the workflow was not started through VanillaBP (the variable is \
        written on start), or its worker did not ask for that variable: it fetches %s. Set '%s' \
        to 'all' to have this worker fetch the complete variable scope."""
        .formatted(
            what,
            jobKey,
            taskDefinition,
            bpmnProcessId,
            aggregateIdName,
            selection.describe(),
            propertyKey(adapterId));

  }

  /**
   * What a delivery says when a <code>&#64;TaskParam</code> names a variable this worker
   * did not fetch. The adapter cannot tell that case apart from a variable which is
   * genuinely absent, and handing the method a <code>null</code> would be a silent loss
   * of what the model computed - so the delivery fails, which on this BPMS means retries
   * and then an incident naming the way out.
   *
   * @param name The variable the method asked for
   * @param taskDefinition The task definition, as the core knows it
   * @param adapterId The adapter id, for the property key
   * @param selection What this worker fetches
   * @return The message
   */
  public static String unfetchedTaskParameter(
      final String name,
      final String taskDefinition,
      final String adapterId,
      final Selection selection) {

    return """
        The @WorkflowTask method serving '%s' reads the process variable '%s' with @TaskParam, \
        but its worker does not fetch that variable: it fetches %s. VanillaBP derives the fetch \
        list from the deployed models and cannot see what your method reads. Either read the \
        value from the workflow aggregate, which is what VanillaBP is about, or set '%s' to \
        'all' - at task level for this one task, or at workflow, workflow-module or adapter \
        level."""
        .formatted(taskDefinition, name, selection.describe(), propertyKey(adapterId));

  }

  /**
   * Every variable name the given BPMN process DECLARES, which is the second half of what
   * a worker fetches.
   *
   * <p>
   * A model hands values to its tasks and computes values of its own, and a
   * {@code @TaskParam} reading one of them reads something the adapter cannot see in any
   * registry - but it CAN see it in the model, which is why these names are part of the
   * derivation rather than of the escape hatch. What VanillaBP leaves out is therefore
   * exactly what only the aggregate sync put into the instance: a copy of data the handler
   * already holds, and the payload the recommendation to fetch less is about.
   * </p>
   *
   * <p>
   * Four constructs declare a variable in Camunda 8, and all four exist on every release
   * line: the input and output mappings of {@code zeebe:ioMapping}, the result variable of
   * an inline script, the result variable of a called decision, and the output collection
   * of a multi-instance element.
   * </p>
   *
   * <p>
   * Call it BEFORE {@link Camunda8MultiInstance#wire}, so the mappings VanillaBP injects
   * itself do not end up in the list of processes whose elements they do not enclose.
   * </p>
   *
   * @param model The BPMN model, as it will be deployed
   * @param bpmnProcessId The process to scan, as the cluster will know it
   * @return The declared variable names
   */
  public static Set<String> declaredVariablesOf(
      final BpmnModelInstance model,
      final String bpmnProcessId) {

    final var names = new TreeSet<String>();
    for (final var ioMapping : model.getModelElementsByType(ZeebeIoMapping.class)) {
      if (!belongsTo(ioMapping, bpmnProcessId)) {
        continue;
      }
      ioMapping.getInputs().forEach(input -> add(names, input.getTarget()));
      ioMapping.getOutputs().forEach(output -> add(names, output.getTarget()));
    }
    for (final var script : model.getModelElementsByType(ZeebeScript.class)) {
      if (belongsTo(script, bpmnProcessId)) {
        add(names, script.getResultVariable());
      }
    }
    for (final var decision : model.getModelElementsByType(ZeebeCalledDecision.class)) {
      if (belongsTo(decision, bpmnProcessId)) {
        add(names, decision.getResultVariable());
      }
    }
    for (final var loop : model.getModelElementsByType(ZeebeLoopCharacteristics.class)) {
      if (belongsTo(loop, bpmnProcessId)) {
        add(names, loop.getOutputCollection());
      }
    }
    return names;

  }

  private static void add(
      final Set<String> names,
      final String name) {

    if ((name != null) && !name.isBlank()) {
      names.add(name.trim());
    }

  }

  /**
   * Whether a model element sits inside the given process - one BPMN file may hold
   * several.
   */
  private static boolean belongsTo(
      final ModelElementInstance element,
      final String bpmnProcessId) {

    var current = element.getParentElement();
    while (current != null) {
      if (current instanceof Process process) {
        return bpmnProcessId.equals(process.getId());
      }
      current = current.getParentElement();
    }
    return false;

  }

  /**
   * The variables one served element contributes: the workflow aggregate's ID plus the
   * multi-instance context of the iterations enclosing that element.
   *
   * @param variables Where the names are collected
   * @param aggregateIdName The name of the aggregate's ID variable of the element's
   *          BPMN process
   * @param chain The multi-instance elements enclosing the element, from
   *          {@link Camunda8MultiInstance.Registry#chainOf(String, String)}
   */
  public static void collect(
      final Set<String> variables,
      final String aggregateIdName,
      final List<Camunda8MultiInstance.MultiInstanceElement> chain) {

    if (aggregateIdName != null) {
      variables.add(aggregateIdName);
    }
    for (final var element : chain) {
      variables.add(element.indexVariable());
      if (element.totalVariable() != null) {
        variables.add(element.totalVariable());
      }
      if (element.elementVariable() != null) {
        variables.add(element.elementVariable());
      }
    }

  }

}
