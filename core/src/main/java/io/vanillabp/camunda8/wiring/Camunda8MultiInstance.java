package io.vanillabp.camunda8.wiring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.Activity;
import io.camunda.zeebe.model.bpmn.instance.BaseElement;
import io.camunda.zeebe.model.bpmn.instance.ExtensionElements;
import io.camunda.zeebe.model.bpmn.instance.FlowElement;
import io.camunda.zeebe.model.bpmn.instance.MultiInstanceLoopCharacteristics;
import io.camunda.zeebe.model.bpmn.instance.Process;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeInput;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeIoMapping;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeLoopCharacteristics;
import io.vanillabp.integration.adapter.spi.workflowtask.MultiInstanceValue;
import lombok.extern.slf4j.Slf4j;

/**
 * What Camunda 8 knows about the iteration a job belongs to, made available to the
 * core.
 *
 * <p>
 * Camunda 7 answers this from the execution hierarchy at runtime. Camunda 8 has no
 * such hierarchy on the client side: a job carries variables and an element ID, and
 * nothing else. Of the three values the SPI asks for, exactly one is there by
 * itself:
 * </p>
 * <ul>
 * <li><strong>the element</strong> is the variable named by
 * <code>inputElement</code>, but a NESTED iteration using the same name shadows the
 * outer one;</li>
 * <li><strong>the index</strong> is <code>loopCounter</code>, counted from 1 rather
 * than from 0, and shadowed the same way;</li>
 * <li><strong>the total</strong> does not exist at all - this engine has no
 * <code>nrOfInstances</code>, and it has no loop cardinality either, so a
 * multi-instance element always iterates over a collection.</li>
 * </ul>
 *
 * <p>
 * So the values are made unambiguous while the BPMN is deployed, which is the stage
 * VanillaBP modifies models anyway: every multi-instance element gets input mappings
 * copying its own index, its own total and its own element into variables named
 * after that element. Those names cannot be shadowed, and a job of a task nested in
 * three iterations sees all three of them. What it costs is three local variables per
 * instance, all of them scalar except the element, which the model was handing over
 * anyway.
 * </p>
 *
 * <p>
 * The chain of multi-instance elements enclosing a task is model knowledge, so it is
 * collected while deploying and kept per process and element - a job carries the ID
 * of its own element only.
 * </p>
 * <p>
 * Why the adapter puts input mappings into the deployed model, why they are idempotent, and why
 * that costs a new process version, is decision 5 in the repository's DECISIONS.md.
 */
@Slf4j
public final class Camunda8MultiInstance {

  /** Prefix of every variable this class injects. */
  static final String VARIABLE_PREFIX = "vanillabpMi";

  private Camunda8MultiInstance() {
  }

  /**
   * One multi-instance element and the variables carrying what it knows about the
   * current iteration.
   *
   * @param elementId The BPMN ID of the multi-instance element - the key the SPI
   *          uses, and what the application writes into
   *          <code>@MultiInstanceElement("...")</code>
   * @param indexVariable The variable holding this element's <code>loopCounter</code>
   * @param totalVariable The variable holding the size of this element's collection,
   *          or <code>null</code> if the element has no input collection
   * @param elementVariable The variable holding this element's current element, or
   *          <code>null</code> if the model declares no <code>inputElement</code>
   */
  public record MultiInstanceElement(
                                     String elementId,
                                     String indexVariable,
                                     String totalVariable,
                                     String elementVariable) {
  }

  /**
   * Which multi-instance elements enclose a BPMN element, outermost first. Filled
   * while deploying, read while dispatching a job.
   */
  public static class Registry {

    private final Map<String, List<MultiInstanceElement>> chains = new ConcurrentHashMap<>();

    private static String key(
        final String bpmnProcessId,
        final String elementId) {

      return bpmnProcessId
          + "#"
          + elementId;

    }

    void register(
        final String bpmnProcessId,
        final String elementId,
        final List<MultiInstanceElement> chain) {

      chains.put(key(bpmnProcessId, elementId), List.copyOf(chain));

    }

    /**
     * @param bpmnProcessId The BPMN process ID as the CLUSTER knows it
     * @param elementId The BPMN element ID the job reports
     * @return The multi-instance elements enclosing that element, outermost first
     */
    public List<MultiInstanceElement> chainOf(
        final String bpmnProcessId,
        final String elementId) {

      return chains.getOrDefault(key(bpmnProcessId, elementId), List.of());

    }

  }

  /**
   * Prepares a process for multi-instance: injects the input mappings which make the
   * values of every iteration unambiguous and records which elements are enclosed by
   * which iterations.
   *
   * @param model The BPMN model, about to be deployed
   * @param bpmnProcessId The process to prepare, as the cluster will know it
   * @param registry Where the chains are recorded
   */
  public static void wire(
      final BpmnModelInstance model,
      final String bpmnProcessId,
      final Registry registry) {

    final var process = model
        .getModelElementsByType(Process.class)
        .stream()
        .filter(candidate -> bpmnProcessId.equals(candidate.getId()))
        .findFirst()
        .orElse(null);
    if (process == null) {
      return;
    }

    final var elements = new LinkedHashMap<String, MultiInstanceElement>();
    final var variableNames = new LinkedHashMap<String, String>();
    for (final var activity : process.getChildElementsByType(Activity.class)) {
      collect(activity, elements, variableNames, bpmnProcessId);
    }
    // no multi-instance in this process - nothing to inject and nothing to remember
    if (elements.isEmpty()) {
      return;
    }

    for (final var flowElement : process.getModelInstance().getModelElementsByType(FlowElement.class)) {
      if (!bpmnProcessId.equals(owningProcessId(flowElement))) {
        continue;
      }
      final var chain = chainOf(flowElement, elements);
      if (!chain.isEmpty()) {
        registry.register(bpmnProcessId, flowElement.getId(), chain);
      }
    }

  }

  /**
   * Walks an activity and everything below it, injecting the input mappings of every
   * multi-instance element found.
   */
  private static void collect(
      final Activity activity,
      final Map<String, MultiInstanceElement> elements,
      final Map<String, String> variableNames,
      final String bpmnProcessId) {

    if (activity.getLoopCharacteristics() instanceof MultiInstanceLoopCharacteristics loopCharacteristics) {
      final var element = describe(activity, loopCharacteristics, variableNames, bpmnProcessId);
      elements.put(activity.getId(), element);
      inject(activity, element, loopCharacteristics);
    }
    activity
        .getChildElementsByType(Activity.class)
        .forEach(child -> collect(child, elements, variableNames, bpmnProcessId));

  }

  /**
   * The variables one multi-instance element uses. Their names are derived from the
   * element's ID, which is what makes them impossible to shadow.
   */
  private static MultiInstanceElement describe(
      final Activity activity,
      final MultiInstanceLoopCharacteristics loopCharacteristics,
      final Map<String, String> variableNames,
      final String bpmnProcessId) {

    final var elementId = activity.getId();
    final var suffix = variableSuffix(elementId);
    final var clashing = variableNames.put(suffix, elementId);
    if ((clashing != null) && !clashing.equals(elementId)) {
      throw new IllegalStateException(
          """
              The multi-instance elements '%s' and '%s' of BPMN process '%s' cannot be told apart by \
              VanillaBP: their IDs differ only in characters which are not valid in a Camunda 8 \
              variable name ('%s' for both). Rename one of them."""
              .formatted(clashing, elementId, bpmnProcessId, suffix));
    }

    final var zeebeLoopCharacteristics = loopCharacteristics
        .getSingleExtensionElement(ZeebeLoopCharacteristics.class);
    final var inputElement = zeebeLoopCharacteristics == null
        ? null
        : zeebeLoopCharacteristics.getInputElement();
    final var inputCollection = zeebeLoopCharacteristics == null
        ? null
        : zeebeLoopCharacteristics.getInputCollection();

    return new MultiInstanceElement(
        elementId, VARIABLE_PREFIX
            + "Index_"
            + suffix, isBlank(inputCollection)
                ? null
                : VARIABLE_PREFIX
                    + "Total_"
                    + suffix, isBlank(inputElement)
                        ? null
                        : VARIABLE_PREFIX
                            + "Element_"
                            + suffix);

  }

  /**
   * Adds the input mappings of one multi-instance element, keeping whatever mappings
   * the model already declares and staying idempotent for a redeployment.
   */
  private static void inject(
      final Activity activity,
      final MultiInstanceElement element,
      final MultiInstanceLoopCharacteristics loopCharacteristics) {

    final var zeebeLoopCharacteristics = loopCharacteristics
        .getSingleExtensionElement(ZeebeLoopCharacteristics.class);

    addInput(activity, element.indexVariable(), "=loopCounter");
    if (element.totalVariable() != null) {
      // the collection is a FEEL expression, not necessarily a variable name, so the
      // size is asked of the expression itself
      addInput(
          activity,
          element.totalVariable(),
          "=count(%s)".formatted(withoutFeelPrefix(zeebeLoopCharacteristics.getInputCollection())));
    }
    if (element.elementVariable() != null) {
      addInput(activity, element.elementVariable(), "="
          + zeebeLoopCharacteristics.getInputElement());
    }

  }

  private static void addInput(
      final Activity activity,
      final String target,
      final String source) {

    if (activity.getExtensionElements() == null) {
      activity
          .setExtensionElements(
              activity
                  .getModelInstance()
                  .newInstance(ExtensionElements.class));
    }
    var ioMapping = activity.getSingleExtensionElement(ZeebeIoMapping.class);
    if (ioMapping == null) {
      ioMapping = activity
          .getExtensionElements()
          .addExtensionElement(ZeebeIoMapping.class);
    }
    final var alreadyThere = ioMapping
        .getInputs()
        .stream()
        .anyMatch(input -> target.equals(input.getTarget()));
    if (alreadyThere) {
      return;
    }
    final var input = activity
        .getModelInstance()
        .newInstance(ZeebeInput.class);
    input.setTarget(target);
    input.setSource(source);
    ioMapping.addChildElement(input);

  }

  /**
   * The multi-instance elements enclosing a BPMN element, outermost first - the order
   * the SPI defines. The element itself is part of the chain when it is
   * multi-instance, which is the usual case: a multi-instance service task.
   */
  private static List<MultiInstanceElement> chainOf(
      final FlowElement flowElement,
      final Map<String, MultiInstanceElement> elements) {

    final var innermostFirst = new LinkedList<MultiInstanceElement>();
    var current = (BaseElement) flowElement;
    while (current != null) {
      final var element = elements.get(current.getId());
      if (element != null) {
        innermostFirst.add(element);
      }
      current = current.getParentElement() instanceof BaseElement parent
          ? parent
          : null;
    }
    final var outermostFirst = new ArrayList<>(innermostFirst);
    Collections.reverse(outermostFirst);
    return outermostFirst;

  }

  /**
   * Builds what the SPI asks for out of the variables a job carries.
   *
   * @param chain The multi-instance elements enclosing the job's element, outermost
   *          first
   * @param variables The job's variables
   * @return The multi-instance contexts, keyed by the ID of the multi-instance
   *         element, outermost first
   */
  public static Map<String, MultiInstanceValue> valuesOf(
      final List<MultiInstanceElement> chain,
      final Map<String, Object> variables) {

    if (chain.isEmpty()) {
      return Map.of();
    }
    final var result = new LinkedHashMap<String, MultiInstanceValue>();
    for (final var element : chain) {
      final var index = intOf(variables.get(element.indexVariable()));
      if (index == null) {
        // a process deployed before this adapter version knew about multi-instance:
        // the mappings are missing, and the core's message names what was supplied
        log.debug(
            "Camunda8: no variable '{}' - multi-instance element '{}' cannot be reported. Redeploy "
                + "the workflow module to have VanillaBP add the mappings.",
            element.indexVariable(),
            element.elementId());
        continue;
      }
      final var total = element.totalVariable() == null
          ? null
          : intOf(variables.get(element.totalVariable()));
      final var currentElement = element.elementVariable() == null
          ? null
          : variables.get(element.elementVariable());
      // Camunda 8 counts iterations from 1, the SPI counts from 0 like Camunda 7 does
      result.put(
          element.elementId(),
          new MultiInstanceValue(
              currentElement, index - 1, total == null
                  ? -1
                  : total));
    }
    return result;

  }

  private static Integer intOf(
      final Object value) {

    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value instanceof String text) {
      try {
        return Integer.valueOf(text);
      } catch (final NumberFormatException e) {
        return null;
      }
    }
    return null;

  }

  /**
   * The element ID, reduced to what a Camunda 8 variable name may consist of.
   */
  static String variableSuffix(
      final String elementId) {

    return elementId.replaceAll("[^A-Za-z0-9_]", "_");

  }

  private static String withoutFeelPrefix(
      final String expression) {

    final var trimmed = expression.trim();
    return trimmed.startsWith("=")
        ? trimmed.substring(1)
        : trimmed;

  }

  private static boolean isBlank(
      final String value) {

    return (value == null) || value.isBlank();

  }

  /**
   * The process a flow element belongs to - a model file may hold several.
   */
  private static String owningProcessId(
      final FlowElement flowElement) {

    var current = flowElement.getParentElement();
    while (current != null) {
      if (current instanceof Process process) {
        return process.getId();
      }
      current = current.getParentElement();
    }
    return null;

  }

}
