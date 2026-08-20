package io.vanillabp.camunda8.deployment;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.wiring.Camunda8FetchVariables;
import io.vanillabp.camunda8.wiring.Camunda8FetchVariablesResolver;
import io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What a worker of this adapter asks the cluster for (story 93). The derivation has to be
 * COMPLETE: a variable missing from the list is a variable the handler simply does not
 * see any more, and nothing fails to say so. The cases below are the ones the adapter
 * reads a variable in.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8FetchVariablesTest {

  private static final String MODULE = "test-module";

  /**
   * Two processes of one workflow module, whose tasks share the task definition
   * <code>approve</code> - so ONE worker serves both.
   */
  private static final String TWO_PROCESSES = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:process id="Loans" isExecutable="true">
          <bpmn:serviceTask id="ApproveLoan">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="approve" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
        </bpmn:process>
        <bpmn:process id="Cards" isExecutable="true">
          <bpmn:serviceTask id="ApproveCard">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="approve" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
        </bpmn:process>
      </bpmn:definitions>
      """;

  /**
   * A task nested in a multi-instance subprocess, itself multi-instance - the case which
   * makes the fetch list depend on the ELEMENT rather than on the process.
   */
  private static final String NESTED_MULTI_INSTANCE = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:process id="MiProcess" isExecutable="true">
          <bpmn:subProcess id="Outer">
            <bpmn:multiInstanceLoopCharacteristics>
              <bpmn:extensionElements>
                <zeebe:loopCharacteristics inputCollection="=groups" inputElement="group" />
              </bpmn:extensionElements>
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:serviceTask id="Inner">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="inner" />
              </bpmn:extensionElements>
              <bpmn:multiInstanceLoopCharacteristics>
                <bpmn:extensionElements>
                  <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
                </bpmn:extensionElements>
              </bpmn:multiInstanceLoopCharacteristics>
            </bpmn:serviceTask>
          </bpmn:subProcess>
        </bpmn:process>
      </bpmn:definitions>
      """;

  /**
   * What a model hands to its own tasks and computes for itself - the four constructs
   * which declare a variable in Camunda 8.
   */
  private static final String DECLARING_MODEL = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:process id="Declaring" isExecutable="true">
          <bpmn:serviceTask id="Rate">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="rate" />
              <zeebe:ioMapping>
                <zeebe:input source="=&quot;acme-rating&quot;" target="ratingProvider" />
                <zeebe:output source="=result" target="rating" />
              </zeebe:ioMapping>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:scriptTask id="Compute">
            <bpmn:extensionElements>
              <zeebe:script expression="=1 + 1" resultVariable="computed" />
            </bpmn:extensionElements>
          </bpmn:scriptTask>
          <bpmn:businessRuleTask id="Decide">
            <bpmn:extensionElements>
              <zeebe:calledDecision decisionId="risk" resultVariable="risk" />
            </bpmn:extensionElements>
          </bpmn:businessRuleTask>
        </bpmn:process>
      </bpmn:definitions>
      """;

  /**
   * A deployment service whose core answers the given aggregate-ID variable per BPMN
   * process, with the given <code>fetch-variables</code> resolution and no
   * <code>&#64;TaskParam</code> anywhere.
   */
  private Camunda8DeploymentService deploymentService(
      final java.util.function.Function<String, String> aggregateIdNames,
      final Camunda8FetchVariablesResolver fetchVariables) {

    return deploymentService(aggregateIdNames, fetchVariables, taskDefinition -> List.of());

  }

  /**
   * A deployment service whose core answers the given aggregate-ID variable per BPMN
   * process and the given <code>&#64;TaskParam</code> names per task definition - the two
   * questions the derivation asks it.
   */
  private Camunda8DeploymentService deploymentService(
      final java.util.function.Function<String, String> aggregateIdNames,
      final Camunda8FetchVariablesResolver fetchVariables,
      final java.util.function.Function<String, List<String>> taskParameters) {

    final var invoker = new Camunda8DeploymentServiceTest.NoOpInvoker() {

      @Override
      public String resolveWorkflowAggregateIdName(
          final String workflowModuleId,
          final String bpmnProcessId) {

        final var name = aggregateIdNames.apply(bpmnProcessId);
        if (name == null) {
          throw new IllegalStateException("no workflow service serves '%s'".formatted(bpmnProcessId));
        }
        return name;

      }

      @Override
      public java.util.Collection<String> taskParameterNames(
          final String workflowModuleId,
          final String bpmnProcessId,
          final String taskDefinitionOrActivityId) {

        return taskParameters.apply(taskDefinitionOrActivityId);

      }

    };
    final var deploymentService = new Camunda8DeploymentService(
        "c8", new Camunda8ClientFactory("c8", new Camunda8AdapterConfiguration()), invoker, (
            m,
            p,
            t) -> Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, java.time.Duration.ofHours(1));
    deploymentService.setFetchVariablesResolver(fetchVariables);
    return deploymentService;

  }

  /**
   * Runs the deployment pipeline up to <code>wireBpmn</code>, which is what fills the
   * multi-instance registry the derivation reads.
   */
  private void wire(
      final Camunda8DeploymentService deploymentService,
      final String xml) {

    final var models = deploymentService
        .readBpmn(MODULE, "test.bpmn", new ByteArrayInputStream(xml.getBytes(UTF_8)), true);
    io.vanillabp.camunda8.Camunda8ProcessingContext context = null;
    for (final var model : models) {
      context = deploymentService.prepareBpmn(MODULE, context, "test.bpmn", model.getKey(), model.getValue());
    }
    for (final var model : models) {
      deploymentService.wireBpmn(MODULE, "test.bpmn", model.getKey(), model.getValue(), context);
    }

  }

  @Test
  @DisplayName("one worker serving two processes fetches BOTH aggregate-ID variables")
  public void theUnionCoversEveryProcessTheWorkerServes() {

    final var deploymentService = deploymentService(
        bpmnProcessId -> "Loans".equals(bpmnProcessId)
            ? "loanId"
            : "cardId",
        null);
    wire(deploymentService, TWO_PROCESSES);

    final var selection = deploymentService.fetchVariablesOf(
        MODULE,
        List
            .of(
                new Camunda8DeploymentService.ServedElement("Loans", "ApproveLoan", "approve"),
                new Camunda8DeploymentService.ServedElement("Cards", "ApproveCard", "approve")));

    assertFalse(selection.all(), "the derivation covers both processes, so nothing has to be fetched blindly");
    assertEquals(
        List.of("cardId", "loanId"),
        selection.names(),
        "fetchVariables is a list, so two processes disagreeing about the name is no conflict - and the "
            + "order is sorted, because the gateway compares the list of two job streams");

  }

  @Test
  @DisplayName("the list is sorted, so it is the same after a restart")
  public void theListIsStableAcrossRestarts() {

    final var deploymentService = deploymentService(
        bpmnProcessId -> "Loans".equals(bpmnProcessId)
            ? "loanId"
            : "cardId",
        null);
    wire(deploymentService, TWO_PROCESSES);

    final var served = List
        .of(
            new Camunda8DeploymentService.ServedElement("Cards", "ApproveCard", "approve"),
            new Camunda8DeploymentService.ServedElement("Loans", "ApproveLoan", "approve"));

    assertEquals(
        List.of("cardId", "loanId"),
        deploymentService.fetchVariablesOf(MODULE, served).names(),
        "the other order of the same processes produces the same list - job streams stay equivalent");

  }

  @Test
  @DisplayName("a task in nested iterations fetches the multi-instance variables of all of them")
  public void theMultiInstanceContextIsPartOfTheList() {

    final var deploymentService = deploymentService(bpmnProcessId -> "id", null);
    wire(deploymentService, NESTED_MULTI_INSTANCE);

    final var selection = deploymentService.fetchVariablesOf(
        MODULE,
        List.of(new Camunda8DeploymentService.ServedElement("MiProcess", "Inner", "inner")));

    assertEquals(
        List
            .of(
                "id",
                "vanillabpMiElement_Inner",
                "vanillabpMiElement_Outer",
                "vanillabpMiIndex_Inner",
                "vanillabpMiIndex_Outer",
                "vanillabpMiTotal_Inner",
                "vanillabpMiTotal_Outer"),
        selection.names(),
        "index, total and element of every iteration enclosing the task - without them the core "
            + "cannot report the iteration the job belongs to");

  }

  @Test
  @DisplayName("an element without iterations contributes the aggregate-ID variable alone")
  public void aPlainTaskFetchesOneVariable() {

    final var deploymentService = deploymentService(bpmnProcessId -> "id", null);
    wire(deploymentService, TWO_PROCESSES);

    assertEquals(
        List.of("id"),
        deploymentService
            .fetchVariablesOf(
                MODULE,
                List.of(new Camunda8DeploymentService.ServedElement("Loans", "ApproveLoan", "approve")))
            .names());

  }

  @Test
  @DisplayName("'all' configured for ONE task of a worker makes that worker fetch everything")
  public void theEscapeHatchWinsForTheWholeWorker() {

    final var deploymentService = deploymentService(
        bpmnProcessId -> "id",
        (
            workflowModuleId,
            bpmnProcessId,
            taskDefinition) -> "Cards".equals(bpmnProcessId)
                ? Camunda8FetchVariables.Mode.ALL
                : Camunda8FetchVariables.Mode.DERIVED);
    wire(deploymentService, TWO_PROCESSES);

    final var selection = deploymentService.fetchVariablesOf(
        MODULE,
        List
            .of(
                new Camunda8DeploymentService.ServedElement("Loans", "ApproveLoan", "approve"),
                new Camunda8DeploymentService.ServedElement("Cards", "ApproveCard", "approve")));

    assertTrue(
        selection.all(),
        "one worker serves one job type, and fetching more than derived is never wrong - so the "
            + "escape hatch wins instead of failing the boot");

  }

  @Test
  @DisplayName("a BPMN process no workflow service serves is fetched blindly rather than incompletely")
  public void anUnknownAggregateFallsBackToEverything() {

    final var deploymentService = deploymentService(bpmnProcessId -> null, null);
    wire(deploymentService, TWO_PROCESSES);

    assertTrue(
        deploymentService
            .fetchVariablesOf(
                MODULE,
                List.of(new Camunda8DeploymentService.ServedElement("Loans", "ApproveLoan", "approve")))
            .all(),
        "a list missing exactly the name the handler needs would be worse than the old behaviour");

  }

  @Test
  @DisplayName("the @TaskParam names of the served task are fetched, whatever the model declares")
  public void theDeclaredTaskParametersAreFetched() {

    final var deploymentService = deploymentService(
        bpmnProcessId -> "id",
        null,
        taskDefinition -> "rate".equals(taskDefinition)
            ? List.of("ratingProvider")
            : List.of());
    wire(deploymentService, DECLARING_MODEL);

    final var selection = deploymentService.fetchVariablesOf(
        MODULE,
        List.of(new Camunda8DeploymentService.ServedElement("Declaring", "Rate", "rate")));

    assertEquals(
        List.of("id", "ratingProvider"),
        selection.names(),
        "the handler reads one of the values this model computes, and the other three are the "
            + "model's own business - reading them off the model would fetch all four");

  }

  @Test
  @DisplayName("a worker serving two tasks fetches the union of their parameters")
  public void theUnionCoversTheParametersOfEveryTaskTheWorkerServes() {

    final var deploymentService = deploymentService(
        bpmnProcessId -> "id",
        null,
        taskDefinition -> "approve".equals(taskDefinition)
            ? List.of("region", "amount")
            : List.of());
    wire(deploymentService, TWO_PROCESSES);

    assertEquals(
        List.of("amount", "id", "region"),
        deploymentService
            .fetchVariablesOf(
                MODULE,
                List
                    .of(
                        new Camunda8DeploymentService.ServedElement("Loans", "ApproveLoan", "approve"),
                        new Camunda8DeploymentService.ServedElement("Cards", "ApproveCard", "approve")))
            .names(),
        "one worker serves one job type across processes, so its list has to satisfy every "
            + "method behind it");

  }

  @Test
  @DisplayName("a @TaskParam naming a variable no model mentions is fetched all the same")
  public void aParameterOutsideTheModelIsFetched() {

    final var deploymentService = deploymentService(
        bpmnProcessId -> "id",
        null,
        taskDefinition -> List.of("bigPayload"));
    wire(deploymentService, TWO_PROCESSES);

    assertEquals(
        List.of("bigPayload", "id"),
        deploymentService
            .fetchVariablesOf(
                MODULE,
                List.of(new Camunda8DeploymentService.ServedElement("Loans", "ApproveLoan", "approve")))
            .names(),
        "the name comes from the method, so a value written past the model reaches the handler "
            + "without the escape hatch");

  }

  @Test
  @DisplayName("the workflow-end worker fetches the aggregate id and nothing the model declares")
  public void theWorkflowEndWorkerStaysAtOneVariable() {

    final var deploymentService = deploymentService(bpmnProcessId -> "id", null);
    wire(deploymentService, DECLARING_MODEL);

    assertEquals(
        List.of("id"),
        deploymentService
            .fetchVariablesOf(
                MODULE,
                List.of(new Camunda8DeploymentService.ServedElement("Declaring", null, null)))
            .names(),
        "a @WorkflowEnded method cannot declare a @TaskParam, so there is nothing else to fetch");

  }

  @Test
  @DisplayName("the guiding messages name the escape hatch and its property key")
  public void theMessagesNameTheWayOut() {

    final var selection = Camunda8FetchVariables.Selection.of(List.of("id"));

    final var missing = Camunda8FetchVariables
        .missingAggregateId("Job", 4711L, "approve", "Loans", "loanId", "c8", selection);
    assertTrue(missing.contains("vanillabp.adapters.c8.fetch-variables"), missing);
    assertTrue(missing.contains("[id]"), "the message names what the worker DID fetch, but was: "
        + missing);

    final var unfetched = Camunda8FetchVariables.unfetchedTaskParameter("bigPayload", "approve", "c8", selection);
    assertTrue(unfetched.contains("vanillabp.adapters.c8.fetch-variables"), unfetched);
    assertTrue(unfetched.contains("bigPayload"), unfetched);
    assertTrue(
        unfetched.contains("@TaskParam(\"bigPayload\")"),
        "since the worker asks for every declared name, reaching this message means the name is "
            + "not on the method - and the message says where to put it, but was: "
            + unfetched);

  }

  @Test
  @DisplayName("a selection asking for everything covers every name")
  public void everythingCoversEveryName() {

    assertTrue(Camunda8FetchVariables.Selection.everything().covers("whatever"));
    assertTrue(Camunda8FetchVariables.Selection.of(List.of("id")).covers("id"));
    assertFalse(Camunda8FetchVariables.Selection.of(List.of("id")).covers("bigPayload"));
    assertEquals(
        "all variables of the process instance",
        Camunda8FetchVariables.Selection.everything().describe());

  }

  @Test
  @DisplayName("a derived list reaches the worker builder, and 'all' leaves the builder alone")
  public void theListReachesTheWorkerBuilder() {

    final var deploymentService = deploymentService(bpmnProcessId -> "id", null);
    final var builder = org.mockito.Mockito
        .mock(io.camunda.client.api.worker.JobWorkerBuilderStep1.JobWorkerBuilderStep3.class);
    org.mockito.Mockito
        .when(builder.fetchVariables(org.mockito.ArgumentMatchers.anyList()))
        .thenReturn(builder);

    deploymentService.applyFetchVariables(
        builder,
        MODULE,
        "task",
        "approve",
        Camunda8FetchVariables.Selection.of(List.of("id")));
    org.mockito.Mockito.verify(builder).fetchVariables(List.of("id"));

    deploymentService.applyFetchVariables(
        builder,
        MODULE,
        "start-event",
        "start",
        Camunda8FetchVariables.Selection.everything());
    // a worker naming no list is what a Camunda 8 worker does by default, so 'all' has
    // nothing to say to the builder
    org.mockito.Mockito.verifyNoMoreInteractions(builder);

  }

  @Test
  @DisplayName("every worker says at DEBUG what it fetches - the first question a missing variable raises")
  public void theStartupLineNamesTheList() {

    final var deploymentService = deploymentService(bpmnProcessId -> "id", null);
    final var builder = org.mockito.Mockito
        .mock(io.camunda.client.api.worker.JobWorkerBuilderStep1.JobWorkerBuilderStep3.class);
    org.mockito.Mockito
        .when(builder.fetchVariables(org.mockito.ArgumentMatchers.anyList()))
        .thenReturn(builder);
    final var logWatcher = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    logWatcher.start();
    final var adapterLog = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
        .getLogger(Camunda8DeploymentService.class);
    final var previousLevel = adapterLog.getLevel();
    adapterLog.setLevel(ch.qos.logback.classic.Level.DEBUG);
    adapterLog.addAppender(logWatcher);
    try {
      deploymentService.applyFetchVariables(
          builder,
          MODULE,
          "task",
          "approve",
          Camunda8FetchVariables.Selection.of(List.of("id")));
    } finally {
      adapterLog.detachAppender(logWatcher);
      adapterLog.setLevel(previousLevel);
    }

    assertTrue(
        logWatcher.list
            .stream()
            .filter(event -> event.getLevel() == ch.qos.logback.classic.Level.DEBUG)
            .anyMatch(event -> event.getFormattedMessage().contains("the task worker 'approve'") && event
                .getFormattedMessage().contains("[id]")),
        () -> "expected one line per worker naming its list, but saw: "
            + logWatcher.list);

  }

  @Test
  @DisplayName("without a resolver the default is the derived list")
  public void theDefaultIsDerived() {

    assertEquals(
        Camunda8FetchVariables.Mode.DERIVED,
        Camunda8FetchVariablesResolver.resolve(null, MODULE, "Loans", "approve"));
    assertEquals(
        Camunda8FetchVariables.Mode.DERIVED,
        Camunda8FetchVariablesResolver.resolve((
            m,
            p,
            t) -> null, MODULE, "Loans", "approve"),
        "a resolver answering nothing is a level configuring nothing");

  }

}
