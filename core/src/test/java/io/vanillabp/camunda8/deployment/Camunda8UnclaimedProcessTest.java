package io.vanillabp.camunda8.deployment;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.Message;
import io.camunda.zeebe.model.bpmn.instance.Process;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeSubscription;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.TestCollaborators;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver;
import io.vanillabp.camunda8.wiring.Camunda8TaskWiring;
import io.vanillabp.integration.adapter.spi.workflowend.WorkflowEndedInvoker;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * A BPMN file carrying a process no <code>@WorkflowService</code> class of this
 * application claims. Such a process is deployed, because a file travels to the cluster as
 * a whole, and the core reports it instead of validating it - so everything the adapter
 * wires has to cope with a process it can learn no workflow aggregate for.
 * <p>
 * Two things in <code>wireBpmn</code> need that aggregate's ID variable, and both used to
 * ask for it without a net: the correlation key injected into a message subscription and
 * the execution listener reporting the end of a workflow. Either one turned an unclaimed
 * process into a failed boot, with a message asking for a class the application may
 * deliberately not have - and the boot ended before the report naming the process could be
 * written, so the developer saw the wrong half of the answer and nothing of the right one.
 * <p>
 * What such a subscription gets instead is a constant, not nothing: the cluster refuses a
 * message catch element whose message carries no subscription and rejects the whole file
 * over it, so leaving the message alone would end the boot from the other side and take
 * the claimed process down with it. That the constant deploys and that a workflow waits at
 * it without an incident was measured against Camunda 8.9.16, and
 * {@code Camunda8RenamedProcessIT} of the Spring Boot module sends such a file to a
 * cluster on every run, so the day the cluster changes its mind about it a build says so.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8UnclaimedProcessTest {

  private static final String MODULE = "test-module";

  /**
   * Two executable processes, one of them claimed by a workflow service and one not. Both
   * wait for a message of their own, neither of them models a correlation key.
   */
  private static final String CLAIMED_AND_UNCLAIMED = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:message id="Msg_LoanApproved" name="LoanApproved" />
        <bpmn:message id="Msg_CardApproved" name="CardApproved" />
        <bpmn:process id="Loans" isExecutable="true">
          <bpmn:serviceTask id="ApproveLoan">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="approveLoan" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:intermediateCatchEvent id="AwaitLoanApproval">
            <bpmn:messageEventDefinition id="LoanApprovedDef" messageRef="Msg_LoanApproved" />
          </bpmn:intermediateCatchEvent>
        </bpmn:process>
        <bpmn:process id="Cards" isExecutable="true">
          <bpmn:serviceTask id="ApproveCard">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="approveCard" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:intermediateCatchEvent id="AwaitCardApproval">
            <bpmn:messageEventDefinition id="CardApprovedDef" messageRef="Msg_CardApproved" />
          </bpmn:intermediateCatchEvent>
        </bpmn:process>
      </bpmn:definitions>
      """;

  /**
   * An unclaimed process which carries no task at all, only the message it waits for -
   * the only shape which could reach the injection before the core started collecting
   * unclaimed processes instead of refusing them, and therefore the shape which must be
   * covered by the same guard.
   */
  private static final String UNCLAIMED_WITHOUT_ANY_TASK = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:message id="Msg_CardApproved" name="CardApproved" />
        <bpmn:process id="Cards" isExecutable="true">
          <bpmn:startEvent id="CardStart" />
          <bpmn:intermediateCatchEvent id="AwaitCardApproval">
            <bpmn:messageEventDefinition id="CardApprovedDef" messageRef="Msg_CardApproved" />
          </bpmn:intermediateCatchEvent>
          <bpmn:endEvent id="CardEnd" />
        </bpmn:process>
      </bpmn:definitions>
      """;

  /**
   * ONE message element shared by two processes, the unclaimed one written FIRST. A
   * message belongs to the file rather than to a process, so both catch events point at
   * the same element and the order they are wired in must not decide what the claimed
   * process correlates by.
   */
  private static final String A_SHARED_MESSAGE = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:message id="Msg_Shared" name="SharedApproval" />
        <bpmn:process id="Cards" isExecutable="true">
          <bpmn:intermediateCatchEvent id="AwaitOnCards">
            <bpmn:messageEventDefinition id="CardsDef" messageRef="Msg_Shared" />
          </bpmn:intermediateCatchEvent>
        </bpmn:process>
        <bpmn:process id="Loans" isExecutable="true">
          <bpmn:intermediateCatchEvent id="AwaitOnLoans">
            <bpmn:messageEventDefinition id="LoansDef" messageRef="Msg_Shared" />
          </bpmn:intermediateCatchEvent>
        </bpmn:process>
      </bpmn:definitions>
      """;

  /**
   * An unclaimed process whose message carries a correlation key its modeller wrote - the
   * case where nothing has to be injected and therefore nothing is missing.
   */
  private static final String UNCLAIMED_WITH_A_MODELLED_KEY = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:message id="Msg_CardApproved" name="CardApproved">
          <bpmn:extensionElements>
            <zeebe:subscription correlationKey="=applicationNumber" />
          </bpmn:extensionElements>
        </bpmn:message>
        <bpmn:process id="Cards" isExecutable="true">
          <bpmn:intermediateCatchEvent id="AwaitCardApproval">
            <bpmn:messageEventDefinition id="CardApprovedDef" messageRef="Msg_CardApproved" />
          </bpmn:intermediateCatchEvent>
        </bpmn:process>
      </bpmn:definitions>
      """;

  /**
   * A deployment service whose core answers the given aggregate-ID variable per BPMN
   * process and throws for every process the function answers <code>null</code> for -
   * which is what the core does for a process no workflow service claims.
   */
  private Camunda8DeploymentService deploymentService(
      final Function<String, String> aggregateIdNames) {

    return deploymentService(aggregateIdNames, mock(WorkflowEndedInvoker.class));

  }

  /**
   * The same, with the core's answer about whether the end of a workflow of a process has
   * to be reported at all.
   */
  private Camunda8DeploymentService deploymentService(
      final Function<String, String> aggregateIdNames,
      final WorkflowEndedInvoker workflowEndedInvoker) {

    final var invoker = new Camunda8DeploymentServiceTest.NoOpInvoker() {

      @Override
      public String resolveWorkflowAggregateIdName(
          final String workflowModuleId,
          final String bpmnProcessId) {

        final var name = aggregateIdNames.apply(bpmnProcessId);
        if (name == null) {
          // word for word what the core answers for a process nothing claims
          throw new IllegalStateException(
              ("No @WorkflowService class is registered for BPMN process '%s' of workflow module "
                  + "'%s' - the aggregate-ID variable name cannot be determined!")
                  .formatted(bpmnProcessId, workflowModuleId));
        }
        return name;

      }

      @Override
      public Collection<String> taskParameterNames(
          final String workflowModuleId,
          final String bpmnProcessId,
          final String taskDefinitionOrActivityId) {

        return List.of();

      }

    };
    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setRestAddress("http://localhost:65535");
    return new Camunda8DeploymentService(
        "c8", new Camunda8ClientFactory("c8", configuration), TestCollaborators
            .of(invoker, workflowEndedInvoker), (
                m,
                p,
                t) -> Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, Duration
                    .ofHours(1), adapterId -> configuration);

  }

  /**
   * Runs the deployment pipeline up to <code>wireBpmn</code> - the stage which rewrites
   * the model - and hands back the models and the context it filled.
   */
  private Wired wire(
      final Camunda8DeploymentService deploymentService,
      final String xml) {

    final var models = deploymentService
        .readBpmn(MODULE, "cards.bpmn", new ByteArrayInputStream(xml.getBytes(UTF_8)), true);
    Camunda8ProcessingContext context = null;
    for (final var model : models) {
      context = deploymentService.prepareBpmn(MODULE, context, "cards.bpmn", model.getKey(), model.getValue());
    }
    for (final var model : models) {
      deploymentService.wireBpmn(MODULE, "cards.bpmn", model.getKey(), model.getValue(), context);
    }
    return new Wired(models.getFirst().getValue(), context);

  }

  /**
   * @param model The model as <code>wireBpmn</code> left it
   * @param context What the pipeline carries on to <code>startWorkflowProcessing</code>
   */
  private record Wired(
                       BpmnModelInstance model,
                       Camunda8ProcessingContext context) {
  }

  /**
   * The correlation-key expression of the named message, or <code>null</code> where the
   * message carries no <code>zeebe:subscription</code> at all.
   */
  private String correlationKeyOf(
      final BpmnModelInstance model,
      final String messageName) {

    final var subscription = messageNamed(model, messageName)
        .getSingleExtensionElement(ZeebeSubscription.class);
    return subscription == null
        ? null
        : subscription.getCorrelationKey();

  }

  /**
   * The message element of the given name, which is one per FILE rather than one per
   * process.
   */
  private Message messageNamed(
      final BpmnModelInstance model,
      final String messageName) {

    return model
        .getModelElementsByType(Message.class)
        .stream()
        .filter(candidate -> messageName.equals(candidate.getName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("the model carries no message '%s'".formatted(messageName)));

  }

  /**
   * How many <code>zeebe:subscription</code> elements the named message carries.
   */
  private long subscriptionsOf(
      final BpmnModelInstance model,
      final String messageName) {

    final var extensionElements = messageNamed(model, messageName).getExtensionElements();
    return extensionElements == null
        ? 0
        : extensionElements
            .getElements()
            .stream()
            .filter(ZeebeSubscription.class::isInstance)
            .count();

  }

  @Test
  @DisplayName("The claimed process correlates by its aggregate, the unclaimed one by a constant")
  public void anUnclaimedProcessCorrelatesByAConstantNothingPublishes() {

    final var wired = wire(
        deploymentService(bpmnProcessId -> "Loans".equals(bpmnProcessId)
            ? "loanId"
            : null),
        CLAIMED_AND_UNCLAIMED);

    assertEquals(
        "=loanId",
        correlationKeyOf(wired.model(), "LoanApproved"),
        "the process this application serves has a workflow aggregate, so its subscription is wired");
    assertEquals(
        Camunda8TaskWiring.CORRELATION_KEY_WITHOUT_A_WORKFLOW_AGGREGATE,
        correlationKeyOf(wired.model(), "CardApproved"),
        "and the one it does not serve correlates by a constant nothing publishes - the file has "
            + "to stay deployable, and there is no aggregate to name");

  }

  @Test
  @DisplayName("An unclaimed process holding no task at all is covered by the same guard")
  public void anUnclaimedProcessWithoutAnyTaskIsCoveredAsWell() {

    final var wired = wire(deploymentService(bpmnProcessId -> null), UNCLAIMED_WITHOUT_ANY_TASK);

    assertEquals(
        Camunda8TaskWiring.CORRELATION_KEY_WITHOUT_A_WORKFLOW_AGGREGATE,
        correlationKeyOf(wired.model(), "CardApproved"),
        "a process without tasks reached the injection before the core collected unclaimed "
            + "processes, so it is the older half of the same defect");

  }

  @Test
  @DisplayName("A correlation key the modeller wrote stays untouched, whoever serves the process")
  public void aModelledCorrelationKeyOfAnUnclaimedProcessSurvives() {

    final var wired = wire(deploymentService(bpmnProcessId -> null), UNCLAIMED_WITH_A_MODELLED_KEY);

    assertEquals(
        "=applicationNumber",
        correlationKeyOf(wired.model(), "CardApproved"),
        "nothing had to be injected here, so the missing workflow aggregate costs nothing");

  }

  @Test
  @DisplayName("One DEBUG line names the messages which got no aggregate to correlate by")
  public void theMessagesWithoutAnAggregateAreNamedAtDebug() {

    final var deploymentService = deploymentService(bpmnProcessId -> "Loans".equals(bpmnProcessId)
        ? "loanId"
        : null);
    final var logWatcher = new ListAppender<ILoggingEvent>();
    logWatcher.start();
    final var adapterLog = (ch.qos.logback.classic.Logger) LoggerFactory
        .getLogger(Camunda8DeploymentService.class);
    final var previousLevel = adapterLog.getLevel();
    adapterLog.setLevel(Level.DEBUG);
    adapterLog.addAppender(logWatcher);
    try {
      wire(deploymentService, CLAIMED_AND_UNCLAIMED);
    } finally {
      adapterLog.detachAppender(logWatcher);
      adapterLog.setLevel(previousLevel);
    }

    final var reported = logWatcher.list
        .stream()
        .filter(event -> event.getLevel() == Level.DEBUG)
        .map(ILoggingEvent::getFormattedMessage)
        .filter(message -> message.contains("CardApproved"))
        .findFirst()
        .orElse(null);
    assertNotNull(
        reported,
        () -> "expected the message which got no aggregate to correlate by to be named, but saw: "
            + logWatcher.list);
    assertTrue(reported.contains("Cards"), () -> "the process it belongs to: "
        + reported);
    assertTrue(reported.contains(MODULE), () -> "the workflow module: "
        + reported);
    assertTrue(reported.contains("cards.bpmn"), () -> "and the file it came from: "
        + reported);
    assertTrue(
        logWatcher.list
            .stream()
            .noneMatch(event -> event.getFormattedMessage().contains("LoanApproved")),
        () -> "the message which got its key is not worth a line: "
            + logWatcher.list);

  }

  @Test
  @DisplayName("A message two processes share correlates by the aggregate of the one which is served")
  public void aSharedMessageEndsUpWithTheRealAggregate() {

    final var wired = wire(
        deploymentService(bpmnProcessId -> "Loans".equals(bpmnProcessId)
            ? "loanId"
            : null),
        A_SHARED_MESSAGE);

    assertEquals(
        "=loanId",
        correlationKeyOf(wired.model(), "SharedApproval"),
        "the unclaimed process is wired first and puts its placeholder there, and the claimed one "
            + "then replaces it - the other way round the workflow this application serves would "
            + "stop correlating");
    assertEquals(
        1,
        subscriptionsOf(wired.model(), "SharedApproval"),
        "and there is one zeebe:subscription, not two - the cluster rejects the whole file over a "
            + "second one");

  }

  @Test
  @DisplayName("An unclaimed process gets no workflow-end listener even where the end is wanted")
  public void anUnclaimedProcessGetsNoWorkflowEndListener() {

    // a workflow module releasing the records of its processed task deliveries when a
    // workflow ends wants the notification for EVERY process of the module, without any
    // application method saying so - which is how an unclaimed process gets here
    final var workflowEndedInvoker = mock(WorkflowEndedInvoker.class);
    when(workflowEndedInvoker.workflowEndedHandlerExists(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
        .thenReturn(true);
    final var deploymentService = deploymentService(bpmnProcessId -> null, workflowEndedInvoker);

    final var wired = wire(deploymentService, UNCLAIMED_WITHOUT_ANY_TASK);

    final var process = wired.model()
        .getModelElementsByType(Process.class)
        .stream()
        .filter(candidate -> "Cards".equals(candidate.getId()))
        .findFirst()
        .orElseThrow();
    assertNull(
        process.getSingleExtensionElement(ZeebeExecutionListeners.class),
        "an 'end' listener nobody can activate would stop the workflow at its own end");
    assertTrue(
        wired.context().getWorkflowEndedProcessesToWire().isEmpty(),
        "so the process is kept out of the list the workers are opened from");

    deploymentService.startWorkflowProcessing(MODULE, wired.context());
    try {
      assertTrue(
          wired.context().getOpenWorkers().isEmpty(),
          "and no workflow-end worker is opened for it");
    } finally {
      deploymentService.stopWorkflowProcessing(MODULE, wired.context());
    }

  }

}
