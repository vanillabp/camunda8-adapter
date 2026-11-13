package io.vanillabp.camunda8.deployment;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.vanillabp.camunda8.TestCollaborators;
import io.vanillabp.camunda8.TestScoping;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver;
import io.vanillabp.integration.adapter.spi.NameClashAvoidance;
import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What a boot does about a user task a job worker serves, which is a user task without a
 * {@code zeebe:userTask} extension element.
 *
 * <p>
 * The shape is the finding. This adapter serves a user task only where the cluster manages it,
 * so nothing here asks whether some worker would fetch the job the cluster hands out.
 * <p>
 * Who claims the process decides the answer. A process a {@code @WorkflowService} class of the
 * application claims is a promise that the application serves it, so the boot ends over such an
 * element. A process nobody claims travels to the cluster because of the file it sits in, and
 * there is nothing to ask of a model somebody else owns, so it is not looked at and the boot
 * goes on (decision 73 in the repository's DECISIONS.md).
 * <p>
 * A user task with a {@code zeebe:taskDefinition} of its own has the same shape, and it is
 * refused the same way. The cluster ignores a job type on a user task and hands out its own
 * user-task job all the same ({@code Camunda8LegacyUserTaskIT} measures it). So the message
 * says that the job type changes nothing, whether or not a method names it.
 * <p>
 * Two shapes reach the message and it has to keep them apart. One carries the
 * {@code formKey} VanillaBP 1 read the task definition from up to its release 1.6.3, and an
 * upgrading application finds those by searching its models for that word. The other carries
 * no {@code formKey} at all, so no such search finds it, while the cluster does exactly the
 * same with it: it creates a job nobody fetches, and the workflow stands at the element with
 * no incident, because a job nobody fetches never loses a retry.
 * <p>
 * The cluster is an address nothing listens on, so the count of open tasks is the one thing
 * these runs cannot have. That the message says so instead of a number is asserted as well.
 * So is that no boot waits for the cluster before the count: the boot which ends does not wait,
 * and a process nobody claims is not counted at all.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
public class Camunda8JobWorkerUserTasksReportTest {

  private static final String MODULE = "loan-approval";

  private static final String PROCESS = "LoanApproval";

  private static final String FILE = "loan-approval.bpmn";

  /**
   * The sentence which tells this finding apart from everything else a boot may say.
   */
  private static final String THE_FINDING = "no 'zeebe:userTask'. Such a user task is served by a job worker";

  /**
   * The reason both messages give: the shape is not one this adapter takes.
   */
  private static final String THE_REASON = "this adapter does not serve that shape";

  /**
   * What is said about the shape a search of the models finds.
   */
  private static final String THE_SHAPE_A_SEARCH_FINDS = "a search of your models for 'formKey' finds such elements";

  /**
   * The way out which gets the process running, named by the refusal only.
   */
  private static final String THE_WAY_OUT_THROUGH_A_METHOD = "choose \"Camunda user task\" as its implementation (this writes '<zeebe:userTask />') and set 'External form reference'";

  /**
   * The other way out, named by the refusal only.
   */
  private static final String THE_WAY_OUT_THROUGH_THE_MODEL = "If that work is not done any more, take the element out of the model.";

  /**
   * That older versions in the cluster are not refused for the same shape.
   */
  private static final String OLDER_VERSIONS_MAY_KEEP_IT = "Older versions of this process which the cluster already holds may keep this shape; they are not refused.";

  /**
   * What the refusal says about a user task which names a job type of its own.
   */
  private static final String THE_JOB_TYPE_CHANGES_NOTHING = "does not change that: the cluster ignores a job type on a user task and hands out a job of type 'io.camunda.zeebe:userTask'";

  private static BpmnModelInstance model(
      final String userTaskContent) {

    final var xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
          <bpmn:process id="%s" isExecutable="true">
        %s
          </bpmn:process>
        </bpmn:definitions>
        """
        .formatted(PROCESS, userTaskContent);
    return Bpmn.readModelFromStream(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

  }

  /**
   * The shape VanillaBP 1 served up to its release 1.6.3.
   */
  private static BpmnModelInstance aUserTaskCarryingAFormKey() {

    return model("""
            <bpmn:userTask id="Activity_SignTheContract">
              <bpmn:extensionElements>
                <zeebe:formDefinition formKey="camunda-forms:bpmn:signTheContract" />
              </bpmn:extensionElements>
            </bpmn:userTask>
        """);

  }

  /**
   * A user task somebody modelled in a modeller and left as it was. It was never a VanillaBP
   * shape.
   */
  private static BpmnModelInstance aUserTaskCarryingNothing() {

    return model("""
            <bpmn:userTask id="Activity_ApproveTheLoan" name="approve the loan" />
        """);

  }

  /**
   * The shape with a job type of its own, which older Camunda Modelers often wrote. It looks
   * like an element a worker of the application serves. The cluster ignores the job type.
   */
  private static BpmnModelInstance aUserTaskWithAJobTypeOfItsOwn() {

    return model("""
            <bpmn:userTask id="Activity_ApproveTheLoan">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="approveTheLoan" />
              </bpmn:extensionElements>
            </bpmn:userTask>
        """);

  }

  /**
   * The shape this version serves.
   */
  private static BpmnModelInstance aCamundaManagedUserTask() {

    return model("""
            <bpmn:userTask id="Activity_SignTheContract">
              <bpmn:extensionElements>
                <zeebe:userTask />
                <zeebe:formDefinition externalReference="signTheContract" />
              </bpmn:extensionElements>
            </bpmn:userTask>
        """);

  }

  /**
   * A core which answers that no {@code @WorkflowService} class of the application claims the
   * process. The core gives that answer by not knowing a workflow aggregate for it.
   */
  private static class NoWorkflowServiceClaimsIt extends Camunda8DeploymentServiceTest.NoOpInvoker {

    @Override
    public String resolveWorkflowAggregateIdName(
        final String workflowModuleId,
        final String bpmnProcessId) {
      throw new IllegalStateException(
          "no @WorkflowService class of this application claims '%s'".formatted(bpmnProcessId));
    }

  }

  /**
   * A core which knows a @WorkflowService class for the process, but no @WorkflowTask method
   * for anything in it. That is the application which serves a user task with a worker of
   * its own.
   */
  private static class NoMethodServesIt extends Camunda8DeploymentServiceTest.NoOpInvoker {

    @Override
    public boolean workflowTaskHandlerExists(
        final String workflowModuleId,
        final String bpmnProcessId,
        final String taskDefinitionOrActivityId) {
      return false;
    }

  }

  /**
   * A client factory which counts the waits for its cluster instead of waiting. The cluster
   * of these runs is an address nothing listens on, so a real wait would only time out.
   */
  private static class ClientFactoryCountingTheWaits extends Camunda8ClientFactory {

    private final AtomicInteger waits = new AtomicInteger();

    ClientFactoryCountingTheWaits(
        final Camunda8AdapterConfiguration configuration) {

      super("c8", configuration);

    }

    @Override
    public void waitUntilTheClusterAnswers() {

      waits.incrementAndGet();

    }

  }

  /**
   * The factory of the last wiring, which tells how often that wiring waited for the cluster.
   */
  private static ClientFactoryCountingTheWaits lastFactory;

  /**
   * Wires one model and hands the wiring back unrun, so the test can read what it said or
   * catch what it refused.
   *
   * @param model The model of the one BPMN file
   * @param claimed Whether a workflow service of the application claims the process
   * @return Preparing and wiring that file
   */
  private static Executable wiringOf(
      final BpmnModelInstance model,
      final boolean claimed) {

    return wiringOf(model, claimed
        ? new Camunda8DeploymentServiceTest.NoOpInvoker()
        : new NoWorkflowServiceClaimsIt());

  }

  private static Executable wiringOf(
      final BpmnModelInstance model,
      final Camunda8DeploymentServiceTest.NoOpInvoker core) {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setRestAddress("http://localhost:65535");
    final var scoping = TestScoping.of(NameClashAvoidance.BY_ADAPTER);
    lastFactory = new ClientFactoryCountingTheWaits(configuration);
    final var service = DeploymentServiceUnderTest.of(
        "c8", lastFactory, TestCollaborators.of(core, scoping),
        (
            workflowModuleId,
            bpmnProcessId,
            taskDefinition) -> Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT,
        Duration.ofHours(1), adapterId -> configuration, scoping);
    return () -> {
      final var context = service.prepareBpmn(MODULE, null, FILE, PROCESS, model);
      service.wireBpmn(MODULE, FILE, PROCESS, model, context);
    };

  }

  /**
   * Wires one model of a process nobody claims and hands back what this run wrote.
   */
  private static String whatTheBootSaid(
      final CapturedOutput output,
      final BpmnModelInstance model) {

    final var before = output.getAll().length();
    assertDoesNotThrow(wiringOf(model, false));
    return output.getAll().substring(before);

  }

  /**
   * Wires one model of a process the application claims and hands back why the boot ended.
   */
  private static String whyTheBootEnded(
      final BpmnModelInstance model) {

    return assertThrows(IllegalStateException.class, wiringOf(model, true)).getMessage();

  }

  @Test
  @DisplayName("A claimed process whose user task carries version 1's formKey does not deploy")
  public void theFormKeyShapeEndsTheBootOfAClaimedProcess() {

    final var refused = whyTheBootEnded(aUserTaskCarryingAFormKey());

    assertTrue(
        refused.contains(THE_FINDING),
        () -> "nothing else in the boot sees such an element: "
            + refused);
    assertTrue(
        refused.contains("'"
            + PROCESS
            + "'") && refused.contains(
                "'"
                    + MODULE
                    + "'"),
        () -> "the process and its workflow module are what a developer looks for first: "
            + refused);
    assertTrue(
        refused.contains("'Activity_SignTheContract'"),
        () -> "a developer has to read WHICH element it is about: "
            + refused);
    assertTrue(
        refused.contains(THE_SHAPE_A_SEARCH_FINDS),
        () -> "and that a search of the models for 'formKey' finds this one: "
            + refused);
    assertTrue(
        refused.contains("set 'External form reference' to 'camunda-forms:bpmn:signTheContract', the task definition"),
        () -> "version 1 read the task definition from the formKey, so the message names it: "
            + refused);
    assertTrue(
        refused.contains(THE_REASON),
        () -> "the reason is the shape of the element, not what the cluster would do with it: "
            + refused);
    assertTrue(
        refused.contains(THE_WAY_OUT_THROUGH_A_METHOD) && refused.contains(THE_WAY_OUT_THROUGH_THE_MODEL),
        () -> "both ways out are named, so the developer needs no documentation: "
            + refused);
    assertTrue(
        refused.contains(OLDER_VERSIONS_MAY_KEEP_IT),
        () -> "a version the cluster already holds may keep the shape, which the developer has to "
            + "know before re-modelling: "
            + refused);
    assertTrue(
        refused.length() < 1400,
        () -> "the message says what to do and links the rest, it is not a page of documentation: "
            + refused.length());
    assertTrue(
        refused.contains(
            "The cluster did not answer how many jobs of type 'io.camunda.zeebe:userTask' are open right now"),
        () -> "this run has no cluster, and the message says that rather than a number: "
            + refused);
    assertEquals(
        0,
        lastFactory.waits.get(),
        "the boot ends over the model, so it does not wait for the cluster first: a cluster "
            + "which is not up yet would sit out the whole startup wait and then end the boot "
            + "with a message which hides the real cause");

  }

  @Test
  @DisplayName("A claimed process whose user task carries no formKey does not deploy either")
  public void theShapeWithoutAFormKeyEndsTheBootToo() {

    final var refused = whyTheBootEnded(aUserTaskCarryingNothing());

    assertTrue(
        refused.contains(THE_FINDING),
        () -> "this was the silent case: the model deployed, the job appeared, nobody fetched it: "
            + refused);
    assertTrue(
        refused.contains("'Activity_ApproveTheLoan'"),
        () -> "named by its element id like every other finding: "
            + refused);
    assertFalse(
        refused.contains(THE_SHAPE_A_SEARCH_FINDS),
        () -> "the sentence about version 1's convention is not true for this element: "
            + refused);
    assertTrue(
        refused.contains(
            "set 'External form reference' to 'Activity_ApproveTheLoan', the task definition of the @WorkflowTask method which serves it."),
        () -> "a method names the element id, so the message names it as the reference to set: "
            + refused);

  }

  @Test
  @DisplayName("Both shapes of a claimed process are named apart, in the one message which ends the boot")
  public void bothShapesAreNamedApart() {

    final var refused = whyTheBootEnded(model("""
            <bpmn:userTask id="Activity_SignTheContract">
              <bpmn:extensionElements>
                <zeebe:formDefinition formKey="camunda-forms:bpmn:signTheContract" />
              </bpmn:extensionElements>
            </bpmn:userTask>
            <bpmn:userTask id="Activity_ApproveTheLoan" name="approve the loan" />
        """));

    assertTrue(
        refused
            .contains("'Activity_SignTheContract' carries the formKey from which VanillaBP 1 read the task definition"),
        () -> "only the one a search finds is said to be found by it: "
            + refused);
    assertTrue(
        refused.contains("user task 'Activity_SignTheContract', 'Activity_ApproveTheLoan' have no 'zeebe:userTask'"),
        () -> "named together, because the way out is the same for both: "
            + refused);
    assertTrue(
        refused.contains("'camunda-forms:bpmn:signTheContract' for 'Activity_SignTheContract'"),
        () -> "and each one with the task definition to set: "
            + refused);

  }

  @Test
  @DisplayName("A claimed process whose user tasks are all Camunda-managed boots")
  public void aServedUserTaskIsNotWorthAWord(
      final CapturedOutput output) {

    final var before = output.getAll().length();
    assertDoesNotThrow(wiringOf(aCamundaManagedUserTask(), true));
    final var logged = output.getAll().substring(before);

    assertFalse(
        logged.contains(THE_FINDING),
        () -> "VanillaBP wires this one and hears about every task it creates: "
            + logged);

  }

  @Test
  @DisplayName("A process nobody claims is not looked at, and the boot neither waits nor counts")
  public void anUnclaimedProcessIsNotLookedAt(
      final CapturedOutput output) {

    final var logged = whatTheBootSaid(output, aUserTaskCarryingNothing());

    assertFalse(
        logged.contains(THE_FINDING),
        () -> "somebody else's model is not judged: "
            + logged);
    assertEquals(
        0,
        lastFactory.waits.get(),
        "nothing is counted for such a process, so nothing waits for the cluster either");

  }


  @Test
  @DisplayName("A claimed process whose user task carries a job type of its own does not deploy either")
  public void aUserTaskWithAJobTypeOfItsOwnEndsTheBootToo() {

    final var refused = assertThrows(
        IllegalStateException.class,
        wiringOf(aUserTaskWithAJobTypeOfItsOwn(), new NoMethodServesIt())).getMessage();

    assertTrue(
        refused.contains("user task 'Activity_ApproveTheLoan' has "
            + THE_FINDING),
        () -> "the cluster serves it with its own user-task job, so it is a job-worker user task "
            + "like every other one, even without a method for its job type: "
            + refused);
    assertTrue(
        refused.contains("The job type in 'zeebe:taskDefinition' ('approveTheLoan' for 'Activity_ApproveTheLoan') "
            + THE_JOB_TYPE_CHANGES_NOTHING),
        () -> "the developer reads a job type in the model, so the message says why it does not help: "
            + refused);
    assertTrue(
        refused.contains("If a job is really what you want, model the element as a service task."),
        () -> "and names the element which does take a job type: "
            + refused);
    assertTrue(
        refused.contains(
            "set 'External form reference' to the task definition of the @WorkflowTask method which serves it."),
        () -> "no method names the job type or the element id, so no task definition is guessed: "
            + refused);

  }

  @Test
  @DisplayName("A method naming the job type of such a user task is named as the reference to set")
  public void aMethodForAUserTaskWithAJobTypeOfItsOwnIsNamedInTheRefusal() {

    final var refused = whyTheBootEnded(aUserTaskWithAJobTypeOfItsOwn());

    assertTrue(
        refused.contains(THE_FINDING) && refused.contains(THE_JOB_TYPE_CHANGES_NOTHING),
        () -> "the same refusal as without a method: "
            + refused);
    assertTrue(
        refused.contains("set 'External form reference' to 'approveTheLoan', the task definition of the "
            + "@WorkflowTask method which serves it."),
        () -> "a method names the job type, so that is the reference to set: "
            + refused);
    assertFalse(
        refused.contains("matching no task"),
        () -> "the task IS in the model, so the message does not ask to add it: "
            + refused);

  }

  @Test
  @DisplayName("A user task without a job type of its own gets no sentence about one")
  public void theSentenceAboutAJobTypeIsOnlyWrittenWhereOneIsNamed() {

    final var refused = whyTheBootEnded(aUserTaskCarryingNothing());

    assertFalse(
        refused.contains(THE_JOB_TYPE_CHANGES_NOTHING),
        () -> "the element names no job type: "
            + refused);

  }

  @Test
  @DisplayName("A process nobody claims is not looked at for a job type of its own either")
  public void anUnclaimedProcessIsNotLookedAtForAJobTypeOfItsOwn(
      final CapturedOutput output) {

    final var logged = whatTheBootSaid(output, aUserTaskWithAJobTypeOfItsOwn());

    assertFalse(
        logged.contains(THE_FINDING) || logged.contains(THE_JOB_TYPE_CHANGES_NOTHING),
        () -> "somebody else's model is not judged: "
            + logged);

  }

  @Test
  @DisplayName("A process nobody claims is not looked at for version 1's shape either")
  public void anUnclaimedProcessIsNotLookedAtForTheFormKeyShape(
      final CapturedOutput output) {

    final var logged = whatTheBootSaid(output, aUserTaskCarryingAFormKey());

    assertFalse(
        logged.contains(THE_FINDING),
        () -> "somebody else's model is not judged: "
            + logged);

  }


}
