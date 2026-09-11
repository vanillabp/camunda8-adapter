package io.vanillabp.camunda8.springboot.refusedstart;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.camunda.client.CamundaClient;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.client.Camunda8Errors;
import io.vanillabp.camunda8.processservice.Camunda8ProcessService;
import io.vanillabp.camunda8.springboot.it.ClusterUnderTest;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What a Camunda 8 cluster does with a start it will not carry out, measured against a
 * real cluster.
 * <p>
 * The phase-two outbox repeats a start until the entry is blocked, and a refusal which
 * reads the same on every attempt turns that repetition into a workflow lost slowly: the
 * application's transaction is committed, the aggregate is there, and the workflow never
 * comes into being. The adapter's answer is the classification in
 * {@code Camunda8Errors}, which names the codes a repetition cannot change. This class
 * asks the cluster which of its refusals carry such a code.
 * <p>
 * The cases below are the answers this cluster gave. A request it will not take is
 * refused with HTTP 400, the entry is blocked after ONE attempt and an operator has a
 * single ERROR to read. A model without a none start event is refused with HTTP 409,
 * which the adapter repeats, so that start does cost the full row of attempts. And a
 * model which cannot evaluate an expression is not refused at all: the instance exists
 * and carries an incident, which the cluster reports and no outbox entry waits for.
 * <p>
 * One thing stays unpinned here. An expression which merely reads a variable nobody
 * passed evaluates to null on Camunda 8, the instance runs through it and nothing fails
 * at all, which is why the third case demands a value such an expression cannot produce.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = RefusedStartTestApplication.class,
    properties = "spring.config.name=camunda8-refused-start-it")
// closed when the class is done: every IT here has a context of its own (its own
// container), Spring would keep them all until the JVM exits, and a context outliving
// its cluster keeps its job workers polling an address nobody answers
@DirtiesContext
public class Camunda8RefusedStartIT {

  @Container
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.cluster();

  @DynamicPropertySource
  static void camunda8Properties(
      final DynamicPropertyRegistry registry) {

    registry
        .add(
            "vanillabp.adapters.c8.rest-address",
            () -> "http://"
                + CAMUNDA.getHost()
                + ":"
                + CAMUNDA.getMappedPort(8080));
    registry
        .add(
            "vanillabp.adapters.c8.grpc-address",
            () -> "http://"
                + CAMUNDA.getHost()
                + ":"
                + CAMUNDA.getMappedPort(26500));

  }

  /**
   * Above the four megabytes a cluster takes per request by default. What the adapter
   * sends is the aggregate, so an aggregate of this size is a request no cluster of that
   * configuration ever accepts.
   */
  private static final int TOO_BIG_FOR_THE_CLUSTER = 5 * 1024 * 1024;

  private static final String IS_THE_ENTRY_BLOCKED = "select blocked from TXNO_OUTBOX";

  private static final String ATTEMPTS_OF_THE_ENTRY = "select attempts from TXNO_OUTBOX";

  @Autowired
  private RefusedStartWorkflowService workflowService;

  @Autowired
  private RefusedStartAggregateRepository repository;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private Camunda8ClientFactoryRegistry clientFactoryRegistry;

  @Autowired
  private Camunda8ProcessService<RefusedStartAggregate> camunda8ProcessService;

  private CamundaClient client() {

    return clientFactoryRegistry
        .getFactory("c8")
        .getClient();

  }

  @Test
  @DisplayName("A request the cluster will not take blocks the outbox entry after one attempt")
  public void aRequestTheClusterWillNotTakeBlocksTheEntryAfterOneAttempt() throws Exception {

    // an application keeps a document in its aggregate, and every attribute of an
    // aggregate travels to the cluster as a process variable. This one is heavier than
    // the request a cluster accepts, which no repetition changes
    final var aggregate = transactionTemplate
        .execute(status -> workflowService.startWorkflow("x".repeat(TOO_BIG_FOR_THE_CLUSTER)));

    // the application is finished at this point: its transaction is committed and the
    // aggregate is there, while the workflow exists nowhere
    assertNotNull(aggregate.getId(), "the start returned a persisted aggregate");
    assertTrue(repository.findById(aggregate.getId()).isPresent(), "which is committed");

    awaitUntil(
        this::theEntryIsBlocked,
        "the outbox entry of the refused start to be blocked");

    // one attempt, not fifty: the adapter called the refusal permanent, so the store
    // wrote the block instead of counting 'vanillabp.outbox.block-after-attempts' down
    assertEquals(
        Integer.valueOf(1),
        attemptsOfTheEntry(),
        "the entry is blocked after the first attempt rather than after the last one");

  }

  @Test
  @DisplayName("A model without a none start event is refused in a way the outbox repeats")
  public void aModelWithoutANoneStartEventIsRefusedInAWayTheOutboxRepeats() {

    // a model which is started by a timer and by nothing else, while the application
    // still calls startWorkflow. The cluster refuses that create for good
    deploy(
        "timer-start-only.bpmn",
        """
            <?xml version="1.0" encoding="UTF-8"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" id="Definitions_timer_only" targetNamespace="http://bpmn.io/schema/bpmn">
              <bpmn:process id="TimerOnlyStartProcess" isExecutable="true">
                <bpmn:startEvent id="start">
                  <bpmn:outgoing>f1</bpmn:outgoing>
                  <bpmn:timerEventDefinition id="timer">
                    <bpmn:timeCycle xsi:type="bpmn:tFormalExpression">R1/PT30M</bpmn:timeCycle>
                  </bpmn:timerEventDefinition>
                </bpmn:startEvent>
                <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="end" />
                <bpmn:endEvent id="end">
                  <bpmn:incoming>f1</bpmn:incoming>
                </bpmn:endEvent>
              </bpmn:process>
            </bpmn:definitions>
            """);

    // the command phase two sends, sent without the outbox around it: what is asked here
    // is the cluster's answer and the adapter's verdict on it, and an outbox would only
    // repeat that answer for as long as the test is willing to wait
    final var refusal = assertThrows(
        Exception.class,
        () -> camunda8ProcessService
            .createProcessInstance("TimerOnlyStartProcess", Map.of("id", "1"), "1"));

    // what the cluster answers is a conflict, and the adapter repeats a conflict: it is
    // the code an instance which lost a race arrives with. So this start is attempted
    // until the entry runs out of attempts, although the model will not change in
    // between. This is measured behaviour, not a wish - a cluster which starts
    // answering such a request with 400 would break this assertion and would be an
    // improvement
    assertTrue(
        Camunda8Errors.rejection(refusal).startsWith("HTTP 409"),
        "the cluster refuses a create of a model without a none start event with a conflict, "
            + "but answered: "
            + Camunda8Errors.rejection(refusal));
    assertTrue(
        camunda8ProcessService.isPhaseTwoFailureRepeatable(refusal),
        "so the outbox repeats this start rather than blocking it");

  }

  @Test
  @DisplayName("A model which cannot evaluate an expression becomes an incident, not a refused start")
  public void aModelWhichCannotEvaluateBecomesAnIncidentRatherThanARefusedStart() throws Exception {

    // the Camunda 7 case which started all of this is an expression the engine evaluates
    // WHILE it creates the instance: the create command ends, the outbox repeats it and
    // the workflow never exists. Camunda 8 evaluates nothing of the kind at creation
    // time, so the same broken expression has to be measured where a Camunda 8 model
    // evaluates one. A gateway condition is that place, and it stands in for the start
    // because the question is not which element fails but WHEN: after the instance
    // exists, or instead of it
    deploy(
        "cannot-evaluate.bpmn",
        """
            <?xml version="1.0" encoding="UTF-8"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" id="Definitions_cannot_evaluate" targetNamespace="http://bpmn.io/schema/bpmn">
              <bpmn:process id="CannotEvaluateProcess" isExecutable="true">
                <bpmn:startEvent id="start">
                  <bpmn:outgoing>f1</bpmn:outgoing>
                </bpmn:startEvent>
                <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="gateway" />
                <bpmn:exclusiveGateway id="gateway">
                  <bpmn:incoming>f1</bpmn:incoming>
                  <bpmn:outgoing>f2</bpmn:outgoing>
                </bpmn:exclusiveGateway>
                <bpmn:sequenceFlow id="f2" sourceRef="gateway" targetRef="end">
                  <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">=order.total &gt; 100</bpmn:conditionExpression>
                </bpmn:sequenceFlow>
                <bpmn:endEvent id="end">
                  <bpmn:incoming>f2</bpmn:incoming>
                </bpmn:endEvent>
              </bpmn:process>
            </bpmn:definitions>
            """);

    final var instance = camunda8ProcessService
        .createProcessInstance("CannotEvaluateProcess", Map.of("id", "1"), "1");

    // the create came back with a key, so the outbox entry of this start is done and
    // nothing about the broken model reaches the outbox at all
    assertTrue(
        instance.getProcessInstanceKey() > 0,
        "the cluster takes the create of a model it cannot run");

    // the workflow EXISTS and carries an incident an operator can see and resolve. That
    // is the difference worth writing down: the Camunda 7 case loses the workflow
    // silently, this one keeps it and says what is wrong with it
    awaitUntil(
        () -> !client()
            .newIncidentSearchRequest()
            .filter(filter -> filter.processInstanceKey(instance.getProcessInstanceKey()))
            .send()
            .join()
            .items()
            .isEmpty(),
        "the incident of the instance whose gateway condition cannot be evaluated");

  }

  /**
   * Whether the one entry of the phase-two outbox is blocked. The scenario has a
   * database of its own and starts one workflow, so there is never another entry.
   */
  private boolean theEntryIsBlocked() {

    return Boolean.TRUE.equals(jdbcTemplate.queryForObject(IS_THE_ENTRY_BLOCKED, Boolean.class));

  }

  /**
   * @return How often the store attempted the entry, see {@link #theEntryIsBlocked()}
   */
  private Integer attemptsOfTheEntry() {

    return jdbcTemplate.queryForObject(ATTEMPTS_OF_THE_ENTRY, Integer.class);

  }

  private void deploy(
      final String name,
      final String model) {

    client()
        .newDeployResourceCommand()
        .addResourceStringUtf8(model, name)
        .send()
        .join();

  }

  private void awaitUntil(
      final Supplier<Boolean> condition,
      final String description) throws InterruptedException {

    // generous on purpose: the incident of the third case is read from secondary
    // storage, and the export pipeline is the slowest part of this class
    final var deadline = System.currentTimeMillis() + 120_000;
    while (!Boolean.TRUE.equals(condition.get())) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("timed out waiting for: "
            + description);
      }
      Thread.sleep(200);
    }

  }

}
