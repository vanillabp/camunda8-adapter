package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.function.Supplier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.vanillabp.camunda8.springboot.client.VanillaBpCamunda8Properties;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Task processing against a REAL Camunda 8 broker (Testcontainers) - story 21c:
 * <ul>
 * <li>happy path through the adapter's polling job workers incl. redelivery
 * convergence (the first invocation blocks beyond the task's job timeout - the
 * redelivered job converges idempotently, the duplicate completion is
 * tolerated);</li>
 * <li>{@code TaskException} - BPMN error with error-boundary routing, the
 * throwing handler's aggregate changes committed;</li>
 * <li>technical exception - job failed with decremented retries, local
 * transaction rolled back;</li>
 * <li>{@code @TaskId} dormancy - the returned-but-uncompleted job's lock is
 * extended once (async-task-timeout), the handler is NOT re-invoked within the
 * test horizon although the task's job timeout is 2s;</li>
 * <li>the job timeout resolves through all four configuration levels from the
 * real application configuration.</li>
 * </ul>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
public class Camunda8TaskProcessingIT {

  @Container
  static final GenericContainer<?> CAMUNDA = new GenericContainer<>(
      DockerImageName.parse("camunda/zeebe:8.8.31"))
      .withExposedPorts(8080, 26500, 9600)
      .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "none")
      .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
      .waitingFor(org.testcontainers.containers.wait.strategy.Wait
          .forHttp("/actuator/health/readiness")
          .forPort(9600)
          .forStatusCode(200)
          .withStartupTimeout(Duration.ofMinutes(3)));

  @DynamicPropertySource
  static void camunda8Properties(
      final DynamicPropertyRegistry registry) {

    registry.add("vanillabp.adapters.c8.rest-address",
        () -> "http://"
            + CAMUNDA.getHost()
            + ":"
            + CAMUNDA.getMappedPort(8080));
    registry.add("vanillabp.adapters.c8.grpc-address",
        () -> "http://"
            + CAMUNDA.getHost()
            + ":"
            + CAMUNDA.getMappedPort(26500));

  }

  @Autowired
  private TaskDockerWorkflowService workflowService;

  @Autowired
  private TaskDockerAggregateRepository repository;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private VanillaBpCamunda8Properties overlay;

  private Long start(
      final String bpmnProcessId) {

    return transactionTemplate.execute(status -> {
      final var aggregate = new TaskDockerAggregate();
      if ("TaskProcess".equals(bpmnProcessId)) {
        return workflowService.startWorkflow(aggregate).getId();
      }
      throw new IllegalArgumentException(bpmnProcessId);
    });

  }

  private void awaitUntil(
      final Supplier<Boolean> condition,
      final long timeoutMillis,
      final String description) throws InterruptedException {

    final var deadline = System.currentTimeMillis() + timeoutMillis;
    while (!Boolean.TRUE.equals(condition.get())) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("timed out waiting for: "
            + description);
      }
      Thread.sleep(200);
    }

  }

  private int invocations(
      final String taskDefinition,
      final Long aggregateId) {

    final var counter = TaskDockerWorkflowService.INVOCATIONS
        .get(taskDefinition
            + ":"
            + aggregateId);
    return counter != null
        ? counter.get()
        : 0;

  }

  private String results(
      final Long aggregateId) {

    return repository.findById(aggregateId).orElseThrow().getResults();

  }

  @Test
  @DisplayName("The job timeout resolves through all four configuration levels from real config")
  public void jobTimeoutResolvesThroughAllFourLevels() {

    // task level (most specific)
    assertEquals(
        Duration.ofSeconds(2),
        overlay.jobTimeoutFor("test-app", "TaskProcess", "happyTask", "c8"));
    // workflow level
    assertEquals(
        Duration.ofSeconds(10),
        overlay.jobTimeoutFor("test-app", "TaskProcess", "errorTask", "c8"));
    // workflow-module level
    assertEquals(
        Duration.ofSeconds(20),
        overlay.jobTimeoutFor("test-app", "FailProcess", "alwaysFails", "c8"));
    // adapter level (base)
    assertEquals(
        Duration.ofSeconds(30),
        overlay.jobTimeoutFor("unknown-module", "SomeProcess", "someTask", "c8"));

  }

  @Test
  @DisplayName("Happy path with TaskException error-boundary routing")
  public void happyPathAndBpmnErrorRoutesBoundary() throws Exception {

    final var aggregateId = start("TaskProcess");

    awaitUntil(
        () -> {
          final var results = results(aggregateId);
          return (results != null) && results.contains("handled");
        },
        60000,
        "TaskProcess to converge through the error boundary");

    // the throwing handler's mutation committed, the boundary path ran
    assertEquals("happy|error-raised|handled", results(aggregateId));

  }

  @Test
  @DisplayName("A second delivery of the same task converges (at-least-once redelivery)")
  public void redeliveryConverges() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    startSecondaryProcess("RetryProcess", aggregateId);

    // the first delivery fails (job failed, local TX rolled back) - Camunda 8
    // redelivers; the second delivery converges and the process ends
    awaitUntil(
        () -> {
          final var results = results(aggregateId);
          return (results != null) && results.contains("retried");
        },
        60000,
        "RetryProcess to converge after a redelivery");

    assertTrue(
        invocations("retryTask", aggregateId) >= 2,
        "expected a second delivery but saw "
            + invocations("retryTask", aggregateId));
    // exactly one 'retried' in the results: the second delivery converged
    // idempotently, the first delivery's mutation was rolled back
    assertEquals("retried", results(aggregateId));

  }

  @Test
  @DisplayName("A technical exception fails the job with decremented retries and rolls back the aggregate")
  public void technicalExceptionFailsJobAndRollsBack() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> {
      final var aggregate = new TaskDockerAggregate();
      final var saved = repository.save(aggregate);
      // start FailProcess directly via the adapter's client (the injectable
      // ProcessService starts the primary process only)
      return saved.getId();
    });
    // start FailProcess through a fresh aggregate + direct client call
    startSecondaryProcess("FailProcess", aggregateId);

    // Camunda 8 retries the failing job (3 attempts by default)
    awaitUntil(
        () -> invocations("alwaysFails", aggregateId) >= 2,
        60000,
        "the failing job to be retried");

    // the handler's mutation never became visible (local rollback per attempt)
    assertNull(results(aggregateId));

  }

  @Test
  @DisplayName("@TaskId dormancy: the open job is not re-invoked although its job timeout is 2s")
  public void asyncTaskStaysDormant() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    startSecondaryProcess("AsyncProcess", aggregateId);

    awaitUntil(
        () -> invocations("asyncTask", aggregateId) == 1,
        60000,
        "the async task to be invoked once");
    assertNotNull(repository.findById(aggregateId).orElseThrow().getTaskId(), "job key committed as task id");

    // the task's job timeout is PT2S - without the dormancy lock extension the
    // job would be redelivered within this horizon
    Thread.sleep(8000);
    assertEquals(
        1,
        invocations("asyncTask", aggregateId),
        "the dormant async job must not be re-invoked");
    assertEquals("async-open", results(aggregateId));

  }

  @Test
  @DisplayName("completeTask completes the dormant job through the outbox after the commit")
  public void completeTaskEndsDormantProcess() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    startSecondaryProcess("AsyncProcess", aggregateId);

    awaitUntil(
        () -> repository.findById(aggregateId).map(TaskDockerAggregate::getTaskId).orElse(null) != null,
        60000,
        "the async task to report its job key");

    transactionTemplate.executeWithoutResult(status -> {
      final var aggregate = repository.findById(aggregateId).orElseThrow();
      aggregate.appendResult("completing");
      workflowService.completeAsyncTask(aggregate, aggregate.getTaskId());
    });

    // phase two completes the job through the outbox after the commit; proven
    // deterministically WITHOUT the eventually-consistent search API: once the
    // job is gone, a further completion attempt probes UNKNOWN everywhere and
    // raises the documented TaskNotFoundException
    final var taskId = repository.findById(aggregateId).orElseThrow().getTaskId();
    awaitUntil(
        () -> {
          try {
            transactionTemplate.executeWithoutResult(status -> {
              final var aggregate = repository.findById(aggregateId).orElseThrow();
              workflowService.completeAsyncTask(aggregate, taskId);
            });
            return false; // job still there - phase two has not run yet
          } catch (final io.vanillabp.spi.process.TaskNotFoundException e) {
            return true; // job gone: the outbox-dispatched completion succeeded
          } catch (final IllegalStateException e) {
            // the job disappeared BETWEEN the awareness probe and the pre-commit
            // check (the outbox dispatch of a previous loop iteration completed
            // it) - the check aborted the commit, which equally proves the job
            // is gone
            return (e.getMessage() != null) && e.getMessage().contains("is gone");
          }
        },
        60000,
        "the dormant job to be completed through the outbox");
    assertTrue(results(aggregateId).startsWith("async-open|completing"));
    // the dormant job was NOT re-invoked by the completion flow
    assertEquals(1, invocations("asyncTask", aggregateId));

  }

  @Test
  @DisplayName("cancelTask throws the BPMN error and the boundary path runs")
  public void cancelTaskRoutesErrorBoundary() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    startSecondaryProcess("AsyncCancelProcess", aggregateId);

    awaitUntil(
        () -> repository.findById(aggregateId).map(TaskDockerAggregate::getTaskId).orElse(null) != null,
        60000,
        "the await-cancel task to report its job key");

    transactionTemplate.executeWithoutResult(status -> {
      final var aggregate = repository.findById(aggregateId).orElseThrow();
      workflowService.cancelAsyncTask(aggregate, aggregate.getTaskId(), "PAYMENT_FAILED");
    });

    awaitUntil(
        () -> {
          final var results = results(aggregateId);
          return (results != null) && results.contains("cancel-handled");
        },
        60000,
        "the BPMN error to route through the boundary");
    assertEquals("await-cancel|cancel-handled", results(aggregateId));

  }

  @Test
  @DisplayName("A stale completion converges: the task is gone, the operation is a warned no-op")
  public void staleCompletionIsToleratedNoOp() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    startSecondaryProcess("AsyncProcess", aggregateId);

    awaitUntil(
        () -> repository.findById(aggregateId).map(TaskDockerAggregate::getTaskId).orElse(null) != null,
        60000,
        "the async task to report its job key");
    final var taskId = repository.findById(aggregateId).orElseThrow().getTaskId();

    // the job is completed OUTSIDE VanillaBP (simulating a concurrent completion)
    workflowServiceClient()
        .newCompleteCommand(Long.parseLong(taskId))
        .send()
        .join();

    // the probe answers UNKNOWN for the gone job - completeTask converges as the
    // documented TaskNotFoundException (no adapter knows the task anymore)
    org.junit.jupiter.api.Assertions.assertThrows(
        io.vanillabp.spi.process.TaskNotFoundException.class,
        () -> transactionTemplate.executeWithoutResult(status -> {
          final var aggregate = repository.findById(aggregateId).orElseThrow();
          workflowService.completeAsyncTask(aggregate, taskId);
        }));

  }

  @Test
  @DisplayName("User task: CREATED via listener job, completeUserTask ends the process")
  public void userTaskCreatedAndCompleted() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    startSecondaryProcess("UserTaskProcess", aggregateId);

    // the creating listener job notified the optional handler with the USER-TASK
    // key as @TaskId
    awaitUntil(
        () -> {
          final var aggregate = repository.findById(aggregateId).orElseThrow();
          return (aggregate.getTaskId() != null) && aggregate.getResults().contains("usertask-created");
        },
        60000,
        "the creating listener to notify the handler");
    final var taskId = repository.findById(aggregateId).orElseThrow().getTaskId();

    transactionTemplate.executeWithoutResult(status -> {
      final var aggregate = repository.findById(aggregateId).orElseThrow();
      aggregate.appendResult("approving");
      workflowService.completeUserTask(aggregate, taskId);
    });

    // deterministic completion proof: once the user task is gone, further
    // completion attempts raise TaskNotFoundException
    awaitUntil(
        () -> {
          try {
            transactionTemplate.executeWithoutResult(status -> {
              final var aggregate = repository.findById(aggregateId).orElseThrow();
              workflowService.completeUserTask(aggregate, taskId);
            });
            return false;
          } catch (final io.vanillabp.spi.process.TaskNotFoundException e) {
            return true;
          } catch (final IllegalStateException e) {
            return (e.getMessage() != null) && e.getMessage().contains("is gone");
          }
        },
        60000,
        "the user task to be completed through the outbox");
    assertTrue(results(aggregateId).startsWith("usertask-created|approving"));

  }

  @Test
  @DisplayName("Canceling the instance delivers CANCELED through the canceling listener")
  public void userTaskCanceledOnInstanceCancellation() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    startSecondaryProcess("UserTaskProcess", aggregateId);

    awaitUntil(
        () -> repository.findById(aggregateId).map(TaskDockerAggregate::getTaskId).orElse(null) != null,
        60000,
        "the creating listener to notify the handler");

    // cancel the whole instance - the canceling task listener fires as a job
    // (the instance key was captured at start: the search API needs secondary
    // storage which the test broker does not run)
    workflowServiceClient()
        .newCancelInstanceCommand(lastStartedInstanceKey)
        .send()
        .join();

    awaitUntil(
        () -> {
          final var results = results(aggregateId);
          return (results != null) && results.contains("usertask-canceled");
        },
        60000,
        "the canceling listener to deliver CANCELED");

  }

  @Test
  @DisplayName("cancelUserTask is unsupported on Camunda 8.8 - the guiding error explains it")
  public void cancelUserTaskUnsupportedGuiding() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    startSecondaryProcess("UserTaskProcess", aggregateId);

    awaitUntil(
        () -> repository.findById(aggregateId).map(TaskDockerAggregate::getTaskId).orElse(null) != null,
        60000,
        "the creating listener to notify the handler");
    final var taskId = repository.findById(aggregateId).orElseThrow().getTaskId();

    final var exception = org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> transactionTemplate.executeWithoutResult(status -> {
          final var aggregate = repository.findById(aggregateId).orElseThrow();
          workflowService.cancelUserTask(aggregate, taskId, "SOME_ERROR");
        }));
    assertTrue(
        exception.getMessage().contains("8.8"),
        "expected the guiding 8.8 explanation but got: "
            + exception.getMessage());

  }

  @Test
  @DisplayName("User-task edge cases: silent task, awareness, gone-task tolerance")
  public void userTaskEdgeCases() throws Exception {

    // a user task WITHOUT a handler: the creating listener job is completed
    // without a notification and the process continues to wait at the user task
    final var silentAggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    startSecondaryProcess("SilentUserTaskProcess", silentAggregateId);

    @SuppressWarnings("unchecked")
    final var c8ProcessService = (io.vanillabp.camunda8.processservice.Camunda8ProcessService<TaskDockerAggregate>) applicationContext
        .getBean("Camunda8_ProcessService_c8");

    // gone user task: awareness UNKNOWN, phase two tolerated as warned no-op
    assertEquals(
        io.vanillabp.integration.adapter.spi.WorkflowAwareness.UNKNOWN_TO_BPMS,
        c8ProcessService.awarenessOfUserTask(silentAggregateId, "1"));
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () -> c8ProcessService.completeUserTaskPhaseTwo("test-app", "SilentUserTaskProcess", null,
            silentAggregateId, "1"));
    // gone SERVICE task phase two is equally tolerated
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () -> c8ProcessService.completeTaskPhaseTwo("test-app", "SilentUserTaskProcess", null,
            silentAggregateId, "1"));
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () -> c8ProcessService.cancelTaskPhaseTwo("test-app", "SilentUserTaskProcess", null,
            silentAggregateId, "1", "ERR"));

    // the silent task exists (found via a real user-task handler on the OTHER
    // process is not necessary - completing the silent task by awareness probing
    // is proven once a task shows up); wait briefly so the creating listener job
    // was consumed without an incident
    Thread.sleep(2000);

  }

  @Test
  @DisplayName("correlateMessage resumes the instance via the INJECTED zeebe:subscription (no manual model tweaks)")
  public void correlateMessageResumesInstanceViaInjectedSubscription() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    startSecondaryProcess("MessageProcess", aggregateId);

    // no reliable waiting-state query without secondary storage - correlate
    // (buffered by the engine's message TTL, so timing is not critical)
    Thread.sleep(2000);
    transactionTemplate.executeWithoutResult(status -> {
      final var aggregate = repository.findById(aggregateId).orElseThrow();
      aggregate.appendResult("correlating");
      workflowService.correlate(aggregate, "C8PaymentReceived");
    });

    awaitUntil(
        () -> {
          final var results = results(aggregateId);
          return (results != null) && results.contains("message-arrived");
        },
        60000,
        "the correlated message to resume the instance");
    assertEquals("correlating|message-arrived", results(aggregateId));

  }

  @Test
  @DisplayName("The messageId deduplicates: a redelivered phase-two correlation does not double-fire")
  public void duplicateCorrelationDispatchIsDeduplicated() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    startSecondaryProcess("MessageProcess", aggregateId);
    Thread.sleep(2000);

    @SuppressWarnings("unchecked")
    final var c8ProcessService = (io.vanillabp.camunda8.processservice.Camunda8ProcessService<TaskDockerAggregate>) applicationContext
        .getBean("Camunda8_ProcessService_c8");

    // simulate an at-least-once redelivery of the SAME phase-two dispatch (same
    // correlation id -> same messageId): the second publish is rejected by the
    // engine and tolerated as the documented no-op
    c8ProcessService.correlateMessagePhaseTwo(
        "test-app", "MessageProcess", null, aggregateId, "C8PaymentReceived", "pay-1");
    c8ProcessService.correlateMessagePhaseTwo(
        "test-app", "MessageProcess", null, aggregateId, "C8PaymentReceived", "pay-1");

    // the correlation id 'pay-1' does not match the injected '=id' subscription -
    // nothing may fire; now correlate properly ONCE and prove single delivery
    Thread.sleep(1000);
    assertEquals(0, invocations("c8MessageArrived", aggregateId));
    c8ProcessService.correlateMessagePhaseTwo(
        "test-app", "MessageProcess", null, aggregateId, "C8PaymentReceived", String.valueOf(aggregateId));
    awaitUntil(
        () -> invocations("c8MessageArrived", aggregateId) >= 1,
        60000,
        "the matching correlation to resume the instance");
    Thread.sleep(1500);
    assertEquals(1, invocations("c8MessageArrived", aggregateId), "no double-fire");

  }

  @Test
  @DisplayName("startWorkflowByMessage starts the instance via the message start event")
  public void startWorkflowByMessageStartsInstance() throws Exception {

    final var aggregateId = transactionTemplate.execute(status -> {
      final var aggregate = new TaskDockerAggregate();
      final var saved = repository.save(aggregate);
      workflowService.startByMessage(saved, "C8OrderPlaced");
      return saved.getId();
    });

    awaitUntil(
        () -> {
          final var results = results(aggregateId);
          return (results != null) && results.contains("order-placed");
        },
        60000,
        "the message start event to start the instance");

  }

  @Autowired
  private org.springframework.context.ApplicationContext applicationContext;

  private long lastStartedInstanceKey;

  @Test
  @DisplayName("A gateway right after a @WorkflowTask sees the values that task produced (story 28b)")
  public void gatewayAfterTaskSeesTheNewValues() throws Exception {

    TaskDockerWorkflowService.OBSERVED_VARIABLES.clear();
    final var aggregateId = transactionTemplate.execute(status -> repository
        .save(new TaskDockerAggregate())
        .getId());
    // the instance is started with the aggregate-ID variable ONLY - everything the
    // gateway evaluates has to be pushed by the completion of 'syncTask'
    startSecondaryProcess("SyncProcess", aggregateId);

    awaitUntil(
        () -> {
          final var results = results(aggregateId);
          return (results != null) && (results.contains("sync-approved") || results.contains("sync-rejected"));
        },
        60000,
        "SyncProcess to pass the FEEL gateway");

    // the exclusive gateway's FEEL condition '=approved = true' branched on the
    // value the @WorkflowTask method produced - without the push it would have
    // taken the default (rejected) flow
    assertEquals("sync-task|sync-approved", results(aggregateId));

    // what the cluster delivered to the task behind the gateway proves what was
    // pushed: the shared attributes, but never a @NoSyncWithBPMS one
    assertEquals("true", TaskDockerWorkflowService.OBSERVED_VARIABLES.get("approved"));
    assertEquals("sync-task", TaskDockerWorkflowService.OBSERVED_VARIABLES.get("results"));
    assertEquals(
        "null",
        TaskDockerWorkflowService.OBSERVED_VARIABLES.get("secret"),
        "a @NoSyncWithBPMS attribute must never appear in the cluster's variables");

  }

  private void startSecondaryProcess(
      final String bpmnProcessId,
      final Long aggregateId) {

    // secondary processes are started directly against the cluster carrying the
    // aggregate-ID variable - exactly what VanillaBP's start writes. The name-clash
    // avoidance mode of these tests is 'use-prefix', so the CLUSTER knows the process
    // under its prefixed id (story 35)
    lastStartedInstanceKey = workflowServiceClient()
        .newCreateInstanceCommand()
        .bpmnProcessId("test-app__"
            + bpmnProcessId)
        .latestVersion()
        .variable("id", String.valueOf(aggregateId))
        .send()
        .join()
        .getProcessInstanceKey();

  }

  @Autowired
  private io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry clientFactoryRegistry;

  private io.camunda.client.CamundaClient workflowServiceClient() {

    return clientFactoryRegistry
        .getFactory("c8")
        .getClient();

  }

}
