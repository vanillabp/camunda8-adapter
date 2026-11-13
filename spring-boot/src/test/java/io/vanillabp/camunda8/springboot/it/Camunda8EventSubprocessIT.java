package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.function.Supplier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.support.TransactionTemplate;

import io.vanillabp.camunda8.springboot.SpringBootTestOnTheSharedCluster;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * End-to-end test of an event subprocess against a real Camunda 8.
 * <p>
 * Its start event fires on a timer, so it looks like a start which the cluster decides on.
 * It is not. The workflow already runs, and it has the aggregate which the application gave
 * it at the start. So the application serves that start event with no
 * <code>&#64;WorkflowStartedByBpms</code> method, and the deployment of this model still has
 * to boot.
 * <p>
 * What runs afterwards proves the second half. The event subprocess takes the workflow
 * over, and the task inside it is handed the aggregate which the application created.
 * <p>
 * An event subprocess may also start on a message. Its subscription belongs to the running
 * workflow, so Camunda 8 needs a correlation key for it, like for an intermediate catch
 * event. The model below carries none, and the application correlates by its aggregate. So
 * the adapter has to add the key.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
public class Camunda8EventSubprocessIT extends SpringBootTestOnTheSharedCluster {

  @Autowired
  private EventSubprocessDockerWorkflowService workflowService;

  @Autowired
  private EventSubprocessDockerAggregateRepository repository;

  @Autowired
  private MessageEventSubprocessDockerWorkflowService messageWorkflowService;

  @Autowired
  private MessageEventSubprocessDockerAggregateRepository messageRepository;

  @Autowired
  private TransactionTemplate transactionTemplate;

  private void awaitUntil(
      final Supplier<Boolean> condition,
      final String description) throws InterruptedException {

    // the workflow instance is created after the commit (phase two). The event subprocess
    // waits two more seconds, and then the job worker polls
    final var deadline = System.currentTimeMillis() + 180_000;
    while (!Boolean.TRUE.equals(condition.get())) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("timed out waiting for: "
            + description);
      }
      Thread.sleep(200);
    }

  }

  @Test
  @DisplayName("the event subprocess takes the workflow over and its task gets the aggregate of that workflow")
  public void theEventSubprocessRunsAgainstTheAggregateOfTheRunningWorkflow() throws Exception {

    final var aggregateId = transactionTemplate
        .execute(status -> workflowService.startWorkflow().getId());
    assertNotNull(aggregateId);

    awaitUntil(
        () -> "recordTakeOver".equals(
            repository
                .findById(aggregateId)
                .map(EventSubprocessDockerAggregate::getProcessedBy)
                .orElse(null)),
        "the event subprocess to take the workflow over");

  }

  @Test
  @DisplayName("a message correlated by the aggregate starts the event subprocesses whose model names no correlation key")
  public void aMessageStartsTheEventSubprocessesOfTheRunningWorkflow() throws Exception {

    final var aggregateId = transactionTemplate
        .execute(status -> messageWorkflowService.startWorkflow().getId());
    assertNotNull(aggregateId);

    // the event subprocess which does not interrupt the workflow. It runs next to the
    // waiting token, so the workflow still waits afterwards
    transactionTemplate.executeWithoutResult(status -> messageWorkflowService
        .correlate(messageRepository.findById(aggregateId).orElseThrow(), "C8NoteAdded"));
    awaitUntil(
        () -> "note;".equals(
            messageRepository
                .findById(aggregateId)
                .map(MessageEventSubprocessDockerAggregate::getNotes)
                .orElse(null)),
        "the message to start the event subprocess which does not interrupt the workflow");

    // the event subprocess which takes the workflow over
    transactionTemplate.executeWithoutResult(status -> messageWorkflowService
        .correlate(messageRepository.findById(aggregateId).orElseThrow(), "C8TakeOver"));
    awaitUntil(
        () -> "recordMessageTakeOver".equals(
            messageRepository
                .findById(aggregateId)
                .map(MessageEventSubprocessDockerAggregate::getTakenOverBy)
                .orElse(null)),
        "the message to start the event subprocess which takes the workflow over");
    assertEquals(
        "note;",
        messageRepository.findById(aggregateId).orElseThrow().getNotes(),
        "the note was recorded once, so the take-over message did not start the other event "
            + "subprocess");

  }

}
