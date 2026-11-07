package io.vanillabp.camunda8.processservice;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.ConnectException;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.camunda.client.CamundaClient;
import io.camunda.client.api.CamundaFuture;
import io.camunda.client.api.ProblemDetail;
import io.camunda.client.api.command.ClientException;
import io.camunda.client.api.command.ClientStatusException;
import io.camunda.client.api.command.ProblemException;
import io.camunda.client.api.command.UpdateTimeoutJobCommandStep1;
import io.camunda.client.api.command.UpdateUserTaskCommandStep1;
import io.grpc.Status;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.client.Camunda8CommandRetry;
import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;
import io.vanillabp.spi.process.TaskNotFoundException;

/**
 * The check which phase one runs right before the caller commits, while the cluster says it is
 * busy. The broker's flow control refuses the change of a job timeout and the update of a user
 * task when too many requests are in flight. That refusal says nothing about the task. So the
 * check waits it out for a few attempts instead of rolling the caller's work back, and repeats
 * nothing else: the caller's transaction is open while it waits.
 * <p>
 * The client is a mock. What the real client adds on REST, one repetition a second later, is
 * held by {@code Camunda8ClientOnABusyClusterTest}.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8PreCommitCheckOnABusyClusterTest {

  private static final String TASK_ID = "4711";

  private final CamundaClient client = mock(CamundaClient.class);

  private final UpdateTimeoutJobCommandStep1.UpdateTimeoutJobCommandStep2 timeoutChange = mock(
      UpdateTimeoutJobCommandStep1.UpdateTimeoutJobCommandStep2.class,
      RETURNS_SELF);

  private final UpdateUserTaskCommandStep1 userTaskUpdate = mock(UpdateUserTaskCommandStep1.class, RETURNS_SELF);

  {
    final var step1 = mock(UpdateTimeoutJobCommandStep1.class, RETURNS_SELF);
    Mockito.lenient().when(step1.timeout(Mockito.any(Duration.class))).thenReturn(timeoutChange);
    Mockito.lenient().when(client.newUpdateTimeoutCommand(Mockito.anyLong())).thenReturn(step1);
    Mockito.lenient().when(client.newUpdateUserTaskCommand(Mockito.anyLong())).thenReturn(userTaskUpdate);
  }

  private static ProblemException problem(
      final int status,
      final String title) {

    final var details = new ProblemDetail();
    details.setStatus(status);
    details.setTitle(title);
    return new ProblemException(status, title, details);

  }

  /**
   * How REST reports a busy cluster, measured against 8.8.46, 8.9.28 and 8.10.7.
   */
  private static ProblemException busy() {

    return problem(503, "RESOURCE_EXHAUSTED");

  }

  @SuppressWarnings("unchecked")
  private static <T> CamundaFuture<T> answered() {

    return mock(CamundaFuture.class);

  }

  private Camunda8ProcessService<Object> aService() {

    final var configuration = new Camunda8AdapterConfiguration();
    // nothing ever contacts this address, every request of this test meets the mock
    configuration.setRestAddress("http://localhost:1");
    configuration.setWorkflowVisibilityTimeout(Duration.ZERO);
    final var clientFactory = new Camunda8ClientFactory("c8", configuration) {

      @Override
      public CamundaClient getClient() {
        return client;
      }

    };
    // a registrar which runs the check at once, as the caller's commit would
    return new Camunda8ProcessService<>(
        "c8", clientFactory, Duration.ofDays(14), (
            aggregateClass,
            check) -> check.run(), null);

  }

  private void checkTheCompletionOf(
      final PhaseOperation operation) {

    PhaseOperations
        .phaseOne(
            aService(),
            operation,
            "test-module",
            "TestProcess",
            null,
            new Object(),
            PhaseOperations.args(PhaseTwoCall.ARG_TASK_ID, TASK_ID));

  }

  private static List<String> warningsWhile(
      final Runnable check) {

    final var logWatcher = new ListAppender<ILoggingEvent>();
    logWatcher.start();
    final var retryLog = (Logger) LoggerFactory.getLogger(Camunda8CommandRetry.class);
    retryLog.addAppender(logWatcher);
    try {
      check.run();
    } finally {
      retryLog.detachAppender(logWatcher);
    }
    return logWatcher.list
        .stream()
        .filter(event -> event.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();

  }

  @Test
  @DisplayName("A service task: a busy cluster is waited out, and the caller's transaction commits")
  public void aBusyClusterIsWaitedOut() {

    Mockito.when(timeoutChange.send()).thenThrow(busy(), busy()).thenReturn(answered());

    assertDoesNotThrow(() -> checkTheCompletionOf(PhaseOperation.COMPLETE_TASK));

    verify(timeoutChange, times(3)).send();

  }

  @Test
  @DisplayName("A user task: a busy cluster is waited out the same way")
  public void aBusyClusterIsWaitedOutForAUserTask() {

    Mockito.when(userTaskUpdate.send()).thenThrow(busy()).thenReturn(answered());

    assertDoesNotThrow(() -> checkTheCompletionOf(PhaseOperation.COMPLETE_USER_TASK));

    verify(userTaskUpdate, times(2)).send();

  }

  @Test
  @DisplayName("A cluster which stays busy rolls the caller back after five attempts, with a WARN which says so")
  public void aClusterWhichStaysBusyRollsTheCallerBack() {

    Mockito.when(timeoutChange.send()).thenThrow(new ClientStatusException(Status.RESOURCE_EXHAUSTED, null));

    final var warnings = warningsWhile(() -> assertThrows(
        ClientStatusException.class,
        () -> checkTheCompletionOf(PhaseOperation.CANCEL_TASK)));

    verify(timeoutChange, times(Camunda8CommandRetry.MAX_ATTEMPTS)).send();
    assertTrue(
        warnings
            .stream()
            .anyMatch(warning -> warning
                .contains("pre-commit check of task '4711' was refused by a busy cluster on all 5 attempts")),
        warnings.toString());

  }

  @Test
  @DisplayName("A task which is gone is not asked again")
  public void aGoneTaskIsNotAskedAgain() {

    Mockito.when(timeoutChange.send()).thenThrow(problem(404, "NOT_FOUND"));

    assertThrows(TaskNotFoundException.class, () -> checkTheCompletionOf(PhaseOperation.COMPLETE_TASK));

    verify(timeoutChange, times(1)).send();

  }

  @Test
  @DisplayName("A cluster which cannot be reached is not asked again, because the caller's transaction is open")
  public void anOutageIsNotRepeated() {

    Mockito.when(timeoutChange.send()).thenThrow(new ClientException("no connection", new ConnectException()));

    final var warnings = warningsWhile(() -> assertThrows(
        ClientException.class,
        () -> checkTheCompletionOf(PhaseOperation.COMPLETE_TASK)));

    verify(timeoutChange, times(1)).send();
    assertTrue(
        warnings.stream().noneMatch(warning -> warning.contains("was refused by a busy cluster on all 5 attempts")),
        warnings.toString());

  }

  @Test
  @DisplayName("A refusal of the request itself is not repeated either")
  public void aRefusedRequestIsNotRepeated() {

    Mockito.when(timeoutChange.send()).thenThrow(problem(500, "INTERNAL_ERROR"));

    assertThrows(ProblemException.class, () -> checkTheCompletionOf(PhaseOperation.COMPLETE_TASK));

    verify(timeoutChange, times(1)).send();

  }

}
