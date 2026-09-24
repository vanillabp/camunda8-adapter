package io.vanillabp.camunda8.springboot;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.enums.ProcessInstanceState;
import io.camunda.client.api.search.response.ProcessInstance;
import io.vanillabp.camunda8.client.Camunda8Errors;
import io.vanillabp.camunda8.test.ClusterUnderTest;

/**
 * A test of this module which runs against the one cluster the module starts.
 * <p>
 * Every class here used to bring a cluster of its own. They all run in the same JVM - one
 * fork per module is what Failsafe does - so one container can serve all of them, and the
 * module starts one cluster instead of some thirty. The container is started when the first
 * class asks for its address and it is never stopped, see
 * {@link ClusterUnderTest#sharedCluster()}.
 * <p>
 * The price is what the cluster remembers. All classes of this module deploy the same files
 * under the same workflow module id, so the cluster holds one set of definitions rather than
 * one per class, and Camunda 8 answers a file it already holds with the version it already
 * has. What a class leaves RUNNING is the part which needs doing something about: prefixed
 * job types are the same for every class, so the workers of the class running next would
 * activate the jobs of a workflow nobody is waiting for any more, and look for a workflow
 * aggregate their own database never held. That is why every class starts by cancelling what
 * is still running and waits until the engine has let go of it.
 * <p>
 * What a class reads and what it cancels are two different things. The workflows come from the
 * search, which answers out of the secondary storage, and the cancellation goes to the engine.
 * A class which only sent its cancellations went on while the engine still held them, and that
 * cost a whole nightly run. On the 8.10 line it cost a second one, because the user tasks such
 * a leftover instance holds keep the alpha's response-mapper defect firing for the rest of the
 * run.
 * <p>
 * The search is the other half of it, and it is the half no class can wait for: it still
 * reported workflows as running a minute after the engine had let go of them. So a test which
 * looks a workflow up by a variable binds its search to a process of its own, rather than
 * trusting that what comes back belongs to it.
 * <p>
 * Two kinds of test keep a cluster of their own, and each of them says so where it stands.
 * One needs the cluster CONFIGURED differently, with authentication switched on, which makes
 * it a different thing under test. The other needs a cluster which has never seen its model,
 * because the cluster acts on a deployment once and never again: a timer start event fires
 * for the first deployment of its version and for no later one. Both declare a
 * {@code @Container} field, the way every class here did before.
 */
@Testcontainers(disabledWithoutDocker = true)
public abstract class TestOnTheSharedCluster {

  /**
   * @return Where the shared cluster answers REST requests
   */
  public static String restAddress() {

    return ClusterUnderTest.restAddress(ClusterUnderTest.sharedCluster());

  }

  /**
   * @return Where the shared cluster answers gRPC requests
   */
  public static String grpcAddress() {

    return ClusterUnderTest.grpcAddress(ClusterUnderTest.sharedCluster());

  }

  /**
   * Takes everything the class before left on the shared cluster away from this one, before
   * the application of this class boots.
   * <p>
   * JUnit calls it after the extensions of the class and before the first test instance is
   * built, which is when Spring loads the test's context. So by the time this runs, the
   * application of the class before is closed and this class has not opened a worker yet.
   *
   * @throws InterruptedException Where the wait is interrupted
   */
  @BeforeAll
  static void nothingOfTheClassBeforeReachesThisOne() throws InterruptedException {

    final var startedAt = System.nanoTime();
    cancelWhateverAnEarlierClassLeftRunning();
    waitOutAnActivationRequestOfTheClassBefore(startedAt);

  }

  /**
   * Cancels what an earlier class left running, until the ENGINE holds none of it any more.
   * <p>
   * The workflows are read from the search, which answers out of the secondary storage, while
   * cancelling them is a command to the engine, so the two do not end at the same moment. This
   * waits for the engine: an engine which does not hold a workflow can hand no job of it to a
   * worker of this class, which is the whole point. It says so with a <code>404</code> to the
   * cancellation.
   * <p>
   * Waiting for the SEARCH to stop reporting them is not possible. Measured on 2026-09-24
   * against a cluster of the 8.9 line: three workflows carrying a Camunda-managed user task
   * were still answered as ACTIVE a minute after the engine had answered <code>404</code> for
   * each of them. A test which looks a workflow up by a variable therefore binds its search to
   * a process of its own instead of trusting that the answer belongs to it.
   * <p>
   * A child of a call activity is not cancelled: the engine refuses to cancel one, and
   * cancelling its parent ends it anyway, so it goes with the parent.
   */
  private static void cancelWhateverAnEarlierClassLeftRunning() {

    try (final var client = clientOfTheTest()) {
      final var deadline = System.currentTimeMillis() + THE_ENGINE_LETS_GO_WITHIN.toMillis();
      final var refusals = new LinkedHashMap<Long, String>();
      while (true) {
        final var running = stillRunning(client);
        if (running.isEmpty()) {
          return;
        }
        final var stillHeldByTheEngine = running
            .stream()
            .filter(workflow -> workflow.getParentProcessInstanceKey() == null)
            .filter(workflow -> cancel(client, workflow, refusals))
            .count();
        if (stillHeldByTheEngine == 0) {
          return;
        }
        if (System.currentTimeMillis() > deadline) {
          throw new IllegalStateException(
              ("The engine still holds %d workflow(s) of an earlier class %s after this class "
                  + "cancelled them, so the workers of this one would be served their jobs: %s")
                  .formatted(
                      stillHeldByTheEngine,
                      THE_ENGINE_LETS_GO_WITHIN,
                      whatIsLeft(running, refusals)));
        }
        pauseBeforeAskingAgain();
      }
    }

  }

  /**
   * Names what is left, so a red run says which models they are and what the engine answered
   * rather than only how many there were.
   *
   * @param running What the search still reports
   * @param refusals What the engine answered per instance, where it refused the cancellation
   * @return One line per instance
   */
  private static String whatIsLeft(
      final List<ProcessInstance> running,
      final Map<Long, String> refusals) {

    return running
        .stream()
        .map(workflow -> "%n  %d of '%s'%s".formatted(
            workflow.getProcessInstanceKey(),
            workflow.getProcessDefinitionId(),
            refusals.containsKey(workflow.getProcessInstanceKey())
                ? ", the engine refused to cancel it: "
                    + refusals.get(workflow.getProcessInstanceKey())
                : ", which the engine still held when it took the last cancellation"))
        .collect(Collectors.joining());

  }

  /**
   * How long the engine may take to let go of what this class cancelled. Generous rather than
   * measured: the ordinary round ends at once, and what this number decides is when a workflow
   * the engine will not release ends the class instead of being handed on.
   */
  private static final Duration THE_ENGINE_LETS_GO_WITHIN = Duration.ofSeconds(60);

  /**
   * How long an activation request of a client which was already closed can still be parked
   * at the cluster. Such a request is a long poll, closing the client does not cancel it, and
   * a job created while it is parked is activated into it and answered by nobody until its
   * lock expires.
   * <p>
   * Ten seconds is the client's default for <code>request-timeout</code>, and this module
   * lowers it in one class and raises it nowhere. A class which raises it raises this with it.
   */
  private static final Duration A_PARKED_REQUEST_LIVES_AT_MOST = Duration.ofSeconds(10);

  /**
   * Whether a class of this module has run in this fork already. The first one talks to a
   * cluster nobody has opened a worker against, so it has nothing to wait for.
   */
  private static boolean aClassHasRunBefore;

  /**
   * Waits until an activation request of the class before cannot be parked at the cluster any
   * more.
   * <p>
   * The drain of a workflow module waits for the cluster to release its workers, so an
   * ordinary shutdown leaves no such request. One which ran out of its grace does, and the
   * class after it is what pays: its first job is activated into that request and comes back
   * only when its lock expires. The cluster cannot be asked about it, and nothing but time
   * closes it, so this waits the window out.
   * <p>
   * The window is measured from the start of the cleanup above rather than from the moment
   * the class before closed its client, which is earlier: JUnit has run every callback of
   * that class by the time this class begins. So the wait is at least as long as it has to
   * be, and the cleanup above pays for the part of it which it took.
   *
   * @param theCleanupStartedAt When this class began taking the cluster over, in nanoseconds
   * @throws InterruptedException Where the wait is interrupted
   */
  private static void waitOutAnActivationRequestOfTheClassBefore(
      final long theCleanupStartedAt) throws InterruptedException {

    if (!aClassHasRunBefore) {
      aClassHasRunBefore = true;
      return;
    }
    final var left = A_PARKED_REQUEST_LIVES_AT_MOST
        .minusNanos(System.nanoTime() - theCleanupStartedAt);
    if (left.isPositive()) {
      Thread.sleep(left.toMillis());
    }

  }

  /**
   * What the cluster answers with, retried while it refuses to answer at all.
   * <p>
   * The class which starts the container is the one which meets a cluster whose search is
   * not up yet: it is ready to take a deployment before it is ready to be asked what it
   * holds. Every class after that meets a cluster which has been answering for a while, so a
   * refusal which outlasts the retries is a broken search rather than a cold one, and it
   * ends the class instead of leaving the workflows of an earlier one running.
   */
  private static List<ProcessInstance> stillRunning(
      final CamundaClient client) {

    final var deadline = System.currentTimeMillis() + SEARCHABLE_WITHIN.toMillis();
    while (true) {
      try {
        return client
            .newProcessInstanceSearchRequest()
            .filter(filter -> filter.state(ProcessInstanceState.ACTIVE))
            // one page for the whole module: a class leaves a handful of workflows behind,
            // and a second page would be a loop against a storage which is still filling up
            .page(page -> page.limit(1000))
            .send()
            .join()
            .items();
      } catch (final RuntimeException refused) {
        if (System.currentTimeMillis() > deadline) {
          throw refused;
        }
        pauseBeforeAskingAgain();
      }
    }

  }

  /**
   * @see #stillRunning(CamundaClient)
   */
  private static final Duration SEARCHABLE_WITHIN = Duration.ofSeconds(60);

  private static void pauseBeforeAskingAgain() {

    try {
      Thread.sleep(500);
    } catch (final InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for the cluster's search", interrupted);
    }

  }

  /**
   * Cancels one workflow and says whether the engine still had it.
   * <p>
   * A workflow which ended before this command is exactly what the caller wanted, and the
   * engine says so with a <code>404</code>. Every other refusal is kept for the message a
   * caller writes where it gives up, because a cancellation refused for some other reason is
   * what a red run has to be able to read.
   *
   * @param client The client of the test
   * @param workflow The workflow to cancel
   * @param refusals What the engine answered, per instance
   * @return Whether the engine still held this workflow
   */
  private static boolean cancel(
      final CamundaClient client,
      final ProcessInstance workflow,
      final Map<Long, String> refusals) {

    try {
      client
          .newCancelInstanceCommand(workflow.getProcessInstanceKey())
          .send()
          .join();
      refusals.remove(workflow.getProcessInstanceKey());
      return true;
    } catch (final RuntimeException refused) {
      refusals.put(workflow.getProcessInstanceKey(), refused.getMessage());
      return !Camunda8Errors.notFound(refused);
    }

  }

  /**
   * A client of the test's own: this runs while no application of the test is up.
   */
  private static CamundaClient clientOfTheTest() {

    return CamundaClient
        .newClientBuilder()
        .preferRestOverGrpc(true)
        .restAddress(URI.create(restAddress()))
        .grpcAddress(URI.create(grpcAddress()))
        .build();

  }

}
