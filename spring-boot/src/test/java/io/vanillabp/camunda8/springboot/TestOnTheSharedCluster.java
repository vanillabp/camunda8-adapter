package io.vanillabp.camunda8.springboot;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.enums.ProcessInstanceState;
import io.camunda.client.api.search.enums.UserTaskState;
import io.camunda.client.api.search.response.ProcessInstance;
import io.camunda.client.api.search.response.UserTask;
import io.vanillabp.camunda8.client.Camunda8Errors;
import io.vanillabp.camunda8.client.Camunda8JobLease;
import io.vanillabp.camunda8.test.ClusterUnderTest;
import io.vanillabp.camunda8.wiring.Camunda8TaskWiring;

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
 * aggregate their own database never held. That is why every class starts by ending what is
 * still running.
 * <p>
 * What a class reads and what it cancels are two different things. The workflows come from the
 * search, which answers out of the secondary storage, and the cancellation goes to the engine.
 * A class which only sent its cancellations went on while the engine still held them, and that
 * cost a whole nightly run.
 * <p>
 * A cancellation the engine took is not an instance which ended, and that is the second half of
 * the cleanup. An instance carrying a Camunda-managed user task waits for the
 * <code>canceling</code> listener job of that task before it terminates, and while no
 * application runs nobody answers that job. Measured against a cluster of the 8.9 line on
 * 2026-09-25: the engine answered <code>404</code> to a second cancellation 24 ms after the
 * first one, the instance was still reported as running 130 seconds later, and it ended 0,5
 * seconds after a worker took the listener job. That is where the "a minute after the
 * cancellation" of the earlier measurement came from - not from a search lagging behind. So
 * this cleanup answers those listener jobs itself, and only then is the cluster free of what
 * the class before it left.
 * <p>
 * The search is still the slower of the two, by about a second. A test which looks a workflow
 * up by a variable therefore binds its search to a process of its own, rather than trusting
 * that what comes back belongs to it.
 * <p>
 * Two kinds of test keep a cluster of their own, and each of them says so where it stands.
 * One needs the cluster CONFIGURED differently, with authentication switched on, which makes
 * it a different thing under test. The other needs a cluster which has never seen its model,
 * because the cluster acts on a deployment once and never again: a timer start event fires
 * for the first deployment of its version and for no later one. Both declare a
 * {@code @Container} field, the way every class here did before.
 * <p>
 * One rule holds for the preview line only, and the cleanup below checks it: no test on this
 * cluster may create a Camunda-managed user task there. On that line such a task can never be
 * ended. Its <code>creating</code> listener job is never handed out, a cancellation leaves it
 * in <code>CANCELING</code>, and the listener job of that task stays activatable and takes
 * down every later activation of the same job type. A test which needs a user task on that
 * line brings a cluster of its own, the way {@code Camunda8GrpcTransportIT} does. See
 * decision 43 in the repository's DECISIONS.md.
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
   * @param whichClassThisIs The class taking the cluster over, for the message a cleanup
   *          which cannot finish writes about the class before it
   * @throws InterruptedException Where the wait is interrupted
   */
  @BeforeAll
  static void nothingOfTheClassBeforeReachesThisOne(
      final TestInfo whichClassThisIs) throws InterruptedException {

    final var startedAt = System.nanoTime();
    endWhateverAnEarlierClassLeftRunning();
    classWhichRanBefore = whichClassThisIs
        .getTestClass()
        .map(Class::getSimpleName)
        .orElse("the class before");
    waitOutAnActivationRequestOfTheClassBefore(startedAt);

  }

  /**
   * The class whose leftovers the next cleanup takes away, so a cleanup which cannot finish
   * names it instead of leaving the reader to work out the order of the classes.
   */
  private static String classWhichRanBefore = "the class before";

  /**
   * Why the cluster can serve no further class, once one cleanup has found that out. Every
   * class after that fails at once with the same sentence rather than sitting out its own
   * minute for an answer which cannot change.
   */
  private static String whyTheClusterCannotBeHandedOver;

  /**
   * Ends what an earlier class left running, until the cluster holds none of it any more.
   * <p>
   * Two things have to be true afterwards, and they end at different moments. The engine must
   * hold no workflow of the class before: one it does not hold can hand no job to a worker of
   * this class, and it says so with a <code>404</code> to the cancellation. And no user task
   * may be left between two of its states, because such a task holds a listener job which is
   * activatable, and the next worker of that job type would be served it.
   * <p>
   * The second one is also what a cancellation waits for. An instance carrying a
   * Camunda-managed user task terminates once the <code>canceling</code> listener job of that
   * task is answered, and while no application runs nobody answers it. So this answers those
   * jobs itself.
   * <p>
   * A child of a call activity is not cancelled: the engine refuses to cancel one, and
   * cancelling its parent ends it anyway, so it goes with the parent.
   */
  private static void endWhateverAnEarlierClassLeftRunning() {

    if (whyTheClusterCannotBeHandedOver != null) {
      throw new IllegalStateException(whyTheClusterCannotBeHandedOver);
    }
    try (final var client = clientOfTheTest()) {
      final var deadline = System.currentTimeMillis() + THE_ENGINE_LETS_GO_WITHIN.toMillis();
      final var refusals = new LinkedHashMap<Long, String>();
      while (true) {
        final var running = stillRunning(client);
        final var stillHeldByTheEngine = running
            .stream()
            .filter(workflow -> workflow.getParentProcessInstanceKey() == null)
            .filter(workflow -> cancel(client, workflow, refusals))
            .count();
        final var waitingForAListenerJob = userTasksBetweenTwoStates(client);
        if ((stillHeldByTheEngine == 0) && waitingForAListenerJob.isEmpty()) {
          return;
        }
        answerTheListenerJobsTheseTasksWaitFor(client, waitingForAListenerJob);
        if (System.currentTimeMillis() > deadline) {
          whyTheClusterCannotBeHandedOver = whyItCouldNotBeEnded(
              running, refusals, stillHeldByTheEngine, waitingForAListenerJob);
          throw new IllegalStateException(whyTheClusterCannotBeHandedOver);
        }
        pauseBeforeAskingAgain();
      }
    }

  }

  /**
   * The user tasks the cluster holds between two of their states.
   * <p>
   * Each of them waits for a listener job of this adapter's own, and each of those jobs is
   * activatable until somebody answers it. No application runs while this is asked, so every
   * one of them belongs to a class which is over.
   *
   * @param client The client of the test
   * @return What the cluster still has to be told about
   */
  private static List<UserTask> userTasksBetweenTwoStates(
      final CamundaClient client) {

    return whatTheClusterAnswers(
        () -> client
            .newUserTaskSearchRequest()
            .filter(filter -> filter
                .state(state -> state.in(UserTaskState.CREATING, UserTaskState.CANCELING)))
            // one page for the whole module: a class leaves a handful of user tasks behind,
            // and a second page would be a loop against a storage which is still filling up
            .page(page -> page.limit(1000))
            .send()
            .join()
            .items());

  }

  /**
   * Answers the listener jobs those tasks wait for, the way the worker of the class before
   * would have done.
   * <p>
   * The job type is the one the deployment wrote into the model: this adapter's prefix and the
   * external form reference of the task. A task without such a reference was deployed by
   * version 1's convention, which this module never does. It is left alone rather than guessed
   * at, and it is then still there when the cleanup gives up, which is where it gets named.
   *
   * @param client The client of the test
   * @param tasks What {@link #userTasksBetweenTwoStates(CamundaClient)} found
   */
  private static void answerTheListenerJobsTheseTasksWaitFor(
      final CamundaClient client,
      final List<UserTask> tasks) {

    tasks
        .stream()
        .map(UserTask::getExternalFormReference)
        .filter(reference -> (reference != null) && !reference.isBlank())
        .distinct()
        .forEach(reference -> client
            .newActivateJobsCommand()
            .jobType(Camunda8TaskWiring.TASKDEFINITION_USERTASK_ZEEBE + reference)
            .maxJobsToActivate(AS_MANY_AS_A_CLASS_CAN_LEAVE)
            .timeout(THE_CLEANUP_HOLDS_A_JOB_FOR)
            .workerName("the class taking the cluster over")
            .requestTimeout(ASKING_FOR_A_LEFTOVER_JOB_ANSWERS_WITHIN)
            .send()
            .join()
            .getJobs()
            .forEach(job -> Camunda8JobLease
                .withToken(client.newCompleteCommand(job.getKey()), Camunda8JobLease.tokenOf(job))
                .send()
                .join()));

  }

  /**
   * @see #answerTheListenerJobsTheseTasksWaitFor(CamundaClient, List)
   */
  private static final int AS_MANY_AS_A_CLASS_CAN_LEAVE = 100;

  /**
   * @see #answerTheListenerJobsTheseTasksWaitFor(CamundaClient, List)
   */
  private static final Duration THE_CLEANUP_HOLDS_A_JOB_FOR = Duration.ofSeconds(10);

  /**
   * How long the cleanup waits for a job which the search says is there. Short, because the
   * job exists before the request is sent and a cluster which does not hand it over at once
   * will not hand it over later either.
   */
  private static final Duration ASKING_FOR_A_LEFTOVER_JOB_ANSWERS_WITHIN = Duration.ofSeconds(2);

  /**
   * What a cleanup says when it gives up, in the words of whichever half of it did not finish.
   *
   * @param running What the search still reports
   * @param refusals What the engine answered per instance, where it refused the cancellation
   * @param stillHeldByTheEngine How many workflows the engine still held
   * @param waitingForAListenerJob The user tasks which are still between two states
   * @return The sentence a reader of a red run gets
   */
  private static String whyItCouldNotBeEnded(
      final List<ProcessInstance> running,
      final Map<Long, String> refusals,
      final long stillHeldByTheEngine,
      final List<UserTask> waitingForAListenerJob) {

    if (!waitingForAListenerJob.isEmpty()) {
      return ("%d user task(s) which %s left are still waiting for a listener job %s after this "
          + "class answered them, so the first worker of that job type in this class would be "
          + "served one of those jobs: %s%nOn the preview line this is what the tag "
          + "'user-task-listener-jobs' is for: a test which creates a Camunda-managed user task "
          + "there leaves one which can never be ended, and it either carries that tag or brings "
          + "a cluster of its own.")
          .formatted(
              waitingForAListenerJob.size(),
              classWhichRanBefore,
              THE_ENGINE_LETS_GO_WITHIN,
              whichTasksAreLeft(waitingForAListenerJob));
    }
    return ("The engine still holds %d workflow(s) of %s %s after this class cancelled them, so "
        + "the workers of this one would be served their jobs: %s")
        .formatted(
            stillHeldByTheEngine,
            classWhichRanBefore,
            THE_ENGINE_LETS_GO_WITHIN,
            whatIsLeft(running, refusals));

  }

  /**
   * Names the user tasks a cleanup could not get rid of, with the state each of them is in.
   *
   * @param tasks What is left
   * @return One line per user task
   */
  private static String whichTasksAreLeft(
      final List<UserTask> tasks) {

    return tasks
        .stream()
        .map(task -> "%n  '%s' of '%s' in %s, instance %d".formatted(
            task.getElementId(),
            task.getBpmnProcessId(),
            task.getState(),
            task.getProcessInstanceKey()))
        .collect(Collectors.joining());

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
   * How long the cleanup may take to get the cluster free of the class before. Generous rather
   * than measured: the ordinary round ends at once, and what this number decides is when a
   * leftover nothing can end fails the class instead of being handed on.
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
   * The workflows the cluster still reports as running.
   *
   * @param client The client of the test
   * @return What an earlier class left, as the search sees it
   */
  private static List<ProcessInstance> stillRunning(
      final CamundaClient client) {

    return whatTheClusterAnswers(
        () -> client
            .newProcessInstanceSearchRequest()
            .filter(filter -> filter.state(ProcessInstanceState.ACTIVE))
            // one page for the whole module: a class leaves a handful of workflows behind,
            // and a second page would be a loop against a storage which is still filling up
            .page(page -> page.limit(1000))
            .send()
            .join()
            .items());

  }

  /**
   * What the cluster answers with, retried while it refuses to answer at all.
   * <p>
   * The class which starts the container is the one which meets a cluster whose search is
   * not up yet: it is ready to take a deployment before it is ready to be asked what it
   * holds. Every class after that meets a cluster which has been answering for a while, so a
   * refusal which outlasts the retries is a broken search rather than a cold one, and it
   * ends the class instead of leaving the workflows of an earlier one running.
   *
   * @param <T> What the search brings back
   * @param search The search to run
   * @return Its answer
   */
  private static <T> T whatTheClusterAnswers(
      final Supplier<T> search) {

    final var deadline = System.currentTimeMillis() + SEARCHABLE_WITHIN.toMillis();
    while (true) {
      try {
        return search.get();
      } catch (final RuntimeException refused) {
        if (System.currentTimeMillis() > deadline) {
          throw refused;
        }
        pauseBeforeAskingAgain();
      }
    }

  }

  /**
   * @see #whatTheClusterAnswers(Supplier)
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
