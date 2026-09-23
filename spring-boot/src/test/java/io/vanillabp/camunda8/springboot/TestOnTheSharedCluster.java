package io.vanillabp.camunda8.springboot;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.enums.ProcessInstanceState;
import io.camunda.client.api.search.response.ProcessInstance;
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
 * is still running.
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
   * Cancels every workflow which is still running on the shared cluster, so this class
   * starts on a cluster which does nothing.
   * <p>
   * It runs before the application of this class boots: JUnit calls it after the extensions
   * of the class and before the first test instance is built, which is when Spring loads the
   * test's context. A workflow left over by an earlier class would otherwise be served by
   * the workers of this one.
   * <p>
   * Only workflows the cluster ANSWERS WITH are cancelled, and the cluster answers out of
   * its secondary storage, which runs behind the engine by an unknown amount. That is enough
   * here because the application which started those workflows was shut down before this
   * class began, so what it started is old enough to have arrived. A child of a call
   * activity is left alone: the engine refuses to cancel one, and cancelling its parent ends
   * it anyway.
   */
  @BeforeAll
  static void cancelWhateverAnEarlierClassLeftRunning() {

    try (final var client = clientOfTheTest()) {
      stillRunning(client)
          .stream()
          .filter(workflow -> workflow.getParentProcessInstanceKey() == null)
          .forEach(workflow -> cancel(client, workflow));
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

  private static void cancel(
      final CamundaClient client,
      final ProcessInstance workflow) {

    try {
      client
          .newCancelInstanceCommand(workflow.getProcessInstanceKey())
          .send()
          .join();
    } catch (final RuntimeException refused) {
      // a workflow which ended between the search and this command is exactly what this
      // method wanted, so the refusal is the result rather than a failure
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
