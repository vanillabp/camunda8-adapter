package io.vanillabp.camunda8.quarkus.nativeimage;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;

/**
 * The assertion of this module, written as the application's main: what has to work is
 * booting and then running a workflow, and an exit code says whether it did.
 * <p>
 * Building the image is the smaller half. The deployment of a workflow module parses
 * every BPMN file through the Camunda model API, modifies it, serializes it back and
 * sends it to the cluster - a path which reads message bundles, a schema and service
 * providers a native image does not carry unless the adapter's extension names them.
 * That path runs at every boot of every application, so the binary starting is what
 * proves the extension registers enough.
 * <p>
 * Starting a workflow afterwards adds the second half: the start goes through the
 * phase-two outbox, reaches the cluster, and the cluster hands the job back to a
 * handler in this binary. Anything on that way either throws or leaves the latch
 * standing, and the exit code is what the pipeline looks at.
 */
@QuarkusMain
public class NativeImageApplication implements QuarkusApplication {

  /**
   * How long the job may take from the start of the workflow to the handler. A REST
   * round trip through a freshly booted cluster is a matter of seconds; the window is
   * wide because a loaded CI runner is slower than a laptop, and it is bounded because a
   * job which never arrives has to end this run rather than hang it.
   */
  private static final Duration UNTIL_SERVED = Duration.ofMinutes(2);

  @Inject
  NativeImageWorkflowService workflowService;

  @Inject
  NativeImagePersistence persistence;

  @Override
  public int run(
      final String... args) throws Exception {

    final var aggregateId = QuarkusTransaction
        .requiringNew()
        .call(() -> workflowService
            .startWorkflow()
            .getId());

    if (!NativeImageWorkflowService.SERVED.await(UNTIL_SERVED.toSeconds(), TimeUnit.SECONDS)) {
      System.err.println(
          "The job of workflow aggregate '%d' did not reach its handler within %s!"
              .formatted(aggregateId, UNTIL_SERVED));
      return 1;
    }

    final var status = QuarkusTransaction
        .requiringNew()
        .call(() -> persistence
            .loadById(aggregateId)
            .getStatus());

    if (!"served".equals(status)) {
      System.err.println(
          "The handler ran but workflow aggregate '%d' reads status '%s' instead of 'served'!"
              .formatted(aggregateId, status));
      return 1;
    }
    System.out.println(
        "VanillaBP ran the workflow of aggregate '%d' on Camunda 8 (status '%s')."
            .formatted(aggregateId, status));
    return 0;

  }

}
