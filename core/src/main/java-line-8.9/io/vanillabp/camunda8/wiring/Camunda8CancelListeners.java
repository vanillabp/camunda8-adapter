package io.vanillabp.camunda8.wiring;

import io.camunda.client.api.response.ActivatedJob;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners;
import io.vanillabp.camunda8.Camunda8ReleaseLine;

/**
 * How an element of this release line says that it was canceled. This is the 8.9 variant.
 * <p>
 * Only a Camunda-managed user task does, through its {@code canceling} task listener. A
 * {@code cancel} execution listener arrived with 8.10, so every other element is canceled without
 * a word here, and the boot of a workflow module whose listeners are served says that out loud
 * rather than leaving it to be found.
 * <p>
 * Both halves live here because both would fail to compile on this line:
 * {@code ZeebeExecutionListenerEventType.cancel} is what would write the listener into the
 * model, and {@code ListenerEventType.CANCEL} is what the job of such a listener would report.
 */
final class Camunda8CancelListeners {

  private Camunda8CancelListeners() {
  }

  /**
   * @return Whether an element other than a Camunda-managed user task can report its
   *         cancellation on this line
   */
  static boolean anyElementCanReportItsCancellation() {

    return false;

  }

  /**
   * Never called on this line: the caller asks
   * {@link #anyElementCanReportItsCancellation()} first. It is here so the shared code has one
   * shape on every line, and it throws rather than doing nothing, because a silent no-op would
   * turn a wiring mistake into a cancellation nobody ever hears.
   *
   * @param listeners The execution listeners of the element
   * @param jobType The job type of the served listener
   */
  static void addCancelListener(
      final ZeebeExecutionListeners listeners,
      final String jobType) {

    throw new IllegalStateException(
        ("Camunda8: a cancel execution listener for job type '%s' cannot be written on release line %s! "
            + "The construct arrived with 8.10. Ask Camunda8CancelListeners whether this line has it "
            + "before writing one.")
            .formatted(jobType, Camunda8ReleaseLine.id()));

  }

  /**
   * @param job The listener job which arrived
   * @return Whether it is the cancellation of the element the listener sits on, which no job of
   *         this line is
   */
  static boolean isCancellationOfAnElement(
      final ActivatedJob job) {

    return false;

  }

}
