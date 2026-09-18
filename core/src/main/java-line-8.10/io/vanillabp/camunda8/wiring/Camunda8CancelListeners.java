package io.vanillabp.camunda8.wiring;

import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.search.enums.JobKind;
import io.camunda.client.api.search.enums.ListenerEventType;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListener;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListenerEventType;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeExecutionListeners;

/**
 * How an element of this release line says that it was canceled. This is the 8.10 variant.
 * <p>
 * 8.10 is the first line with a {@code cancel} execution listener, so it is the first line on
 * which any element can report a cancellation. On 8.8 and 8.9 only a Camunda-managed user task
 * can, through its {@code canceling} task listener, and the variants of this class for those
 * lines say so instead.
 * <p>
 * Both halves live here because both would fail to compile on an older line:
 * {@code ZeebeExecutionListenerEventType.cancel} is what writes the listener into the model, and
 * {@code ListenerEventType.CANCEL} is what the job of such a listener reports.
 */
final class Camunda8CancelListeners {

  private Camunda8CancelListeners() {
  }

  /**
   * @return Whether an element other than a Camunda-managed user task can report its
   *         cancellation on this line
   */
  static boolean anyElementCanReportItsCancellation() {

    return true;

  }

  /**
   * Writes the cancel execution listener of one served listener into the model.
   *
   * @param listeners The execution listeners of the element
   * @param jobType The job type of the served listener, which is the job type of its
   *          cancellation as well
   */
  static void addCancelListener(
      final ZeebeExecutionListeners listeners,
      final String jobType) {

    final var cancelListener = listeners
        .getModelInstance()
        .newInstance(ZeebeExecutionListener.class);
    cancelListener.setEventType(ZeebeExecutionListenerEventType.cancel);
    cancelListener.setType(jobType);
    cancelListener.setRetries(Camunda8Listeners.CANCEL_LISTENER_RETRIES);
    listeners.addChildElement(cancelListener);

  }

  /**
   * @param job The listener job which arrived
   * @return Whether it is the cancellation of the element the listener sits on
   */
  static boolean isCancellationOfAnElement(
      final ActivatedJob job) {

    return (job.getKind() == JobKind.EXECUTION_LISTENER) && (job.getListenerEventType() == ListenerEventType.CANCEL);

  }

}
