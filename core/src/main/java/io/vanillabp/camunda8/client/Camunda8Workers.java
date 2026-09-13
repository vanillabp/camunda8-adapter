package io.vanillabp.camunda8.client;

import io.camunda.client.api.worker.JobWorkerBuilderStep1;
import io.vanillabp.camunda8.observability.Camunda8Metrics;

/**
 * How a job worker of one Camunda 8 adapter id is set up, wherever it is opened.
 * <p>
 * Almost everything a worker of this adapter uses is set on the CLIENT while the client is
 * built - <code>max-jobs-active</code>, <code>poll-interval</code>,
 * <code>request-timeout</code>, <code>stream-enabled</code> - so a worker inherits it and an
 * environment variable can still overrule it and be reported for it. Two things cannot be
 * set there, and those are what this class adds: the stream timeout, which the client has no
 * equivalent for, and the job counters, which only exist per worker because they carry the
 * job type.
 * <p>
 * Public because an EXTENSION opens workers on the same cluster. A worker it assembled by
 * hand is missing from what an operator reads: the counters are what an operator looks at to
 * see how much work is queued in front of the execution slots, and a worker which does not
 * report them is not a quieter worker, it is an invisible one. Calling this is how a
 * worker of an extension looks like a worker of the adapter.
 */
public final class Camunda8Workers {

  private Camunda8Workers() {
  }

  /**
   * Applies the options every worker of an adapter id shares.
   * <p>
   * It only sets what the client does not carry, so it may be called on a builder the
   * caller has already configured and the caller keeps configuring the returned builder -
   * which is the same object.
   *
   * @param builder The worker builder, with its job type and its handler already named
   * @param adapterId The adapter id whose worker this is - the counters are reported under
   *          it, so an operator reads which of two configured clusters is busy
   * @param jobType The job type the worker subscribes to, which the counters carry
   * @param configuration The configuration of that adapter id, as the adapter resolved it
   *          ({@code Camunda8ClientFactory#getConfiguration()})
   * @param metrics Where the counters go, or {@link Camunda8Metrics#NONE} where the
   *          application brought no Micrometer
   * @return The same builder
   */
  public static JobWorkerBuilderStep1.JobWorkerBuilderStep3 applyWorkerOptions(
      final JobWorkerBuilderStep1.JobWorkerBuilderStep3 builder,
      final String adapterId,
      final String jobType,
      final Camunda8AdapterConfiguration configuration,
      final Camunda8Metrics metrics) {

    // the client's own counters: it activates and hands over jobs long before
    // the core sees a delivery, so this is where the queue in front of the execution
    // slots becomes visible
    final var withMetrics = builder.metrics(metrics.workerMetrics(adapterId, jobType));
    final var streamTimeout = configuration.getStreamTimeout();
    return streamTimeout == null
        ? withMetrics
        : withMetrics.streamTimeout(streamTimeout);

  }

}
