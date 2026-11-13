package io.vanillabp.camunda8.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.enums.JobState;

/**
 * How many jobs of one BPMN process are open which this adapter would deliver to a
 * <code>&#64;WorkflowTask</code> method. The startup finding about open tasks without a
 * delivery record asks for this number.
 *
 * <h2>Two filters, and why each is needed</h2>
 *
 * The index of the cluster keeps a job after it is over. So a search by process alone answers
 * every job the process ever had, and the number never goes down. The open ones are found the
 * same way {@link Camunda8UnservedUserTaskJobs} finds them: all jobs, minus those in a state a
 * job does not leave again. A state this build has no literal for counts as open.
 * <p>
 * And not every job of the process is one this adapter delivers. The job of a user task without
 * <code>zeebe:userTask</code> goes to nobody here. Neither does the job of a connector, of an
 * element which something else serves, or of a user task with a job type of its own. A redelivery
 * of such a job can never run a method a second time. So the search names the job types this
 * adapter opens workers for, and nothing else.
 *
 * <h2>What the number is not</h2>
 *
 * An exporter feeds the index, so the index runs behind the engine. A job which ended a moment
 * ago can still be counted. The two searches are two readings as well. So the number is close,
 * not exact. It is asked once per BPMN process while the application starts.
 */
public final class Camunda8OpenJobsItDelivers {

  private Camunda8OpenJobsItDelivers() {
  }

  /**
   * Counts the open jobs of one process whose job type is one of the given ones.
   *
   * @param client The client of the adapter asking
   * @param scopedBpmnProcessId The process definition id as the cluster knows it
   * @param jobTypes The job types this adapter delivers for that process, as the cluster knows
   *          them
   * @return How many of those jobs are open, never below zero. Whatever the search threw is
   *         thrown on
   */
  public static long countFor(
      final CamundaClient client,
      final String scopedBpmnProcessId,
      final Collection<String> jobTypes) {

    if (jobTypes.isEmpty()) {
      return 0L;
    }
    final var types = List.copyOf(jobTypes);
    final var all = howManyJobs(client, scopedBpmnProcessId, types, null);
    if (all == 0L) {
      return 0L;
    }
    final var thoseWhichAreOver = howManyJobs(
        client, scopedBpmnProcessId, types, Camunda8UnservedUserTaskJobs.A_JOB_WHICH_IS_OVER);
    // two readings. A job which ended between them is in the second one only
    return Math.max(0L, all - thoseWhichAreOver);

  }

  private static long howManyJobs(
      final CamundaClient client,
      final String scopedBpmnProcessId,
      final List<String> jobTypes,
      final List<JobState> states) {

    final var found = client
        .newJobSearchRequest()
        .filter(filter -> {
          filter
              .processDefinitionId(scopedBpmnProcessId)
              .type(type -> type.in(new ArrayList<>(jobTypes)));
          if (states != null) {
            filter.state(state -> state.in(states));
          }
        })
        // the TOTAL and not the page which came back. Only one item is fetched, because
        // only the number is wanted
        .page(page -> page.limit(1))
        .send()
        .join();
    final var total = found.page().totalItems();
    return total == null
        ? 0L
        : total.longValue();

  }

}
