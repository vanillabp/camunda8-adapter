package io.vanillabp.camunda8.wiring;

import java.time.Duration;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.camunda.client.CamundaClient;
import io.vanillabp.camunda8.client.Camunda8Errors;
import io.vanillabp.integration.adapter.spi.workflowtask.TaskExistence;
import io.vanillabp.integration.adapter.spi.workflowtask.TaskInvocationContext;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskInvoker;
import lombok.extern.slf4j.Slf4j;

/**
 * Asks the cluster whether the other tasks of a workflow are still there, every time it hands
 * this application a job.
 * <p>
 * Zeebe tells a worker nothing about a job it took away, so an application on this BPMS learns
 * nothing on its own: an asynchronous task whose element an interrupting boundary event
 * removed stays open in VanillaBP's own record forever. The core drives the check - the list,
 * the budget, the transaction and the <code>CANCELED</code> delivery are all its business -
 * and this class answers the one question an adapter can answer, for one task at a time.
 * <p>
 * It is called from the three handlers which are a wake-up of a workflow: the job handler,
 * after the outcome went back to the cluster, and the two listener handlers, after their
 * notification. Not from the handler which reports the end of a workflow, because the end is a
 * stronger statement than a job and the core already derives the same cancelations from it,
 * and not from the handler which starts an instance, because an instance which has just
 * started has nothing else open.
 * <p>
 * <b>The probe.</b>
 * <code>UpdateJobTimeout</code> for the task the record names, which is the command the adapter
 * sends for every open asynchronous task anyway. A 404 or a gRPC <code>NOT_FOUND</code> is
 * gone. Everything else is "cannot say", including the 400 which says the cluster holds the job
 * and nobody has it activated right now: that answer means the task is alive.
 * <p>
 * It is deliberately not {@code Camunda8ProcessService#awarenessOfTask}. That one asks which
 * scope a key belongs to first, which is a query-API round trip on a shared cluster and which
 * this check does not need - the record names the adapter which delivered the task, and the
 * core drops a record of another adapter before anything is sent. And it folds two answers into
 * one: its <code>UNKNOWN_TO_BPMS</code> means both "the cluster says 404" and "this is not my
 * adapter's task", and reading the second as a cancelation would report a living task as
 * canceled.
 * <p>
 * <b>A user task is not a job.</b>
 * A Camunda-managed user task is delivered through its listener job, and the id the record
 * keeps for it is the USER-TASK key. Handed to a job command that key answers
 * <code>NOT_FOUND</code> for as long as the task is open, which read as gone would cancel a
 * task the cluster is holding out to somebody. The probe of the SPI names the task and not
 * what kind of task it is, so the two cannot be told apart from the arguments, and the
 * records of one wake-up all belong to the ONE process instance the workflow id names.
 * <p>
 * So the BPMN process decides. Where its model carries no Camunda-managed user task, a
 * <code>NOT_FOUND</code> is the whole answer and the task is gone. Where it carries one,
 * every <code>NOT_FOUND</code> of that process is answered with
 * {@link TaskExistence#CANNOT_SAY}: the check then reports nothing for that process, which
 * is what being honest costs here and what keeps it from reporting a living user task as
 * canceled. Such a workflow still hears about a task which went away, through the end of the
 * workflow and through the next operation which names the task.
 * <p>
 * The user tasks themselves need no probe at all. VanillaBP writes a <code>canceling</code>
 * task listener next to every user task it manages, and the cluster delivers
 * <code>CANCELED</code> for it straight from there.
 */
@Slf4j
public class Camunda8OpenTaskProbe {

  private final String adapterId;

  private final String workflowModuleId;

  private final WorkflowTaskInvoker workflowTaskInvoker;

  /**
   * The client of this workflow module, asked for the client rather than holding one: the
   * workers are opened with the client of the run, and a probe must use the same one.
   */
  private final Supplier<CamundaClient> client;

  /**
   * What the probe writes as the job's new deadline, which is the same value every other
   * <code>UpdateJobTimeout</code> of this adapter writes. The command moves the deadline of a
   * job somebody else is holding, so a value of its own would make a probe shorten or extend
   * a lock behind that holder's back.
   */
  private final Duration asyncTaskLockRenewal;

  /**
   * Whether the model of a BPMN process carries a Camunda-managed user task, asked by the
   * PLAIN process id. It decides whether a <code>NOT_FOUND</code> from a job command is the
   * whole answer for a task of that process.
   */
  private final Predicate<String> theProcessHasCamundaManagedUserTasks;

  public Camunda8OpenTaskProbe(
      final String adapterId,
      final String workflowModuleId,
      final WorkflowTaskInvoker workflowTaskInvoker,
      final Supplier<CamundaClient> client,
      final Duration asyncTaskLockRenewal,
      final Predicate<String> theProcessHasCamundaManagedUserTasks) {

    this.adapterId = adapterId;
    this.workflowModuleId = workflowModuleId;
    this.workflowTaskInvoker = workflowTaskInvoker;
    this.client = client;
    this.asyncTaskLockRenewal = asyncTaskLockRenewal;
    this.theProcessHasCamundaManagedUserTasks = theProcessHasCamundaManagedUserTasks;

  }

  /**
   * Hands the core the wake-up it just served, so it can look at the other tasks it believes
   * are open in the same workflow.
   * <p>
   * Nothing here may cost the delivery which led to it. The job is answered by the time this
   * runs, and a check which threw would otherwise turn a finished job into a failed one, so
   * every failure ends here with one line.
   *
   * @param bpmnProcessId The BPMN process of the delivery, as the application knows it
   * @param wakeUp The context of that delivery
   */
  public void reportWhatTheClusterNoLongerHas(
      final String bpmnProcessId,
      final TaskInvocationContext wakeUp) {

    if ((workflowTaskInvoker == null) || (wakeUp == null)) {
      return;
    }
    try {
      // every record the core asks about belongs to the one process instance the workflow
      // id names, so they are all tasks of the BPMN process of this wake-up
      final var theProcessHasUserTasks = theProcessHasCamundaManagedUserTasks.test(bpmnProcessId);
      workflowTaskInvoker
          .reportTasksTheBpmsNoLongerHas(workflowModuleId, bpmnProcessId, wakeUp, (
              workflowId,
              taskId) -> stillExists(taskId, theProcessHasUserTasks));
    } catch (final RuntimeException e) {
      log
          .debug(
              "Camunda8[{}]: looking at the other open tasks of the workflow of BPMN process '{}' "
                  + "failed - the delivery which led here is done either way, and the next wake-up "
                  + "of that workflow looks again",
              adapterId,
              bpmnProcessId,
              e);
    }

  }

  /**
   * Whether the cluster still has the task of that id.
   *
   * @param taskId The job key respectively the user-task key the delivery record kept
   * @param theProcessHasUserTasks Whether the BPMN process of that task carries a
   *          Camunda-managed user task, which is what makes a <code>NOT_FOUND</code>
   *          ambiguous
   * @return What the cluster said
   */
  TaskExistence stillExists(
      final String taskId,
      final boolean theProcessHasUserTasks) {

    if (taskId == null) {
      return TaskExistence.CANNOT_SAY;
    }
    final long key;
    try {
      key = Long.parseLong(taskId);
    } catch (final NumberFormatException e) {
      // a key of this cluster is a number, so a record naming something else was written
      // by another BPMS and the core should not have asked - saying so is still cheaper
      // than guessing
      return TaskExistence.CANNOT_SAY;
    }
    try {
      client
          .get()
          .newUpdateTimeoutCommand(key)
          .timeout(asyncTaskLockRenewal)
          .send()
          .join();
      return TaskExistence.STILL_THERE;
    } catch (final Exception e) {
      if (Camunda8Errors.jobIsThereButNotActive(e)) {
        // the cluster holds the job and nobody has it activated right now, which is what
        // an asynchronous task whose lock ran out looks like
        return TaskExistence.STILL_THERE;
      }
      if (!Camunda8Errors.notFound(e)) {
        log
            .debug(
                "Camunda8[{}]: the cluster did not say whether it still has task '{}' - it answered {}",
                adapterId,
                taskId,
                Camunda8Errors.rejection(e));
        return TaskExistence.CANNOT_SAY;
      }
      if (theProcessHasUserTasks) {
        // the key may be the USER-TASK key of a task the cluster is holding open, and a
        // job command answers NOT_FOUND for one of those as well. Nothing in the question
        // tells the two apart, so this adapter says so rather than canceling a living task
        return TaskExistence.CANNOT_SAY;
      }
      return TaskExistence.GONE;
    }

  }

}
