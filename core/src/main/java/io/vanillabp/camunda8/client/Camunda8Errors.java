package io.vanillabp.camunda8.client;

import io.camunda.client.api.command.ProblemException;

/**
 * Shared classification of Camunda 8 client errors: whether a job-based command
 * failed because the job is GONE (already completed, canceled by a boundary event,
 * or the workflow moved on) - the at-least-once residual tolerated by completions
 * and mapped to UNKNOWN_TO_BPMS by awareness probes. Everything else is treated as
 * an infrastructure failure.
 */
public final class Camunda8Errors {

  private Camunda8Errors() {
  }

  /**
   * Whether the given failure means "this job does not exist (anymore)".
   *
   * @param throwable The failure of a job-based command
   * @return Whether the job is gone
   */
  public static boolean jobAlreadyGone(
      final Throwable throwable) {

    var current = throwable;
    while (current != null) {
      if (current instanceof ProblemException problem && (problem.details() != null) && (problem.details()
          .getStatus() == 404)) {
        return true;
      }
      final var message = current.getMessage();
      if ((message != null) && (message.contains("NOT_FOUND") || message.contains("was not found"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;

  }

}
