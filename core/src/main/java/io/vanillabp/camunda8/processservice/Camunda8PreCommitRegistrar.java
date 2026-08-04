package io.vanillabp.camunda8.processservice;

/**
 * Registers a check to run RIGHT BEFORE the caller's transaction commits -
 * supplied by the platform module (Spring: a {@code TransactionSynchronization},
 * Quarkus: an interposed JTA synchronization). Used for the phase-one existence
 * check of task completions: running the non-advancing check as late as possible
 * minimizes the window between check and phase two and therefore the number of
 * stale outbox entries (the V1 refinement). A throwing check aborts the commit.
 */
@FunctionalInterface
public interface Camunda8PreCommitRegistrar {

  /**
   * Runs the given check right before the current transaction commits. Without an
   * active transaction the check runs immediately.
   *
   * @param check The check to run (throws to abort the commit)
   */
  void beforeCommit(
      Runnable check);

}
