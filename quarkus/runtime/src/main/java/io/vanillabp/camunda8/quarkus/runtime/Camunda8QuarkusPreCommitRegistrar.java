package io.vanillabp.camunda8.quarkus.runtime;

import io.vanillabp.camunda8.processservice.Camunda8PreCommitRegistrar;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;

/**
 * Quarkus (JTA) implementation of the pre-commit hook used for phase-one
 * existence checks: registers an interposed {@link Synchronization} whose
 * {@code beforeCompletion} runs the check right before the commit (throwing
 * aborts the commit). Without an active transaction the check runs immediately.
 */
public class Camunda8QuarkusPreCommitRegistrar implements Camunda8PreCommitRegistrar {

  private final TransactionSynchronizationRegistry synchronizationRegistry;

  public Camunda8QuarkusPreCommitRegistrar(
      final TransactionSynchronizationRegistry synchronizationRegistry) {

    this.synchronizationRegistry = synchronizationRegistry;

  }

  @Override
  public void beforeCommit(
      final Runnable check) {

    if (synchronizationRegistry.getTransactionStatus() != Status.STATUS_ACTIVE) {
      check.run();
      return;
    }
    synchronizationRegistry.registerInterposedSynchronization(new Synchronization() {

      @Override
      public void beforeCompletion() {

        check.run();

      }

      @Override
      public void afterCompletion(
          final int status) {
      }

    });

  }

}
