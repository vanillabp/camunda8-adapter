package io.vanillabp.camunda8.springboot;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import io.vanillabp.camunda8.processservice.Camunda8PreCommitRegistrar;

/**
 * Spring implementation of the pre-commit hook used for phase-one existence
 * checks: registers a {@link TransactionSynchronization} whose
 * {@code beforeCommit} runs the check right before the commit (throwing aborts
 * the commit). Without transaction synchronization the check runs immediately.
 */
public class Camunda8SpringPreCommitRegistrar implements Camunda8PreCommitRegistrar {

  @Override
  public void beforeCommit(
      final Runnable check) {

    if (!TransactionSynchronizationManager.isSynchronizationActive()) {
      check.run();
      return;
    }
    TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {

      @Override
      public void beforeCommit(
          final boolean readOnly) {

        check.run();

      }

    });

  }

}
