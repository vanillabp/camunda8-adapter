package io.vanillabp.camunda8.processservice;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;

/**
 * The pre-commit shape of the phase-one existence check (story 22, the V1
 * refinement): {@code completeTaskPhaseOne}/{@code cancelTaskPhaseOne} do NOT
 * contact the cluster at method-call time - they only REGISTER the check, which
 * runs when the platform's transaction synchronization fires right before the
 * commit. This minimizes the window between check and phase-two dispatch (fewer
 * stale outbox entries). Proven without a cluster: the client points at a closed
 * port, so any contact raises - no exception until the hook runs, exception when
 * it runs.
 */
public class Camunda8PreCommitCheckTest {

  static class RecordingRegistrar implements Camunda8PreCommitRegistrar {

    final List<Runnable> registered = new ArrayList<>();

    @Override
    public void beforeCommit(
        final Runnable check) {

      registered.add(check);

    }

  }

  private final RecordingRegistrar registrar = new RecordingRegistrar();

  private Camunda8ProcessService<Object> service() {

    final var configuration = new Camunda8AdapterConfiguration();
    // closed port: every cluster contact raises immediately
    configuration.setRestAddress("http://localhost:1");
    return new Camunda8ProcessService<>(
        "c8", new Camunda8ClientFactory("c8", configuration), Duration.ofDays(14), registrar);

  }

  @Test
  @DisplayName("phase one only registers the check - the cluster is not contacted at method-call time")
  public void phaseOneRegistersWithoutContactingCluster() {

    final var service = service();

    assertDoesNotThrow(() -> service.completeTaskPhaseOne(
        "module", "Process", null, new Object(), "123"));
    assertDoesNotThrow(() -> service.cancelTaskPhaseOne(
        "module", "Process", null, new Object(), "124", "SOME_ERROR"));

    assertEquals(2, registrar.registered.size(), "one registered check per operation");

  }

  @Test
  @DisplayName("the registered check contacts the cluster when the synchronization fires (pre-commit)")
  public void checkContactsClusterWhenHookFires() {

    final var service = service();
    service.completeTaskPhaseOne("module", "Process", null, new Object(), "123");
    final var check = registrar.registered.getFirst();
    assertNotNull(check);

    // the closed port proves the contact happens HERE - and an infrastructure
    // failure aborts the commit (it propagates)
    assertThrows(Exception.class, check::run);

  }

}
