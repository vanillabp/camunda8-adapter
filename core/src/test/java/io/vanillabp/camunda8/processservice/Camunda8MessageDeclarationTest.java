package io.vanillabp.camunda8.processservice;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.deployment.Camunda8DeployedProcesses;
import io.vanillabp.integration.spi.AggregatePersistenceAware;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Phase one of a message correlation asks the deployed models whether they
 * declare the message. A name no model knows would be published into the void - the
 * cluster buffers it until its time-to-live passes, so nothing correlates and nothing
 * fails.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8MessageDeclarationTest {

  private record Aggregate(Object id) {
  }

  private static AggregatePersistenceAware<Aggregate> persistence() {

    return new AggregatePersistenceAware<>() {

      @Override
      public Class<Aggregate> getAggregateClass() {
        return Aggregate.class;
      }

      @Override
      public Aggregate save(
          final Aggregate aggregate) {
        return aggregate;
      }

      @Override
      public Object getAggregateId(
          final Aggregate aggregate) {
        return aggregate.id();
      }

    };

  }

  private static BpmnModelInstance modelWaitingFor(
      final String messageName) {

    return Bpmn
        .createExecutableProcess("Process")
        .startEvent()
        .intermediateCatchEvent("wait")
        .message(message -> message.name(messageName))
        .endEvent()
        .done();

  }

  private static Camunda8ClientFactory clientFactory() {

    final var configuration = new Camunda8AdapterConfiguration();
    // never contacted: phase one asks the models, not the cluster
    configuration.setRestAddress("http://localhost:1");
    return new Camunda8ClientFactory("c8", configuration);

  }

  private static Camunda8ProcessService<Aggregate> serviceOf(
      final Camunda8ClientFactory clientFactory) {

    return new Camunda8ProcessService<>(
        "c8", clientFactory, java.time.Duration.ofDays(14), (
            aggregateClass,
            check) -> check.run(), null, java.time.Duration.ZERO);

  }

  private static void deploy(
      final Camunda8ClientFactory clientFactory,
      final String workflowModuleId,
      final BpmnModelInstance model) {

    clientFactory
        .getDeployedProcesses()
        .record(
            new Camunda8DeployedProcesses.DeployedProcess(
                workflowModuleId, "Process", "2251799813685249", 1, model));

  }

  @Test
  @DisplayName("A message the model declares passes phase one")
  public void aDeclaredMessagePasses() {

    final var clientFactory = clientFactory();
    deploy(clientFactory, "module", modelWaitingFor("PaymentReceived"));

    assertDoesNotThrow(
        () -> PhaseOperations.phaseOne(serviceOf(clientFactory),
            io.vanillabp.integration.spi.PhaseOperation.CORRELATE_MESSAGE, "module", "Process", persistence(),
            new Aggregate("agg-1"), PhaseOperations.args(io.vanillabp.integration.spi.PhaseTwoCall.ARG_MESSAGE_NAME,
                "PaymentReceived", io.vanillabp.integration.spi.PhaseTwoCall.ARG_CORRELATION_ID, null)));

  }

  @Test
  @DisplayName("A message no model declares fails where the application called, naming the declared ones")
  public void anUndeclaredMessageFailsInPhaseOne() {

    final var clientFactory = clientFactory();
    deploy(clientFactory, "module", modelWaitingFor("PaymentReceived"));

    final var failure = assertThrows(
        IllegalArgumentException.class,
        () -> PhaseOperations.phaseOne(serviceOf(clientFactory),
            io.vanillabp.integration.spi.PhaseOperation.CORRELATE_MESSAGE, "module", "Process", persistence(),
            new Aggregate("agg-1"), PhaseOperations.args(io.vanillabp.integration.spi.PhaseTwoCall.ARG_MESSAGE_NAME,
                "PaymentRecieved", io.vanillabp.integration.spi.PhaseTwoCall.ARG_CORRELATION_ID, null)));

    assertTrue(failure.getMessage().contains("PaymentRecieved"), failure.getMessage());
    // the remedy: what IS declared
    assertTrue(failure.getMessage().contains("PaymentReceived"), failure.getMessage());

  }

  @Test
  @DisplayName("Without a process deployed by this application version the check stays silent")
  public void withoutDeployedProcessesTheCheckIsSilent() {

    final var clientFactory = clientFactory();
    // a workflow still running on a definition of a PREVIOUS application version: the
    // declared names are unknown here rather than absent, so correlating must not fail
    deploy(clientFactory, "another-module", modelWaitingFor("PaymentReceived"));

    assertDoesNotThrow(
        () -> PhaseOperations.phaseOne(serviceOf(clientFactory),
            io.vanillabp.integration.spi.PhaseOperation.CORRELATE_MESSAGE, "module", "Process", persistence(),
            new Aggregate("agg-1"), PhaseOperations.args(io.vanillabp.integration.spi.PhaseTwoCall.ARG_MESSAGE_NAME,
                "AnyMessage", io.vanillabp.integration.spi.PhaseTwoCall.ARG_CORRELATION_ID, null)));

  }

  @Test
  @DisplayName("Messages of every deployed process of the module count, not only the calling one")
  public void everyProcessOfTheModuleCounts() {

    final var clientFactory = clientFactory();
    deploy(clientFactory, "module", modelWaitingFor("PaymentReceived"));
    clientFactory
        .getDeployedProcesses()
        .record(
            new Camunda8DeployedProcesses.DeployedProcess(
                "module", "CalledProcess", "2251799813685250", 1, modelWaitingFor("DocumentsArrived")));

    // the message waits in a called process - same aggregate, other BPMN process
    assertDoesNotThrow(
        () -> PhaseOperations.phaseOne(serviceOf(clientFactory),
            io.vanillabp.integration.spi.PhaseOperation.CORRELATE_MESSAGE, "module", "Process", persistence(),
            new Aggregate("agg-1"), PhaseOperations.args(io.vanillabp.integration.spi.PhaseTwoCall.ARG_MESSAGE_NAME,
                "DocumentsArrived", io.vanillabp.integration.spi.PhaseTwoCall.ARG_CORRELATION_ID, null)));

  }

}
