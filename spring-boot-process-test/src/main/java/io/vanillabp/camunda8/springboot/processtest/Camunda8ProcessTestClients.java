package io.vanillabp.camunda8.springboot.processtest;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.event.EventListener;

import io.camunda.client.spring.event.CamundaClientClosingSpringEvent;
import io.camunda.client.spring.event.CamundaClientCreatedSpringEvent;
import io.camunda.process.test.api.CamundaProcessTestExecutionListener;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.client.Camunda8TestRuntimeConnection;

/**
 * Connects the Camunda 8 adapter to the cluster Camunda Process Test hands over before each
 * test method, and takes it off again after the method.
 * <p>
 * Camunda Process Test publishes a {@link CamundaClientCreatedSpringEvent} before each test
 * method, with a client it built against its cluster. The adapter takes the address and the
 * credentials of that client and builds its own client with them. Then it deploys every
 * workflow module and opens the workers, still before the test method starts. After the
 * method, Camunda Process Test publishes a {@link CamundaClientClosingSpringEvent} and then
 * deletes everything in its cluster. The adapter closes its workers and its client while that
 * event is handled, so no worker polls a cluster which is being emptied.
 * <p>
 * Only the events which Camunda Process Test publishes count. Camunda's Spring starter
 * publishes the same events for a client of its own, and that client is not the cluster of
 * the test.
 */
public class Camunda8ProcessTestClients {

  /**
   * The listener of Camunda Process Test which starts the cluster and publishes the events.
   */
  static final String EXECUTION_LISTENER = "io.camunda.process.test.api.CamundaProcessTestExecutionListener";

  private final ObjectProvider<Camunda8ClientFactoryRegistry> registry;

  /**
   * Built by {@link Camunda8ProcessTestAutoConfiguration}.
   *
   * @param registry The clients of every Camunda 8 adapter id of the application
   */
  public Camunda8ProcessTestClients(
      final ObjectProvider<Camunda8ClientFactoryRegistry> registry) {

    this.registry = registry;

  }

  /**
   * Connects every Camunda 8 adapter id to the cluster of the test method which is about to
   * start. A deployment which fails here fails that test method, with the message of the
   * adapter.
   *
   * @param event What Camunda Process Test published, with its client
   */
  @EventListener
  public void connectToTheClusterOfTheTestMethod(
      final CamundaClientCreatedSpringEvent event) {

    if (!publishedByCamundaProcessTest(event.getSource())) {
      return;
    }
    final var connection = Camunda8TestRuntimeConnection.of(event.getClient().getConfiguration());
    registry.ifAvailable(clients -> clients.connectToTheTestRuntime(connection));

  }

  /**
   * Takes every Camunda 8 adapter id off the cluster of the test method which just ended.
   *
   * @param event What Camunda Process Test published before it deletes the data of its cluster
   */
  @EventListener
  public void disconnectFromTheClusterOfTheTestMethod(
      final CamundaClientClosingSpringEvent event) {

    if (!publishedByCamundaProcessTest(event.getSource())) {
      return;
    }
    registry.ifAvailable(Camunda8ClientFactoryRegistry::disconnectFromTheTestRuntime);

  }

  /**
   * Whether an event comes from the listener of Camunda Process Test.
   */
  private static boolean publishedByCamundaProcessTest(
      final Object source) {

    return source instanceof CamundaProcessTestExecutionListener;

  }

}
