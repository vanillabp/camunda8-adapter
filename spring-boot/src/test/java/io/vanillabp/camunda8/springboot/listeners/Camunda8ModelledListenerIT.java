package io.vanillabp.camunda8.springboot.listeners;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vanillabp.camunda8.springboot.it.ClusterUnderTest;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Whether a listener somebody modelled is really served on a real cluster while
 * {@code allow-listeners} is on for its process.
 * <p>
 * A cluster is what this needs and nothing else would do. The cluster creates a job per
 * listener, a worker has to be subscribed to the listener's job type for the workflow to move at
 * all, and the workflow module runs under {@code name-clash-avoidance: use-prefix} - so the job
 * type the cluster knows carries the module and the process, while the methods below are named
 * after what the modeller typed. Without a cluster nothing proves that the prefix and its way
 * back meet.
 * <p>
 * The scenario brings its own application, its own configuration file and its own resources
 * location, see {@link ListenerTestApplication}: a model carrying a listener does not deploy
 * without the key, so no other integration test of this module may see it.
 * <p>
 * The class is skipped when Docker is unavailable
 * ({@code @Testcontainers(disabledWithoutDocker = true)}).
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = ListenerTestApplication.class,
    properties = "spring.config.name=camunda8-listeners-it")
@DirtiesContext
public class Camunda8ModelledListenerIT {

  @Container
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.cluster();

  @DynamicPropertySource
  static void camunda8Properties(
      final DynamicPropertyRegistry registry) {

    registry
        .add(
            "vanillabp.adapters.c8.rest-address",
            () -> "http://"
                + CAMUNDA.getHost()
                + ":"
                + CAMUNDA.getMappedPort(8080));
    registry
        .add(
            "vanillabp.adapters.c8.grpc-address",
            () -> "http://"
                + CAMUNDA.getHost()
                + ":"
                + CAMUNDA.getMappedPort(26500));

  }

  @Autowired
  private ListenerDockerWorkflowService workflowService;

  @Autowired
  private ListenerDockerAggregateRepository repository;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Test
  @DisplayName("Both modelled listeners reach a method, and the workflow moves past them")
  public void bothListenersReachAMethod() throws Exception {

    final var aggregateId = transactionTemplate
        .execute(status -> workflowService.startWorkflow().getId());
    assertNotNull(aggregateId);

    final var deadline = System.currentTimeMillis() + 150_000;
    while (System.currentTimeMillis() < deadline) {
      final var aggregate = transactionTemplate
          .execute(status -> repository.findById(aggregateId).orElseThrow());
      if (aggregate.isTheWorkWasAudited() && aggregate.isTheOrderWasArchived()) {
        assertTrue(aggregate.isTheWorkWasDone(), "the ordinary task ran as it always did");
        return;
      }
      Thread.sleep(1000);
    }

    final var aggregate = transactionTemplate
        .execute(status -> repository.findById(aggregateId).orElseThrow());
    fail(
        ("the listeners did not both reach a method within 150 seconds: the listener of the task %s, "
            + "the listener of the end event %s, the task itself %s. A listener job nothing serves "
            + "stops the workflow there with no incident and no message, which is what this key exists "
            + "for")
            .formatted(
                aggregate.isTheWorkWasAudited(),
                aggregate.isTheOrderWasArchived(),
                aggregate.isTheWorkWasDone()));

  }

}
