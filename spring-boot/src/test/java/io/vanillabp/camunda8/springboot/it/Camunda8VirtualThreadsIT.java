package io.vanillabp.camunda8.springboot.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The virtual-thread execution model end to end against a real cluster: a
 * <code>@WorkflowTask</code> method really runs on a virtual thread, and a blocking
 * handler does not delay the job of another worker there either.
 * <p>
 * The bound of the executor is unit-tested ({@code Camunda8VirtualThreadExecutorTest});
 * what only a cluster can show is that the executor the adapter hands the client is the
 * one the client runs the handlers on.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
    classes = DockerTestApplication.class,
    properties = "spring.config.name=camunda8-it")
@DirtiesContext
public class Camunda8VirtualThreadsIT {

  @Container
  static final GenericContainer<?> CAMUNDA = ClusterUnderTest.standaloneBroker();

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
    registry.add("vanillabp.adapters.c8.worker-threads", () -> "virtual");
    registry.add("vanillabp.adapters.c8.worker-threads-bound", () -> "8");

  }

  @Autowired
  private WorkerThreadsDockerWorkflowService blockingWorkflowService;

  @Autowired
  private QuickDockerWorkflowService quickWorkflowService;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private Camunda8ClientFactoryRegistry clientFactoryRegistry;

  @BeforeEach
  public void resetObservations() {

    WorkerThreadsDockerWorkflowService.reset();

  }

  @AfterEach
  public void resetObservationsForWhoeverComesNext() {

    WorkerThreadsDockerWorkflowService.reset();

  }

  @Test
  @DisplayName("the client runs the workers on the adapter's bounded virtual-thread executor")
  public void theClientRunsOnTheAdaptersExecutor() {

    final var factory = clientFactoryRegistry.getFactory("c8");

    assertNotNull(factory.getVirtualThreadExecutor(), "the adapter supplies the executor itself");
    assertEquals(8, factory.getVirtualThreadExecutor().getBound());
    assertSame(factory.getVirtualThreadExecutor(),
        factory.getClient().getConfiguration().jobWorkerExecutor(),
        "the client runs its workers on it");

  }

  @Test
  @DisplayName("a handler runs on a virtual thread, and blocking one does not delay another worker")
  public void handlersRunOnVirtualThreads() throws Exception {

    final var blocked = transactionTemplate
        .execute(status -> blockingWorkflowService.startBlocking().getId());
    assertNotNull(blocked);

    assertTrue(
        WorkerThreadsDockerWorkflowService.BLOCKING_ENTERED
            .await(30, TimeUnit.SECONDS),
        "the blocking handler was delivered");

    final var quick = transactionTemplate
        .execute(status -> quickWorkflowService.startWorkflow().getId());
    assertNotNull(quick);

    assertTrue(
        WorkerThreadsDockerWorkflowService.QUICK_SERVED
            .await(WorkerThreadsDockerWorkflowService.BLOCK_MILLIS, TimeUnit.MILLISECONDS),
        "the other worker's job was served while a handler was blocking");
    assertTrue(WorkerThreadsDockerWorkflowService.QUICK_SERVED_WHILE_BLOCKED.get(),
        "the two really ran at the same time");
    assertTrue(WorkerThreadsDockerWorkflowService.QUICK_SERVED_ON_VIRTUAL_THREAD.get(),
        "the handler ran on a virtual thread");

  }

}
