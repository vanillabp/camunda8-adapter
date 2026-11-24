package io.vanillabp.camunda8.springboot.processtest.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.camunda.process.test.api.CamundaAssert;
import io.camunda.process.test.api.CamundaSpringProcessTest;
import io.camunda.process.test.api.assertions.ProcessInstanceSelectors;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;
import io.vanillabp.spi.process.ProcessService;

/**
 * A test of a version 1 application on Camunda Process Test, run unchanged on the adapter
 * of this line. The class carries {@code @SpringBootTest} and {@code @CamundaSpringProcessTest}
 * and nothing else about its cluster: no address, no container of its own. The test methods
 * are the ones a version 1 application writes. Each starts a workflow inside a transaction,
 * waits for the aggregate, and the last one asks Camunda's assertions about the end of the
 * workflow.
 * <p>
 * There are three methods on purpose. Camunda Process Test deletes everything in its cluster
 * after each method, the deployment included. The second and the third method only pass if
 * the adapter deploys again and opens its workers against the cluster of their method.
 * <p>
 * The first two lines of annotations are the conventions of this repository: the output of a
 * green test stays out of the build log, and a machine without Docker skips the class.
 */
@ExtendWith(SuppressOutputExtension.class)
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@CamundaSpringProcessTest
@TestMethodOrder(MethodOrderer.MethodName.class)
public class Camunda8ProcessTestIT {

  @Autowired
  private ProcessService<Order> processService;

  @Autowired
  private OrderRepository orders;

  @Autowired
  private TransactionTemplate transaction;

  private String startOrder() {
    final var id = UUID.randomUUID().toString();
    transaction.executeWithoutResult(status -> processService.startWorkflow(new Order(id)));
    return id;
  }

  private void awaitProcessed(
      final String id) {
    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(orders.findById(id)).get()
        .extracting(Order::getStatus)
        .isEqualTo("PROCESSED"));
  }

  @Test
  void test1_theServiceTaskProcessesTheOrder() {
    awaitProcessed(startOrder());
  }

  @Test
  void test2_aSecondOrderIsProcessedToo() {
    awaitProcessed(startOrder());
  }

  /** Uses Camunda's assertions, so only a variant with Camunda Process Test can run it. */
  @Test
  void test3_theWorkflowEnds() {
    awaitProcessed(startOrder());
    CamundaAssert.assertThatProcessInstance(ProcessInstanceSelectors.byProcessId("DemoProcess")).isCompleted();
  }

}
