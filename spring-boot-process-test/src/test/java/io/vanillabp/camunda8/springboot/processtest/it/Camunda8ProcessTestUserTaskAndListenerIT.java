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

import io.camunda.process.test.api.CamundaSpringProcessTest;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;
import io.vanillabp.spi.process.ProcessService;

/**
 * A user task and a listener the model declares, on Camunda Process Test, in three test
 * methods of one class. The adapter delivers both through jobs of their own kind, and the core
 * remembers each delivery by its delivery id. Camunda Process Test empties its cluster after
 * each method, and the cluster then hands out the same job keys again. So the second and the
 * third method only pass if the delivery ids of user-task listeners and of modelled listeners
 * carry the mark of their handover, as the ones of service tasks do.
 */
@ExtendWith(SuppressOutputExtension.class)
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@CamundaSpringProcessTest
@TestMethodOrder(MethodOrderer.MethodName.class)
public class Camunda8ProcessTestUserTaskAndListenerIT {

  @Autowired
  private ProcessService<Review> processService;

  @Autowired
  private ReviewRepository reviews;

  @Autowired
  private TransactionTemplate transaction;

  private void reviewToTheEnd() {

    final var id = UUID.randomUUID().toString();
    transaction.executeWithoutResult(status -> processService.startWorkflow(new Review(id)));
    final var taskId = await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> reviews
            .findById(id)
            .map(Review::getTaskId)
            .orElse(null), taskIdOfTheOpenTask -> taskIdOfTheOpenTask != null);
    transaction.executeWithoutResult(status -> processService.completeUserTask(reviews
        .findById(id)
        .orElseThrow(), taskId));
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(reviews.findById(id))
            .get()
            .extracting(Review::isArchived)
            .isEqualTo(true));

  }

  @Test
  void test1_theUserTaskAndTheListenerAreDelivered() {
    reviewToTheEnd();
  }

  @Test
  void test2_theyAreDeliveredAgainOnTheNextCluster() {
    reviewToTheEnd();
  }

  @Test
  void test3_andOnTheOneAfterIt() {
    reviewToTheEnd();
  }

}
