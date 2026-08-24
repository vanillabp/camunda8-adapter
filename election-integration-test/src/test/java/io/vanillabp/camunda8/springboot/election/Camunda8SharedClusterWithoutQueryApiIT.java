package io.vanillabp.camunda8.springboot.election;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The other half: two <code>camunda8</code> adapter ids on a cluster WITHOUT
 * secondary storage do not boot.
 * <p>
 * On one cluster the two ids are told apart by the scope a workflow was deployed under,
 * and mapping the KEY of a job or a user task to its scope is a query-API question. Where
 * the query API is missing, every operation of both ids would be executed by whichever one
 * stands first in <code>prioritized-adapters</code>, which publishes messages into the
 * wrong scope and writes a changed workflow aggregate into the wrong instance without ever
 * failing. A boot which stops with a message naming the two ids is the better outcome, and
 * that decision is what this test holds.
 * <p>
 * An application with ONE Camunda 8 adapter is not affected: it keeps booting and working
 * against a cluster without secondary storage, which the other integration tests of this
 * module do throughout.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@Testcontainers(disabledWithoutDocker = true)
public class Camunda8SharedClusterWithoutQueryApiIT {

  @Container
  static final GenericContainer<?> CAMUNDA = ElectionCluster.standaloneBroker();

  private ConfigurableApplicationContext application;

  private static String restAddress() {

    return "http://"
        + CAMUNDA.getHost()
        + ":"
        + CAMUNDA.getMappedPort(8080);

  }

  private static String grpcAddress() {

    return "http://"
        + CAMUNDA.getHost()
        + ":"
        + CAMUNDA.getMappedPort(26500);

  }

  @AfterEach
  public void closeWhatIsLeft() {

    if ((application != null) && application.isActive()) {
      application.close();
    }

  }

  @Test
  @DisplayName("Two adapter ids on a cluster without the query API end the boot, naming both")
  public void twoIdsWithoutSecondaryStorageDoNotBoot() {

    final var failure = assertThrows(
        Exception.class,
        () -> application = new SpringApplicationBuilder(ElectionTestApplication.class)
            .run(
                "--spring.config.name=camunda8-election-it",
                "--spring.main.web-application-type=none",
                "--vanillabp.adapters.c8-plain.rest-address="
                    + restAddress(),
                "--vanillabp.adapters.c8-plain.grpc-address="
                    + grpcAddress(),
                "--vanillabp.adapters.c8-prefix.type=camunda8",
                "--vanillabp.adapters.c8-prefix.name-clash-avoidance=use-prefix",
                "--vanillabp.adapters.c8-prefix.rest-address="
                    + restAddress(),
                "--vanillabp.adapters.c8-prefix.grpc-address="
                    + grpcAddress(),
                "--vanillabp.workflow-modules.election-app.adapters.c8-prefix.resources-location=classpath:it-election",
                "--vanillabp.prioritized-adapters[0]=c8-prefix",
                "--vanillabp.prioritized-adapters[1]=c8-plain"));

    final var message = messageOf(failure);
    assertNotNull(message, "the boot failed with a message");
    assertTrue(
        message.contains("shares its cluster with the adapter id"),
        "the failure names what cannot work: "
            + message);
    assertTrue(
        message.contains("c8-prefix") && message.contains("c8-plain"),
        "and both adapter ids, so an operator knows which two: "
            + message);
    assertTrue(
        message.contains("secondaryStorage"),
        "and what to configure: "
            + message);

  }

  /**
   * @param failure What the boot threw
   * @return The message of the cause which named the defect, or <code>null</code>
   */
  private static String messageOf(
      final Throwable failure) {

    var current = failure;
    while (current != null) {
      final var message = current.getMessage();
      if ((message != null) && message.contains("shares its cluster with the adapter id")) {
        return message;
      }
      current = current.getCause();
    }
    return failure.getMessage();

  }

}
