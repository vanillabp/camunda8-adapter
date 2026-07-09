package io.vanillabp.camunda8.springboot.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.camunda8.processservice.Camunda8ProcessService;

/**
 * Integration test of the Camunda 8 adapter against a real Camunda 8 cluster started via
 * Testcontainers. It exercises the real adapter classes (client factory, deployment
 * service, process service):
 * <ol>
 *   <li>a BPMN is parsed and deployed to the cluster ({@code readBpmn} &rarr;
 *       {@code prepareBpmn} &rarr; {@code deployResources}),</li>
 *   <li>the phase-two creation logic
 *       ({@link Camunda8ProcessService#createProcessInstance(String, Object)}) creates a
 *       process instance of the deployed process, and</li>
 *   <li>the single {@code aggregateId} process variable round-trips through the engine
 *       (proven with a synchronously completing process).</li>
 * </ol>
 * <p>
 * The image {@code camunda/zeebe:8.8.31} (broker + embedded gateway, REST on 8080 and
 * gRPC on 26500) is used with the secondary storage disabled so it runs standalone
 * without Elasticsearch. The class is skipped when Docker is unavailable
 * ({@code @Testcontainers(disabledWithoutDocker = true)}).
 * <p>
 * <b>Not covered here (platform-integration gap):</b> starting a workflow through
 * {@code ProcessService#startWorkflow} (write outbox entry in the transaction, create the
 * instance only after commit, rollback safety) - the adapter SPI method
 * {@code MigratableProcessService.startWorkflowPhaseTwo(Object)} does not supply the BPMN
 * process ID the create needs. See the repository-root {@code README.md}.
 */
@Testcontainers(disabledWithoutDocker = true)
public class Camunda8DeploymentAndStartIT {

  private static final String BPMN_PROCESS_ID = "TestProcess";

  @Container
  static final GenericContainer<?> CAMUNDA = new GenericContainer<>(
      DockerImageName.parse("camunda/zeebe:8.8.31"))
      .withExposedPorts(8080, 26500, 9600)
      // run the broker standalone (no Elasticsearch) with an unprotected API so no
      // authentication/identity provider is needed for this test
      .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "none")
      .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
      // the readiness probe turns UP only once the partition leader can accept
      // deployments, avoiding a transient 503 on the first deploy
      .waitingFor(Wait
          .forHttp("/actuator/health/readiness")
          .forPort(9600)
          .forStatusCode(200)
          .withStartupTimeout(Duration.ofMinutes(3)));

  private static Camunda8ClientFactory clientFactory;

  private static Camunda8DeploymentService deploymentService;

  private static Camunda8ProcessService<Object> processService;

  @BeforeAll
  static void setUp() {

    final var configuration = new Camunda8AdapterConfiguration();
    // exercise the adapter's default REST transport against the unprotected broker
    configuration.setRestAddress("http://"
        + CAMUNDA.getHost()
        + ":"
        + CAMUNDA.getMappedPort(8080));

    clientFactory = new Camunda8ClientFactory("c8", configuration);
    deploymentService = new Camunda8DeploymentService("c8", clientFactory);
    processService = new Camunda8ProcessService<>("c8", clientFactory);

  }

  @AfterAll
  static void tearDown() {

    if (clientFactory != null) {
      clientFactory.close();
    }

  }

  private static InputStream bpmn() {

    final var model = Bpmn
        .createExecutableProcess(BPMN_PROCESS_ID)
        .name("Test process")
        .startEvent()
        .endEvent()
        .done();
    return new ByteArrayInputStream(Bpmn.convertToString(model).getBytes(UTF_8));

  }

  @Test
  @DisplayName("BPMN is deployed and a process instance is created with the aggregateId variable")
  public void deployAndStart() {

    // deploy the BPMN through the adapter's deployment pipeline
    final var entries = deploymentService.readBpmn("test-module", "test.bpmn", bpmn(), true);
    assertEquals(1, entries.size());
    assertEquals(BPMN_PROCESS_ID, entries.get(0).getKey());

    final Camunda8ProcessingContext context = deploymentService.prepareBpmn(
        "test-module", null, "test.bpmn", entries.get(0).getKey(), entries.get(0).getValue());
    deploymentService.deployResources("test-module", context);

    // the phase-two creation logic starts an instance of the deployed process
    final var event = processService.createProcessInstance(BPMN_PROCESS_ID, "aggregate-42");
    assertEquals(BPMN_PROCESS_ID, event.getBpmnProcessId());
    assertTrue(event.getProcessInstanceKey() > 0, "expected a process instance to be created");

    // prove the aggregateId variable really reaches the engine: run a synchronously
    // completing instance and read back its variables
    final var result = clientFactory
        .getClient()
        .newCreateInstanceCommand()
        .bpmnProcessId(BPMN_PROCESS_ID)
        .latestVersion()
        .variable(Camunda8ProcessService.AGGREGATE_ID_VARIABLE, "aggregate-99")
        .withResult()
        .fetchVariables(Camunda8ProcessService.AGGREGATE_ID_VARIABLE)
        .send()
        .join();
    assertEquals("aggregate-99", result.getVariablesAsMap().get(Camunda8ProcessService.AGGREGATE_ID_VARIABLE));

  }

}
