package io.vanillabp.camunda8.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.camunda.client.api.command.ClientStatusException;
import io.camunda.client.api.command.ProblemException;
import io.camunda.zeebe.gateway.protocol.GatewayGrpc;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.DeployResourceRequest;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.DeployResourceResponse;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.UpdateJobTimeoutRequest;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.UpdateJobTimeoutResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What the client of the active line does when a cluster answers that it is busy. This is
 * about the client and not about the adapter, and the adapter's texts and its retries rest on
 * it. Every line runs this class, so a client bump which changes the answer turns it red.
 * <p>
 * No cluster takes part, because a test cannot ask a broker to refuse on demand. A small server
 * answers with the refusal, and the client is the real one, built as the application gets it.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8ClientOnABusyClusterTest {

  private static final byte[] A_MODEL = "<bpmn/>".getBytes(StandardCharsets.UTF_8);

  /**
   * What the client waits for before it repeats a 503. A little below the second of the
   * HTTP library, because a timer may fire early by a tick.
   */
  private static final long THE_WAIT_OF_THE_CLIENT_MILLIS = 900;

  /**
   * The REST gateway. It refuses until the given number of requests has arrived.
   */
  private static final class RestGateway {

    final List<Long> requestsAt = new CopyOnWriteArrayList<>();

    final List<byte[]> bodies = new CopyOnWriteArrayList<>();

    private final HttpServer server;

    RestGateway(
        final int refusals) throws IOException {

      server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      server.createContext("/", exchange -> answer(exchange, refusals));
      server.start();

    }

    private void answer(
        final HttpExchange exchange,
        final int refusals) throws IOException {

      requestsAt.add(System.nanoTime());
      bodies.add(exchange.getRequestBody().readAllBytes());
      if (requestsAt.size() <= refusals) {
        final var refusal = """
            {"type":"about:blank","title":"RESOURCE_EXHAUSTED","status":503,"detail":"busy"}"""
            .getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/problem+json");
        exchange.sendResponseHeaders(503, refusal.length);
        exchange.getResponseBody().write(refusal);
      } else if (exchange.getRequestURI().getPath().endsWith("/deployments")) {
        final var deployment = """
            {"deploymentKey":"1","tenantId":"<default>","deployments":[]}"""
            .getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, deployment.length);
        exchange.getResponseBody().write(deployment);
      } else {
        exchange.sendResponseHeaders(204, -1);
      }
      exchange.close();

    }

    String address() {

      return "http://localhost:%d".formatted(server.getAddress().getPort());

    }

    long millisBetweenTheFirstTwoRequests() {

      return Duration.ofNanos(requestsAt.get(1) - requestsAt.get(0)).toMillis();

    }

    void stop() {

      server.stop(0);

    }

  }

  /**
   * The gRPC gateway. It refuses every command.
   */
  private static final class GrpcGateway extends GatewayGrpc.GatewayImplBase {

    final AtomicInteger requests = new AtomicInteger();

    @Override
    public void updateJobTimeout(
        final UpdateJobTimeoutRequest request,
        final StreamObserver<UpdateJobTimeoutResponse> response) {

      requests.incrementAndGet();
      response.onError(Status.RESOURCE_EXHAUSTED.asRuntimeException());

    }

    @Override
    public void deployResource(
        final DeployResourceRequest request,
        final StreamObserver<DeployResourceResponse> response) {

      requests.incrementAndGet();
      response.onError(Status.RESOURCE_EXHAUSTED.asRuntimeException());

    }

  }

  private RestGateway restGateway;

  private Server grpcServer;

  private Camunda8ClientFactory clientFactory;

  @AfterEach
  public void stopEverything() {

    if (clientFactory != null) {
      clientFactory.close();
    }
    if (restGateway != null) {
      restGateway.stop();
    }
    if (grpcServer != null) {
      grpcServer.shutdownNow();
    }

  }

  private Camunda8ClientFactory overRest(
      final RestGateway gateway) {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setRestAddress(gateway.address());
    return overRest(configuration);

  }

  private Camunda8ClientFactory overRest(
      final Camunda8AdapterConfiguration configuration) {

    clientFactory = new Camunda8ClientFactory("c8", configuration);
    return clientFactory;

  }

  private Camunda8ClientFactory overGrpc(
      final GrpcGateway gateway) throws IOException {

    grpcServer = ServerBuilder.forPort(0).addService(gateway).build().start();
    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setPreferRestOverGrpc(false);
    configuration.setGrpcAddress("http://localhost:%d".formatted(grpcServer.getPort()));
    clientFactory = new Camunda8ClientFactory("c8", configuration);
    return clientFactory;

  }

  @Test
  @DisplayName("REST: a 503 to the change of a job timeout is repeated once, a second later, and then reaches the caller")
  public void restRepeatsARefusedTimeoutChangeOnce() throws IOException {

    restGateway = new RestGateway(Integer.MAX_VALUE);
    final var client = overRest(restGateway).getClient();

    final var refusal = assertThrows(ProblemException.class, () -> client
        .newUpdateTimeoutCommand(4711L)
        .timeout(Duration.ofMinutes(1))
        .send()
        .join());

    assertEquals(503, refusal.code());
    assertEquals(2, restGateway.requestsAt.size());
    assertTrue(restGateway.millisBetweenTheFirstTwoRequests() >= THE_WAIT_OF_THE_CLIENT_MILLIS);

  }

  @Test
  @DisplayName("REST: a 503 to a deployment is repeated once as well, a second later, and then reaches the caller")
  public void restRepeatsARefusedDeploymentOnce() throws IOException {

    restGateway = new RestGateway(Integer.MAX_VALUE);
    final var client = overRest(restGateway).getClient();

    final var refusal = assertThrows(ProblemException.class, () -> client
        .newDeployResourceCommand()
        .addResourceBytes(A_MODEL, "a.bpmn")
        .send()
        .join());

    assertEquals(503, refusal.code());
    assertEquals(2, restGateway.requestsAt.size());
    assertTrue(restGateway.millisBetweenTheFirstTwoRequests() >= THE_WAIT_OF_THE_CLIENT_MILLIS);

  }

  @Test
  @DisplayName("REST: the repeated deployment carries the same model, and the caller never hears of the first 503")
  public void restRepeatsADeploymentWithItsBody() throws IOException {

    restGateway = new RestGateway(1);
    final var client = overRest(restGateway).getClient();

    client
        .newDeployResourceCommand()
        .addResourceBytes(A_MODEL, "a.bpmn")
        .send()
        .join();

    assertEquals(2, restGateway.bodies.size());
    assertTrue(restGateway.bodies.getFirst().length > A_MODEL.length, "the multipart body arrived");
    assertArrayEquals(restGateway.bodies.getFirst(), restGateway.bodies.getLast());

  }

  @Test
  @DisplayName("REST: a request timeout below the client's wait turns its repetition off")
  public void aShortRequestTimeoutTurnsTheRepetitionOff() throws IOException {

    restGateway = new RestGateway(Integer.MAX_VALUE);
    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setRestAddress(restGateway.address());
    configuration.setRequestTimeout(Duration.ofMillis(500));
    final var client = overRest(configuration).getClient();

    assertThrows(ProblemException.class, () -> client
        .newUpdateTimeoutCommand(4711L)
        .timeout(Duration.ofMinutes(1))
        .send()
        .join());

    assertEquals(1, restGateway.requestsAt.size());

  }

  @Test
  @DisplayName("gRPC: RESOURCE_EXHAUSTED to the change of a job timeout reaches the caller at once")
  public void grpcDoesNotRepeatARefusedTimeoutChange() throws IOException {

    final var gateway = new GrpcGateway();
    final var client = overGrpc(gateway).getClient();

    final var refusal = assertThrows(ClientStatusException.class, () -> client
        .newUpdateTimeoutCommand(4711L)
        .timeout(Duration.ofMinutes(1))
        .send()
        .join());

    assertEquals(Status.Code.RESOURCE_EXHAUSTED, refusal.getStatusCode());
    assertEquals(1, gateway.requests.get());

  }

  @Test
  @DisplayName("gRPC: RESOURCE_EXHAUSTED to a deployment reaches the caller at once")
  public void grpcDoesNotRepeatARefusedDeployment() throws IOException {

    final var gateway = new GrpcGateway();
    final var client = overGrpc(gateway).getClient();

    assertThrows(ClientStatusException.class, () -> client
        .newDeployResourceCommand()
        .addResourceBytes(A_MODEL, "a.bpmn")
        .send()
        .join());

    assertEquals(1, gateway.requests.get());

  }

  @Test
  @DisplayName("REST: the attempts of the adapter multiply with the client's, ten requests and five seconds of its waits")
  public void theAttemptsOfTheAdapterMultiplyWithTheClientsOnRest() throws IOException {

    restGateway = new RestGateway(Integer.MAX_VALUE);
    final var client = overRest(restGateway).getClient();
    final var startedAt = System.nanoTime();

    assertThrows(ProblemException.class, () -> Camunda8CommandRetry.send(
        "c8", "completion", 4711L, "someTask", System.currentTimeMillis() + 60_000, () -> false, () -> client
            .newCompleteCommand(4711L)
            .send()
            .join()));

    assertEquals(2 * Camunda8CommandRetry.MAX_ATTEMPTS, restGateway.requestsAt.size());
    final var tookMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();
    assertTrue(tookMillis >= Camunda8CommandRetry.MAX_ATTEMPTS * THE_WAIT_OF_THE_CLIENT_MILLIS, "took "
        + tookMillis);

  }

  @Test
  @DisplayName("gRPC: the attempts of the adapter are all there is")
  public void theAttemptsOfTheAdapterAreAllThereIsOnGrpc() throws IOException {

    final var gateway = new GrpcGateway();
    final var client = overGrpc(gateway).getClient();

    assertThrows(ClientStatusException.class, () -> Camunda8CommandRetry.send(
        "c8", "lock renewal", 4711L, "someTask", System.currentTimeMillis() + 60_000, () -> false, () -> client
            .newUpdateTimeoutCommand(4711L)
            .timeout(Duration.ofMinutes(1))
            .send()
            .join()));

    assertEquals(Camunda8CommandRetry.MAX_ATTEMPTS, gateway.requests.get());

  }

}
