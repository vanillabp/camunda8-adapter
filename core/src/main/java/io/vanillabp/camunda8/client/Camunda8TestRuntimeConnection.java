package io.vanillabp.camunda8.client;

import java.net.URI;

import io.camunda.client.CamundaClientConfiguration;
import io.camunda.client.CredentialsProvider;

/**
 * Where the cluster of a test runtime answers, and how a client proves who it is there. A
 * test runtime such as Camunda Process Test starts its cluster only after the application
 * has started, and it hands out a client of its own before each test method. The adapter
 * takes the address and the credentials of that client and builds its own client with them
 * (see {@link Camunda8ClientFactory#connectToTheTestRuntime(Camunda8TestRuntimeConnection)}).
 * Everything else, the workers, the execution slots and the timeouts, still comes from the
 * adapter's own configuration.
 *
 * @param restAddress The REST address of the cluster
 * @param grpcAddress The gRPC address of the cluster
 * @param credentials What the client of the test runtime sends to prove who it is, or
 *          <code>null</code> where it sends nothing
 */
public record Camunda8TestRuntimeConnection(
                                            URI restAddress,
                                            URI grpcAddress,
                                            CredentialsProvider credentials) {

  /**
   * Reads the connection out of the configuration of the client a test runtime built.
   *
   * @param configuration The configuration of that client
   * @return Where the cluster answers, and with which credentials
   */
  public static Camunda8TestRuntimeConnection of(
      final CamundaClientConfiguration configuration) {

    return new Camunda8TestRuntimeConnection(
        configuration.getRestAddress(), configuration.getGrpcAddress(), configuration.getCredentialsProvider());

  }

  /**
   * The two addresses, the way a log line names them.
   *
   * @return Both addresses in one string
   */
  public String describe() {

    return "rest-address '%s', grpc-address '%s'".formatted(restAddress, grpcAddress);

  }

}
