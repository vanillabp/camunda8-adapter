package io.vanillabp.camunda8.springboot.client;

/**
 * A bean of this type says that a test runtime hands over the Camunda 8 cluster, and that
 * the adapter must not connect to the cluster it is configured for. Camunda Process Test is
 * such a runtime: it starts its cluster after the application context, hands out a client
 * before each test method and deletes everything in the cluster after it.
 * <p>
 * An application never declares this bean itself. The artifact
 * {@code camunda8-adapter-spring-boot-process-test} declares it in a test which runs on
 * Camunda Process Test, and it connects the adapter to the cluster of each test method.
 * {@link Camunda8ClientAutoConfiguration} reads whether the bean is there before it builds
 * the clients.
 */
public final class Camunda8ClusterOfATestRuntime {

  /**
   * Built by the auto-configuration of the test runtime support.
   */
  public Camunda8ClusterOfATestRuntime() {
  }

}
