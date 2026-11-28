package io.vanillabp.camunda8.springboot.processtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import io.camunda.client.CamundaClient;
import io.camunda.client.spring.event.CamundaClientClosingSpringEvent;
import io.camunda.client.spring.event.CamundaClientCreatedSpringEvent;
import io.camunda.client.spring.testsupport.CamundaSpringProcessTestContext;
import io.camunda.process.test.api.CamundaProcessTestExecutionListener;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.springboot.client.Camunda8ClientAutoConfiguration;
import io.vanillabp.camunda8.springboot.client.Camunda8ClusterOfATestRuntime;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * When the adapter follows the cluster of Camunda Process Test, and which events move it. No
 * cluster is needed: building a client sends nothing, and the registry here has no workflow
 * module which would deploy. {@code Camunda8ProcessTestIT} runs the whole way against the
 * cluster of Camunda Process Test.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8FollowsTheProcessTestClusterTest {

  private static final String ADAPTER_ID = "c8";

  private Camunda8ClientFactoryRegistry registry;

  private Camunda8FollowsTheProcessTestCluster listener;

  private CamundaClient clientOfTheTestRuntime;

  @BeforeEach
  public void buildTheAdapterAndTheClientOfTheTestRuntime() {

    registry = new Camunda8ClientFactoryRegistry(Map.of(ADAPTER_ID, new Camunda8AdapterConfiguration()), true);
    final var beans = new StaticListableBeanFactory();
    beans.addBean("registry", registry);
    listener = new Camunda8FollowsTheProcessTestCluster(beans.getBeanProvider(Camunda8ClientFactoryRegistry.class));
    clientOfTheTestRuntime = CamundaClient
        .newClientBuilder()
        .restAddress(URI.create("http://cluster-of-the-test:8080"))
        .grpcAddress(URI.create("http://cluster-of-the-test:26500"))
        .build();

  }

  @AfterEach
  public void closeEverything() {

    registry.close();
    clientOfTheTestRuntime.close();

  }

  @Test
  public void theAdapterFollowsTheClusterOfEachTestMethod() {

    listener.connectToTheClusterOfTheTestMethod(
        new CamundaClientCreatedSpringEvent(new CamundaProcessTestExecutionListener(), clientOfTheTestRuntime));

    final var client = registry
        .getFactory(ADAPTER_ID)
        .getClient();
    assertThat(client).isNotSameAs(clientOfTheTestRuntime);
    assertThat(client.getConfiguration().getRestAddress()).isEqualTo(URI.create("http://cluster-of-the-test:8080"));
    assertThat(client.getConfiguration().getGrpcAddress()).isEqualTo(URI.create("http://cluster-of-the-test:26500"));

    listener.disconnectFromTheClusterOfTheTestMethod(
        new CamundaClientClosingSpringEvent(new CamundaProcessTestExecutionListener(), clientOfTheTestRuntime));

    assertThatThrownBy(() -> registry
        .getFactory(ADAPTER_ID)
        .getClient())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Camunda 8 adapter 'c8' has no cluster right now")
        .hasMessageContaining("only while a test method runs");

  }

  @Test
  public void aClientOfCamundasStarterIsNotTheClusterOfTheTest() {

    // the starter publishes the same event for its own client, with itself as the source
    listener.connectToTheClusterOfTheTestMethod(new CamundaClientCreatedSpringEvent(this, clientOfTheTestRuntime));

    assertThatThrownBy(() -> registry
        .getFactory(ADAPTER_ID)
        .getClient())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Camunda 8 adapter 'c8' has no cluster right now");

    listener.connectToTheClusterOfTheTestMethod(
        new CamundaClientCreatedSpringEvent(new CamundaProcessTestExecutionListener(), clientOfTheTestRuntime));
    listener.disconnectFromTheClusterOfTheTestMethod(new CamundaClientClosingSpringEvent(this, clientOfTheTestRuntime));

    assertThat(registry
        .getFactory(ADAPTER_ID)
        .getClient()).isNotNull();

  }

  @Test
  public void theSupportIsActiveInATestOnCamundaProcessTestOnly() {

    final var runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(Camunda8ProcessTestAutoConfiguration.class));

    runner.run(context -> {
      assertThat(context).doesNotHaveBean(Camunda8ClusterOfATestRuntime.class);
      assertThat(context).doesNotHaveBean(Camunda8FollowsTheProcessTestCluster.class);
    });
    runner
        .withBean(CamundaSpringProcessTestContext.class)
        .run(context -> {
          assertThat(context).hasSingleBean(Camunda8ClusterOfATestRuntime.class);
          assertThat(context).hasSingleBean(Camunda8FollowsTheProcessTestCluster.class);
        });

  }

  /**
   * An application with one Camunda 8 adapter id which names its own cluster. Building its
   * client sends nothing, so no cluster is needed.
   */
  private static ApplicationContextRunner applicationOnItsConfiguredCluster() {

    return new ApplicationContextRunner()
        .withPropertyValues(
            "vanillabp.adapters.c8.type=camunda8",
            "vanillabp.adapters.c8.rest-address=http://configured-cluster:8088",
            "vanillabp.adapters.c8.grpc-address=http://configured-cluster:26500",
            "vanillabp.adapters.c8.job-lease=do-not-use");

  }

  @Test
  public void aTestClassWithoutCamundaProcessTestStaysOnTheConfiguredCluster() {

    applicationOnItsConfiguredCluster()
        .withConfiguration(AutoConfigurations.of(
            Camunda8ClientAutoConfiguration.class, Camunda8ProcessTestAutoConfiguration.class))
        .run(context -> {
          assertThat(context).hasNotFailed();
          final var factory = context
              .getBean(Camunda8ClientFactoryRegistry.class)
              .getFactory(ADAPTER_ID);
          assertThat(factory.followsATestRuntime()).isFalse();
          assertThat(factory
              .getClient()
              .getConfiguration()
              .getRestAddress()).isEqualTo(URI.create("http://configured-cluster:8088"));
        });

  }

  @Test
  public void aTestOnCamundaProcessTestFollowsItsClusterInstead() {

    applicationOnItsConfiguredCluster()
        .withBean(CamundaSpringProcessTestContext.class)
        .withConfiguration(AutoConfigurations.of(
            Camunda8ClientAutoConfiguration.class, Camunda8ProcessTestAutoConfiguration.class))
        .run(context -> {
          assertThat(context).hasNotFailed();
          final var factory = context
              .getBean(Camunda8ClientFactoryRegistry.class)
              .getFactory(ADAPTER_ID);
          assertThat(factory.followsATestRuntime()).isTrue();
          assertThatThrownBy(factory::getClient).hasMessageContaining("has no cluster right now");
        });

  }

  @Test
  public void aTestOnCamundaProcessTestWithoutThisArtifactEndsTheStart() {

    // the auto-configuration of this artifact is left out, as if the application had not added it
    applicationOnItsConfiguredCluster()
        .withBean(CamundaSpringProcessTestContext.class)
        .withConfiguration(AutoConfigurations.of(Camunda8ClientAutoConfiguration.class))
        .run(context -> assertThat(context)
            .getFailure()
            .rootCause()
            .hasMessageContaining("This test runs on Camunda Process Test (@CamundaSpringProcessTest), "
                + "but the Camunda 8 adapter 'c8' cannot follow its cluster")
            .hasMessageContaining("Add the artifact 'io.vanillabp:camunda8-adapter-spring-boot-process-test' "
                + "as a test dependency")
            .hasMessageContaining("'vanillabp.adapters.c8.startup-wait'"));

  }

}
