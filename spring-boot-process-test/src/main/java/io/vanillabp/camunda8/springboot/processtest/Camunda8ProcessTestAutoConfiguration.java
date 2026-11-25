package io.vanillabp.camunda8.springboot.processtest;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;

import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.springboot.client.Camunda8ClusterOfATestRuntime;

/**
 * Lets the Camunda 8 adapter follow the cluster of Camunda Process Test. It is active in a
 * test class annotated with {@code @CamundaSpringProcessTest} and nowhere else: that
 * annotation imports a marker bean, {@code CamundaSpringProcessTestContext}, and this class
 * waits for it. A test class of the same application without the annotation keeps the
 * adapter on its configured cluster.
 * <p>
 * Camunda Process Test starts its cluster after the application context and hands out a
 * client of its own before each test method. After the method it deletes everything in the
 * cluster, the deployments included. So this configuration tells the adapter not to build a
 * client at startup ({@link Camunda8ClusterOfATestRuntime}), and it connects the adapter to
 * the cluster of every test method ({@link Camunda8FollowsTheProcessTestCluster}).
 * <p>
 * The types of Camunda Process Test are named as strings. Spring Boot evaluates the
 * conditions before it loads anything else of this class. So an application which has this
 * artifact but not Camunda Process Test on its classpath starts as if it had neither.
 */
@AutoConfiguration
@ConditionalOnClass(name = Camunda8FollowsTheProcessTestCluster.EXECUTION_LISTENER)
@ConditionalOnBean(type = Camunda8ProcessTestAutoConfiguration.TEST_CONTEXT_MARKER)
public class Camunda8ProcessTestAutoConfiguration {

  /**
   * The marker bean which {@code @CamundaSpringProcessTest} imports into the application
   * context of a test class.
   */
  static final String TEST_CONTEXT_MARKER = "io.camunda.client.spring.testsupport.CamundaSpringProcessTestContext";

  /**
   * Built by Spring Boot while it reads the auto-configuration imports.
   */
  public Camunda8ProcessTestAutoConfiguration() {
  }

  /**
   * Tells the adapter that a test runtime hands over its cluster.
   *
   * @return The marker the adapter's client configuration reads
   */
  @Bean
  public Camunda8ClusterOfATestRuntime camunda8ClusterOfATestRuntime() {

    return new Camunda8ClusterOfATestRuntime();

  }

  /**
   * Connects the adapter to the cluster of each test method and takes it off again
   * afterwards.
   *
   * @param registry The clients of every Camunda 8 adapter id of the application. It is
   *          empty where the application configures no such id
   * @return The listener to the events of Camunda Process Test
   */
  @Bean
  public Camunda8FollowsTheProcessTestCluster camunda8FollowsTheProcessTestCluster(
      final ObjectProvider<Camunda8ClientFactoryRegistry> registry) {

    return new Camunda8FollowsTheProcessTestCluster(registry);

  }

}
