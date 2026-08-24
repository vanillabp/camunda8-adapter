package io.vanillabp.camunda8.springboot.election;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The application of the shared-cluster election test, which deploys a single
 * BPMN process to TWO adapter ids of one cluster.
 * <p>
 * It lives in a MAVEN MODULE of its own, and that is the point: a workflow
 * module is a classpath entry carrying {@code META-INF/workflow-module}, so two scenarios
 * in one Maven module are one workflow module, and every application is asked for the
 * persistence of the other scenario's aggregates, which ends the startup rather than
 * failing at the first task. Here the marker names
 * {@code election-app}, the Spring Boot module's tests name {@code test-app}, and neither
 * context registers what it does not configure.
 * <p>
 * The price of the separation is one local helper: {@link ElectionCluster} does what
 * {@code ClusterUnderTest} does for the Spring Boot module's own tests, which is the same
 * duplication the Quarkus integration tests keep with their {@code ClusterImage}.
 */
@SpringBootApplication
public class ElectionTestApplication {

}
