package io.vanillabp.camunda8.springboot.election;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The application of the shared-cluster election test (story 103). It lives in its own
 * package on purpose: the workflow services of the other Camunda 8 integration tests are
 * scanned by their own application, and this one deploys a single BPMN process to TWO
 * adapter ids of one cluster.
 */
@SpringBootApplication
public class ElectionTestApplication {

}
