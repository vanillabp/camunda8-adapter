package io.vanillabp.camunda8.springboot.foreigncluster;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The application of {@link Camunda8ForeignClusterAtStartupIT}. It has a package of its own, so
 * it scans one workflow service and nothing of the other tests.
 */
@SpringBootApplication
public class ForeignClusterTestApplication {

}
