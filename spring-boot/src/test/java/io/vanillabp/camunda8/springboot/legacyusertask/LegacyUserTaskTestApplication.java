package io.vanillabp.camunda8.springboot.legacyusertask;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The application of {@link Camunda8LegacyUserTaskIT}. It has a package of its own, so it
 * scans one workflow service and nothing of the other tests.
 */
@SpringBootApplication
public class LegacyUserTaskTestApplication {

}
