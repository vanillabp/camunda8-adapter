package io.vanillabp.camunda8.deployment;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.integration.adapter.spi.BpmnParseException;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Unit tests of {@link Camunda8DeploymentService} not requiring a cluster: BPMN parsing
 * (executable-process extraction, parse errors) and that deployment of an empty module
 * does not touch the client.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8DeploymentServiceTest {

  private static final String TWO_PROCESSES_ONE_EXECUTABLE = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          id="Definitions_1" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:process id="ExecutableProcess" isExecutable="true">
          <bpmn:startEvent id="start"/>
        </bpmn:process>
        <bpmn:process id="NonExecutableProcess" isExecutable="false">
          <bpmn:startEvent id="start2"/>
        </bpmn:process>
      </bpmn:definitions>
      """;

  private static final String TWO_EXECUTABLE_PROCESSES = """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          id="Definitions_2" targetNamespace="http://bpmn.io/schema/bpmn">
        <bpmn:process id="First" isExecutable="true">
          <bpmn:startEvent id="start1"/>
        </bpmn:process>
        <bpmn:process id="Second" isExecutable="true">
          <bpmn:startEvent id="start2"/>
        </bpmn:process>
      </bpmn:definitions>
      """;

  private Camunda8DeploymentService newDeploymentService() {

    // an unconfigured factory: getClient() would throw if ever called
    return new Camunda8DeploymentService("c8", new Camunda8ClientFactory("c8", new Camunda8AdapterConfiguration()), new NoOpInvoker(), (
        m2,
        p2,
        t2) -> io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, java.time.Duration
            .ofDays(14));

  }

  private static InputStream bpmn(
      final String bpmnProcessId) {

    final var model = Bpmn
        .createExecutableProcess(bpmnProcessId)
        .startEvent()
        .endEvent()
        .done();
    return new ByteArrayInputStream(Bpmn.convertToString(model).getBytes(UTF_8));

  }

  @Test
  @DisplayName("readBpmn returns one entry per executable process")
  public void readBpmnReturnsExecutableProcess() {

    final var service = newDeploymentService();

    final var result = service.readBpmn("module", "test.bpmn", bpmn("MyProcess"), true);

    assertEquals(1, result.size());
    assertEquals("MyProcess", result.get(0).getKey());

  }

  @Test
  @DisplayName("readBpmn ignores non-executable processes of a multi-process file")
  public void readBpmnIgnoresNonExecutableProcesses() {

    final var service = newDeploymentService();

    final var result = service.readBpmn(
        "module",
        "two.bpmn",
        new ByteArrayInputStream(TWO_PROCESSES_ONE_EXECUTABLE.getBytes(UTF_8)),
        true);

    assertEquals(1, result.size());
    assertEquals("ExecutableProcess", result.get(0).getKey());

  }

  @Test
  @DisplayName("readBpmn wraps parse errors in BpmnParseException")
  public void readBpmnWrapsParseErrors() {

    final var service = newDeploymentService();

    final var exception = assertThrows(
        BpmnParseException.class,
        () -> service.readBpmn(
            "module",
            "broken.bpmn",
            new ByteArrayInputStream("this is not valid bpmn xml".getBytes(UTF_8)),
            true));
    assertTrue(exception.getMessage().contains("broken.bpmn"));

  }

  @Test
  @DisplayName("prepareBpmn accumulates resources and process IDs into the context")
  public void prepareBpmnAccumulates() {

    final var service = newDeploymentService();
    final var model = Bpmn
        .createExecutableProcess("P1")
        .startEvent()
        .endEvent()
        .done();

    final Camunda8ProcessingContext context = service.prepareBpmn("module", null, "a.bpmn", "P1", model);
    // a second executable process of the same file reuses the context and deduplicates
    // the resource by filename
    service.prepareBpmn("module", context, "a.bpmn", "P2", model);

    assertEquals(1, context.getResources().size());

  }

  @Test
  @DisplayName("deploying an empty module does not touch the (unconfigured) client")
  public void deployEmptyModuleDoesNotTouchClient() {

    final var service = newDeploymentService();

    // null context (no BPMN files at all) and empty context must not build a client
    assertDoesNotThrow(() -> service.deployResources("module", null));
    assertDoesNotThrow(() -> service.deployResources("module", new Camunda8ProcessingContext("module")));

  }


  /**
   * Wiring validation is exercised by the integration tests - unit tests use a
   * permissive no-op invoker.
   */

  @Test
  @DisplayName("a listener worker's lock is resolved like a task's, defaulting to the same five minutes")
  public void listenerLockDefaultsToTheJobTimeout() {

    final var deploymentService = newDeploymentService();

    assertEquals(
        io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT,
        deploymentService.listenerLockOf("m", java.util.List.of("Process"), "workflow-end", "jobType"),
        "the user-task listener, the BPMS-initiated start and the workflow end run application code "
            + "in a transaction just like a task, so there is no second rule");

  }

  @Test
  @DisplayName("a listener worker's lock follows the configured job-timeout of its workflow")
  public void listenerLockFollowsTheConfiguredJobTimeout() {

    final var deploymentService = new Camunda8DeploymentService(
        "c8", new Camunda8ClientFactory("c8", new Camunda8AdapterConfiguration()), new NoOpInvoker(), (
            workflowModuleId,
            bpmnProcessId,
            taskDefinition) -> "Process".equals(bpmnProcessId)
                ? java.time.Duration.ofMinutes(2)
                : io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, java.time.Duration
                    .ofDays(14));

    assertEquals(
        java.time.Duration.ofMinutes(2),
        deploymentService.listenerLockOf("m", java.util.List.of("Process"), "start-event", "jobType"),
        "there is no task to key a listener by, so it resolves at adapter, module and workflow level");

  }

  @Test
  @DisplayName("one listener job type with conflicting locks fails naming both processes")
  public void conflictingListenerLocksFailGuiding() {

    final var deploymentService = new Camunda8DeploymentService(
        "c8", new Camunda8ClientFactory("c8", new Camunda8AdapterConfiguration()), new NoOpInvoker(), (
            workflowModuleId,
            bpmnProcessId,
            taskDefinition) -> "Fast".equals(bpmnProcessId)
                ? java.time.Duration.ofSeconds(30)
                : java.time.Duration.ofMinutes(10), java.time.Duration.ofDays(14));

    final var exception = assertThrows(
        IllegalStateException.class,
        () -> deploymentService.listenerLockOf(
            "m", java.util.List.of("Fast", "Slow"), "user-task listener", "io.vanillabp.userTask:form"));

    assertTrue(exception.getMessage().contains("Fast"), exception.getMessage());
    assertTrue(exception.getMessage().contains("Slow"), exception.getMessage());
    assertTrue(exception.getMessage().contains("job-timeout"),
        "the message names the property to align, but was: "
            + exception.getMessage());

  }


  @Test
  @DisplayName("stream-timeout is the one worker option with no client-wide equivalent, so it is set per worker")
  public void streamTimeoutReachesTheWorkerBuilder() {

    final var configuration = new Camunda8AdapterConfiguration();
    configuration.setStreamTimeout(java.time.Duration.ofMinutes(30));
    final var deploymentService = new Camunda8DeploymentService(
        "c8", new Camunda8ClientFactory("c8", configuration), new NoOpInvoker(), (
            m,
            p,
            t) -> io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, java.time.Duration
                .ofDays(14));
    final var builder = org.mockito.Mockito
        .mock(io.camunda.client.api.worker.JobWorkerBuilderStep1.JobWorkerBuilderStep3.class);
    org.mockito.Mockito
        .when(builder.metrics(org.mockito.ArgumentMatchers.any()))
        .thenReturn(builder);
    org.mockito.Mockito
        .when(builder.streamTimeout(org.mockito.ArgumentMatchers.any()))
        .thenReturn(builder);

    deploymentService.applyWorkerOptions(builder, "approve");

    org.mockito.Mockito
        .verify(builder)
        .streamTimeout(java.time.Duration.ofMinutes(30));

  }

  @Test
  @DisplayName("without a configured stream-timeout the worker keeps the client's")
  public void withoutStreamTimeoutTheWorkerIsLeftAlone() {

    final var deploymentService = newDeploymentService();
    final var builder = org.mockito.Mockito
        .mock(io.camunda.client.api.worker.JobWorkerBuilderStep1.JobWorkerBuilderStep3.class);
    org.mockito.Mockito
        .when(builder.metrics(org.mockito.ArgumentMatchers.any()))
        .thenReturn(builder);

    assertNotNull(deploymentService.applyWorkerOptions(builder, "approve"));

    // story 92: the metrics hook is the ONE thing every worker gets, and without a
    // metrics backend it is the client's own no-op
    org.mockito.Mockito
        .verify(builder)
        .metrics(org.mockito.ArgumentMatchers.any());
    org.mockito.Mockito.verifyNoMoreInteractions(builder);

  }

  static class NoOpInvoker implements io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskInvoker {

    @Override
    public void validateTaskWiring(
        final String workflowModuleId,
        final String bpmnProcessId,
        final java.util.Collection<io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec> tasks) {
    }

    @Override
    public void validateNoUnwiredWorkflowTaskMethods(
        final String workflowModuleId) {
    }

    @Override
    public io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskOutcome invokeWorkflowTask(
        final String workflowModuleId,
        final String bpmnProcessId,
        final io.vanillabp.integration.adapter.spi.workflowtask.TaskInvocationContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.util.Map<String, Object> syncedWorkflowAggregateValues(
        final String workflowModuleId,
        final String bpmnProcessId,
        final String workflowAggregateId,
        final io.vanillabp.integration.adapter.spi.AggregateSyncMode adapterDefault) {
      return java.util.Map.of();
    }

    // story 66's migration fallback, deprecated for removal in 2.1 and none of Camunda
    // 8's business: a test double implements it as long as the interface declares it,
    // and the mandatory 'removal' lint needs the suppression
    @SuppressWarnings("removal")
    @Override
    public boolean workflowAggregateHasProperty(
        final String workflowModuleId,
        final String bpmnProcessId,
        final String propertyName) {
      return false;
    }

    @SuppressWarnings("removal")
    @Override
    public Object resolveWorkflowAggregateProperty(
        final String workflowModuleId,
        final String bpmnProcessId,
        final String workflowAggregateId,
        final String propertyName) {
      return null;
    }

    @Override
    public boolean workflowTaskHandlerExists(
        final String workflowModuleId,
        final String bpmnProcessId,
        final String taskDefinitionOrActivityId) {
      return true;
    }

    @Override
    public String resolveWorkflowAggregateIdName(
        final String workflowModuleId,
        final String bpmnProcessId) {
      return "id";
    }

  }


  /**
   * Story 35: the tenant a workflow module is deployed to, and the prefixed
   * identifiers replacing it. Asserted on the model and the resolved tenant - the
   * cluster round-trip is covered by the Docker ITs.
   */
  @org.junit.jupiter.api.Nested
  @DisplayName("Name-clash avoidance")
  class NameClashAvoidanceTests {

    private static final String MODULE = "loan-approval";

    /**
     * A minimal {@link io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport}
     * of the given mode - the adapter is tested against the SPI, not against the
     * core's implementation (this module deliberately depends on the SPI only).
     */
    private io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport scopingWith(
        final io.vanillabp.integration.adapter.spi.NameClashAvoidance mode) {

      final var separator = io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport.SEPARATOR;
      return new io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport() {

        private boolean prefixes() {
          return mode == io.vanillabp.integration.adapter.spi.NameClashAvoidance.USE_PREFIX;
        }

        @Override
        public io.vanillabp.integration.adapter.spi.NameClashAvoidance modeFor(
            final String workflowModuleId,
            final String bpmnProcessId,
            final String adapterId) {
          return mode;
        }

        @Override
        public String scopedProcessId(
            final String workflowModuleId,
            final String bpmnProcessId,
            final String adapterId) {
          return prefixes()
              ? workflowModuleId + separator + bpmnProcessId
              : bpmnProcessId;
        }

        @Override
        public String scopedIdentifier(
            final String workflowModuleId,
            final String identifier,
            final String adapterId) {
          return prefixes() && (identifier != null)
              ? workflowModuleId + separator + identifier
              : identifier;
        }

        @Override
        public String scopedTaskDefinition(
            final String workflowModuleId,
            final String bpmnProcessId,
            final String taskDefinition,
            final String adapterId) {
          return prefixes() && (taskDefinition != null)
              ? workflowModuleId + separator + bpmnProcessId + separator + taskDefinition
              : taskDefinition;
        }

        @Override
        public String plainProcessId(
            final String workflowModuleId,
            final String scopedBpmnProcessId,
            final String adapterId) {
          final var prefix = workflowModuleId + separator;
          return prefixes() && scopedBpmnProcessId.startsWith(prefix)
              ? scopedBpmnProcessId.substring(prefix.length())
              : scopedBpmnProcessId;
        }

        @Override
        public String plainIdentifier(
            final String workflowModuleId,
            final String scopedIdentifier,
            final String adapterId) {
          return plainProcessId(workflowModuleId, scopedIdentifier, adapterId);
        }

        @Override
        public String plainTaskDefinition(
            final String workflowModuleId,
            final String bpmnProcessId,
            final String scopedTaskDefinition,
            final String adapterId) {
          final var prefix = workflowModuleId + separator + bpmnProcessId + separator;
          return prefixes() && scopedTaskDefinition.startsWith(prefix)
              ? scopedTaskDefinition.substring(prefix.length())
              : scopedTaskDefinition;
        }

        @Override
        public void validateNoneNameClashStrategy(
            final String adapterId,
            final String byAdapterOnlyPropertyKey) {
        }

        @Override
        public void validateNativeIsolationSupported(
            final String adapterId,
            final String workflowModuleId,
            final String bpmsDescription) {
        }

        @Override
        public void validateNoCollidingProcessIds(
            final String adapterId,
            final java.util.Collection<DeployedProcess> deployedProcesses) {
        }

      };

    }

    private io.camunda.zeebe.model.bpmn.BpmnModelInstance modelOf(
        final io.vanillabp.integration.adapter.spi.NameClashAvoidance mode) {

      final var service = new Camunda8DeploymentService(
          "c8", new Camunda8ClientFactory("c8", new Camunda8AdapterConfiguration()), new NoOpInvoker(), (
              m2,
              p2,
              t2) -> io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, java.time.Duration
                  .ofDays(14), null, scopingWith(mode));
      final var model = io.camunda.zeebe.model.bpmn.Bpmn
          .createExecutableProcess("RiskAssessment")
          .startEvent()
          .serviceTask("Task", task -> task.zeebeJobType("scoreApplicant"))
          .endEvent()
          .done();
      service.prepareBpmn(MODULE, null, "risk.bpmn", "RiskAssessment", model);
      return model;

    }

    @Test
    @DisplayName("BY_ADAPTER leaves the model alone - the TENANT isolates, as in version 1")
    public void byAdapterKeepsTheModelAndUsesTheModuleAsTenant() {

      final var model = modelOf(io.vanillabp.integration.adapter.spi.NameClashAvoidance.BY_ADAPTER);

      assertNotNull(model.getModelElementById("RiskAssessment"), "the process id stays plain");
      // the tenant a module is deployed to: the workflow module id (version 1's
      // behavior, overridable by the adapter's tenant-id)
      assertEquals(
          MODULE,
          io.vanillabp.camunda8.wiring.Camunda8Scoping.tenantIdFor(
              scopingWith(io.vanillabp.integration.adapter.spi.NameClashAvoidance.BY_ADAPTER), MODULE, "c8", null));
      // ... unless the adapter configured a name
      assertEquals(
          "banking",
          io.vanillabp.camunda8.wiring.Camunda8Scoping.tenantIdFor(
              scopingWith(io.vanillabp.integration.adapter.spi.NameClashAvoidance.BY_ADAPTER), MODULE, "c8",
              "banking"));

    }

    @Test
    @DisplayName("USE_PREFIX rewrites process id and job type - and uses NO tenant")
    public void usePrefixRewritesTheModel() {

      final var model = modelOf(io.vanillabp.integration.adapter.spi.NameClashAvoidance.USE_PREFIX);

      assertNotNull(
          model.getModelElementById("loan-approval__RiskAssessment"),
          "the process id carries the workflow module as prefix");
      final var xml = io.camunda.zeebe.model.bpmn.Bpmn.convertToString(model);
      assertTrue(
          xml.contains("loan-approval__RiskAssessment__scoreApplicant"),
          () -> "the job type is scoped per module AND process but was: "
              + xml);
      assertNull(
          io.vanillabp.camunda8.wiring.Camunda8Scoping.tenantIdFor(
              scopingWith(io.vanillabp.integration.adapter.spi.NameClashAvoidance.USE_PREFIX), MODULE, "c8",
              "banking"),
          "the prefix IS the isolation - no tenant, which is what saves tenant licenses");

    }

    @Test
    @DisplayName("a multi-process file is prefixed ONCE, although prepareBpmn is called per process")
    public void multiProcessFileIsScopedOnlyOnce() {

      final var service = new Camunda8DeploymentService(
          "c8", new Camunda8ClientFactory("c8", new Camunda8AdapterConfiguration()), new NoOpInvoker(), (
              m2,
              p2,
              t2) -> io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, java.time.Duration
                  .ofDays(14), null, scopingWith(io.vanillabp.integration.adapter.spi.NameClashAvoidance.USE_PREFIX));
      final var model = io.camunda.zeebe.model.bpmn.Bpmn
          .readModelFromStream(new ByteArrayInputStream(TWO_EXECUTABLE_PROCESSES.getBytes(UTF_8)));

      // this is what the core does for a file holding two executable processes: one
      // prepareBpmn call per PROCESS, both sharing the very same model instance
      var context = service.prepareBpmn(MODULE, null, "two.bpmn", "First", model);
      context = service.prepareBpmn(MODULE, context, "two.bpmn", "Second", model);

      assertNotNull(model.getModelElementById("loan-approval__First"), "prefixed exactly once");
      assertNotNull(model.getModelElementById("loan-approval__Second"), "prefixed exactly once");
      assertNull(
          model.getModelElementById("loan-approval__loan-approval__First"),
          "the second prepareBpmn call must not prefix the model again");

    }

    @Test
    @DisplayName("NONE leaves the model alone and uses no tenant")
    public void noneScopesNothing() {

      final var model = modelOf(io.vanillabp.integration.adapter.spi.NameClashAvoidance.NONE);

      assertNotNull(model.getModelElementById("RiskAssessment"));
      assertNull(
          io.vanillabp.camunda8.wiring.Camunda8Scoping.tenantIdFor(
              scopingWith(io.vanillabp.integration.adapter.spi.NameClashAvoidance.NONE), MODULE, "c8", null));

    }

    private Camunda8DeploymentService serviceOfAdapterId(
        final String adapterId) {

      return new Camunda8DeploymentService(
          adapterId, new Camunda8ClientFactory(adapterId, new Camunda8AdapterConfiguration()), new NoOpInvoker(), (
              m2,
              p2,
              t2) -> io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, java.time.Duration
                  .ofDays(14), null, null);

    }

    @Test
    @DisplayName("Without configuration the mode is NONE - a stock cluster rejects tenant ids")
    public void defaultsToNone() {

      assertEquals(
          io.vanillabp.integration.adapter.spi.NameClashAvoidance.NONE,
          serviceOfAdapterId("c8").defaultNameClashAvoidance(),
          "multi-tenancy is switched off in a cluster started from the stock image, so "
              + "by-adapter would fail the boot of an application configuring nothing");

    }

    /**
     * The WARNs the adapter logged (the module's logback-test.xml has no appender on
     * purpose).
     */
    private java.util.List<String> warningsOf(
        final Runnable action) {

      final var logWatcher = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
      logWatcher.start();
      final var adapterLog = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
          .getLogger(Camunda8DeploymentService.class);
      adapterLog.addAppender(logWatcher);
      try {
        action.run();
      } finally {
        adapterLog.detachAndStopAllAppenders();
      }
      return logWatcher.list
          .stream()
          .filter(event -> event.getLevel() == ch.qos.logback.classic.Level.WARN)
          .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
          .toList();

    }

    @Test
    @DisplayName("An unscoped workflow module is reported naming Camunda 8's alternatives")
    public void unscopedIdentifiersAreReported() {

      final var service = serviceOfAdapterId("myengine");

      final var byDefault = warningsOf(() -> service.warnAboutUnscopedIdentifiers(MODULE, true));
      assertEquals(1, byDefault.size(), () -> byDefault.toString());
      final var message = byDefault.getFirst();
      assertTrue(message.contains("'"
          + MODULE
          + "'"), () -> message);
      assertTrue(message.contains("nothing is configured"), () -> message);
      assertTrue(
          message.contains("vanillabp.adapters.myengine.name-clash-avoidance: use-prefix"),
          () -> message);
      assertTrue(
          message.contains("vanillabp.adapters.myengine.name-clash-avoidance: by-adapter"),
          () -> message);
      assertTrue(message.contains("multi-tenancy"), () -> message);
      assertTrue(message.contains("cluster per workflow module"), () -> message);

      // a configured 'none' is reported as the deliberate choice it is
      final var configured = warningsOf(() -> service.warnAboutUnscopedIdentifiers(MODULE, false));
      assertTrue(!configured.getFirst().contains("nothing is configured"), () -> configured.toString());
      // ... and the way out of the warning is part of it
      assertTrue(
          configured.getFirst().contains("vanillabp.adapters.myengine.accept-unscoped-identifiers: true"),
          () -> configured.toString());

    }

    @Test
    @DisplayName("Accepting unscoped identifiers deliberately silences the warning")
    public void acceptedUnscopedIdentifiersStaySilent() {

      final var configuration = new Camunda8AdapterConfiguration();
      configuration.setAcceptUnscopedIdentifiers(true);
      final var service = new Camunda8DeploymentService(
          "myengine", new Camunda8ClientFactory("myengine", configuration), new NoOpInvoker(), (
              m2,
              p2,
              t2) -> io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, java.time.Duration
                  .ofDays(14), null, null);

      assertEquals(
          java.util.List.of(),
          warningsOf(() -> service.warnAboutUnscopedIdentifiers(MODULE, true)),
          "the decision is on record, so there is nothing left to ask");

    }

  }

}
