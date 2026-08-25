package io.vanillabp.camunda8.deployment;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.camunda.client.api.command.DeployResourceCommandStep1;
import io.camunda.client.api.command.DeployResourceCommandStep1.DeployResourceCommandStep2;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.Process;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.Camunda8ReleaseLine;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.wiring.Camunda8JobHandler;
import io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver;
import io.vanillabp.camunda8.wiring.Camunda8TaskWiring;
import io.vanillabp.integration.adapter.spi.AdapterDeploymentService;
import io.vanillabp.integration.adapter.spi.AdapterPlatformVersion;
import io.vanillabp.integration.adapter.spi.BpmnParseException;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskInvoker;
import lombok.extern.slf4j.Slf4j;

/**
 * Camunda 8 implementation of the {@link AdapterDeploymentService}. One instance is
 * created per configured adapter ID (not per adapter type) because the same BPMS type
 * may be configured multiple times (BPMS migration).
 * <p>
 * The BPMN model type is {@link BpmnModelInstance}, shipped with the Camunda 8 client via
 * {@code io.camunda:zeebe-bpmn-model}. The processing context is
 * {@link Camunda8ProcessingContext}, which collects all deployable resources of a workflow
 * module so they are deployed in a single {@code DeployResourceCommand}.
 * <p>
 * Task wiring ({@code wireBpmn}) validates the BPMN's job-worker tasks
 * (zeebe:taskDefinition) against the registered {@code @WorkflowTask} methods;
 * {@code startWorkflowProcessing} opens one polling job worker per task definition
 * (closed on {@code stopWorkflowProcessing}).
 */
@Slf4j
// see decision 4 in the repository's DECISIONS.md
@SuppressWarnings("LombokSetterMayBeUsed")
public class Camunda8DeploymentService implements AdapterDeploymentService<BpmnModelInstance, Camunda8ProcessingContext> {

  /**
   * The adapter type of the Camunda 8 adapter. Constant across all instances; the
   * adapter ID (see {@link #getAdapterId()}) distinguishes instances.
   */
  public static final String ADAPTER_TYPE = io.vanillabp.camunda8.Camunda8Adapter.ADAPTER_TYPE;

  private final String adapterId;

  private final Camunda8ClientFactory clientFactory;

  /**
   * The core's task-processing entry point: wiring validation during
   * {@link #wireBpmn} and job dispatch at runtime.
   */
  private final WorkflowTaskInvoker workflowTaskInvoker;

  /**
   * What this adapter knows about the multi-instance elements of the processes it
   * deployed. Filled while wiring a model, read while dispatching a job - a job
   * carries the ID of its own element and nothing about the iterations enclosing it.
   */
  private final io.vanillabp.camunda8.wiring.Camunda8MultiInstance.Registry multiInstanceRegistry = new io.vanillabp.camunda8.wiring.Camunda8MultiInstance.Registry();

  /**
   * The core's entry point for workflows the cluster starts on its own:
   * the start events of a process are reported here while wiring, and the start
   * execution-listener workers dispatch through it. May be <code>null</code> (tests).
   */
  private io.vanillabp.integration.adapter.spi.workflowstart.BpmsInitiatedStartInvoker bpmsInitiatedStartInvoker;

  /**
   * The core's entry point for workflows which ended. May be
   * <code>null</code> (tests) - no end listener is attached then.
   */
  private io.vanillabp.integration.adapter.spi.workflowend.WorkflowEndedInvoker workflowEndedInvoker;

  /**
   * Hands over the core's entry point for workflows which ended.
   *
   * @param workflowEndedInvoker The core's invoker
   */
  public void setWorkflowEndedInvoker(
      final io.vanillabp.integration.adapter.spi.workflowend.WorkflowEndedInvoker workflowEndedInvoker) {

    this.workflowEndedInvoker = workflowEndedInvoker;

  }

  /**
   * Hands over the core's entry point for workflows the cluster starts on its own.
   *
   * @param bpmsInitiatedStartInvoker The core's invoker
   */
  public void setBpmsInitiatedStartInvoker(
      final io.vanillabp.integration.adapter.spi.workflowstart.BpmsInitiatedStartInvoker bpmsInitiatedStartInvoker) {

    this.bpmsInitiatedStartInvoker = bpmsInitiatedStartInvoker;

  }

  /**
   * Whether a worker asks the cluster for the variables this adapter derived or for all
   * of them. Handed in by the platform module after construction rather than
   * through the constructor, whose parameter list is long enough; <code>null</code>
   * (tests) means the default, which is the derived list.
   */
  private io.vanillabp.camunda8.wiring.Camunda8FetchVariablesResolver fetchVariablesResolver;

  /**
   * Hands over how <code>fetch-variables</code> resolves for this adapter instance.
   *
   * @param fetchVariablesResolver The resolver, or <code>null</code> for the default
   */
  public void setFetchVariablesResolver(
      final io.vanillabp.camunda8.wiring.Camunda8FetchVariablesResolver fetchVariablesResolver) {

    this.fetchVariablesResolver = fetchVariablesResolver;

  }

  /**
   * What this adapter instance measures on top of what the core measures.
   * Handed in by the platform module after construction, because it exists once per
   * application while deployment services exist per adapter id;
   * {@link io.vanillabp.camunda8.observability.Camunda8Metrics#NONE} for an application
   * without a metrics backend.
   */
  private io.vanillabp.camunda8.observability.Camunda8Metrics metrics = io.vanillabp.camunda8.observability.Camunda8Metrics.NONE;

  /**
   * Hands over what to measure into, and registers the execution slots of this adapter
   * instance right away - the client is built before this, so there is nothing to wait
   * for.
   *
   * @param metrics What to measure into, never <code>null</code>
   */
  public void setMetrics(
      final io.vanillabp.camunda8.observability.Camunda8Metrics metrics) {

    this.metrics = metrics == null
        ? io.vanillabp.camunda8.observability.Camunda8Metrics.NONE
        : metrics;
    registerExecutionSlots();

  }

  /**
   * Publishes how many handlers this adapter instance may run, how many of them run right
   * now and how many jobs wait for a slot.
   * <p>
   * The last two exist only in the virtual-thread mode, where the adapter's own
   * executor holds the bound. In the platform-thread mode the client owns its own pool and does
   * not report what it does with it, so those two gauges are absent rather than guessed.
   */
  private void registerExecutionSlots() {

    final var executionModel = clientFactory.getExecutionModel();
    final var executor = clientFactory.getVirtualThreadExecutor();
    metrics
        .registerExecutionSlots(
            adapterId,
            executionModel::slots,
            executor == null
                ? null
                : () -> executor.getBound() - executor.getFreeSlots(),
            executor == null
                ? null
                : executor::getWaiting);

  }

  @Override
  public io.vanillabp.integration.adapter.spi.health.AdapterHealth checkHealth() {

    return io.vanillabp.camunda8.health.Camunda8Health.check(adapterId, clientFactory);

  }

  /**
   * What this adapter instance does with a task the core reports as older than
   * <code>vanillabp.delivery.max-task-age</code>. Read from the adapter's own
   * configuration rather than passed through the wiring, because it belongs to the
   * connection like every other adapter-level key; a setup without the resolver (tests)
   * reports only.
   *
   * @return The action, never <code>null</code>
   */
  private io.vanillabp.camunda8.client.Camunda8AdapterConfiguration.AsyncTaskMaxAgeAction asyncTaskMaxAgeAction() {

    if (configurations == null) {
      return io.vanillabp.camunda8.client.Camunda8AdapterConfiguration.AsyncTaskMaxAgeAction.REPORT;
    }
    final var configuration = configurations.apply(adapterId);
    return (configuration == null) || (configuration.getAsyncTaskMaxAgeAction() == null)
        ? io.vanillabp.camunda8.client.Camunda8AdapterConfiguration.AsyncTaskMaxAgeAction.REPORT
        : configuration.getAsyncTaskMaxAgeAction();

  }

  /**
   * How long this adapter instance's shutdown waits for the handlers it has in flight.
   * Read from the adapter's own configuration rather than passed through the
   * wiring, because it belongs to the connection like every other adapter-level key; a
   * setup without the resolver (tests) uses the default.
   *
   * @return The grace period, never <code>null</code>
   */
  private Duration shutdownGrace() {

    if (configurations == null) {
      return io.vanillabp.camunda8.client.Camunda8AdapterConfiguration.DEFAULT_SHUTDOWN_GRACE;
    }
    final var configuration = configurations.apply(adapterId);
    return configuration == null
        ? io.vanillabp.camunda8.client.Camunda8AdapterConfiguration.DEFAULT_SHUTDOWN_GRACE
        : configuration.resolvedShutdownGrace();

  }

  /**
   * What each workflow module of this adapter instance has in flight, and whether it is
   * going down. One per workflow module: stopping one module must not make the
   * handlers of another one believe they were cut off.
   */
  private final Map<String, io.vanillabp.camunda8.client.Camunda8Drain> drains = new java.util.concurrent.ConcurrentHashMap<>();

  /**
   * @param workflowModuleId The workflow module
   * @return The drain of that module, created on first use
   */
  io.vanillabp.camunda8.client.Camunda8Drain drainOf(
      final String workflowModuleId) {

    return drains.computeIfAbsent(
        workflowModuleId,
        moduleId -> new io.vanillabp.camunda8.client.Camunda8Drain(adapterId, moduleId));

  }

  /**
   * Gives a workflow module which starts processing a drain which is NOT shutting down.
   * A module can be started again after it was stopped - a checkpoint and restore, or a
   * platform which restarts its lifecycle beans - and the drain of the previous run stays
   * marked as shutting down forever, which would keep every handler of the new run from
   * ever reporting a failed job.
   *
   * @param workflowModuleId The workflow module
   * @return The fresh drain the new workers register their deliveries in
   */
  private io.vanillabp.camunda8.client.Camunda8Drain freshDrainOf(
      final String workflowModuleId) {

    final var drain = new io.vanillabp.camunda8.client.Camunda8Drain(adapterId, workflowModuleId);
    drains.put(workflowModuleId, drain);
    return drain;

  }

  /**
   * Resolves the per-task job timeout from the adapter's configuration overlay
   * (task &gt; workflow &gt; workflow-module &gt; adapter, most specific wins).
   */
  private final Camunda8JobTimeoutResolver jobTimeoutResolver;

  /**
   * Resolves how long the cluster waits before it hands a FAILED job out again,
   * from the same four levels the job timeout comes from. Unlike the timeout this is not a
   * property of the worker but of each fail command, so nothing has to be aligned between
   * the processes one worker serves. May be <code>null</code> (tests): the default of ten
   * seconds applies then.
   */
  private final io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver retryBackoffResolver;

  /**
   * The window the lock of a job left open by a {@code @TaskId} handler is renewed in
   * (see {@link Camunda8JobHandler}).
   */
  private final Duration asyncTaskLockRenewal;

  /**
   * Resolves an adapter id's connection configuration - platform-supplied, used by
   * {@link #validateDistinctAdapterInstances(List)}. May be <code>null</code>
   * (tests): the check is skipped then.
   */
  private final java.util.function.Function<String, io.vanillabp.camunda8.client.Camunda8AdapterConfiguration> configurations;

  /**
   * The core's name-clash-avoidance model: decides whether a workflow
   * module is isolated by a TENANT ({@code by-adapter}, version 1's behavior), by
   * PREFIXING the identifiers ({@code use-prefix} - no tenant, which is what makes
   * tenant licenses avoidable) or not at all ({@code none}, this adapter's default).
   * May be <code>null</code> (tests): nothing is scoped then.
   */
  private final io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport scoping;

  /**
   * The tenants already verified against the cluster - asked once per tenant, not once
   * per workflow module (several modules may share a configured tenant).
   */
  private final java.util.Set<String> verifiedTenants = java.util.concurrent.ConcurrentHashMap.newKeySet();

  /**
   * Whether the configured tenant was already checked against the mode (once per adapter
   * instance, the check is adapter-wide).
   */
  private boolean tenantConfigurationValidated;

  /**
   * Convenience constructor without the configuration resolver (tests) - two
   * adapter ids of this type are not checked for distinctness then.
   */
  public Camunda8DeploymentService(
      final String adapterId,
      final Camunda8ClientFactory clientFactory,
      final WorkflowTaskInvoker workflowTaskInvoker,
      final Camunda8JobTimeoutResolver jobTimeoutResolver,
      final Duration asyncTaskLockRenewal) {

    this(adapterId, clientFactory, workflowTaskInvoker, jobTimeoutResolver, asyncTaskLockRenewal, null, null);

  }

  /**
   * Convenience constructor without the name-clash-avoidance support (tests).
   */
  public Camunda8DeploymentService(
      final String adapterId,
      final Camunda8ClientFactory clientFactory,
      final WorkflowTaskInvoker workflowTaskInvoker,
      final Camunda8JobTimeoutResolver jobTimeoutResolver,
      final Duration asyncTaskLockRenewal,
      final java.util.function.Function<String, io.vanillabp.camunda8.client.Camunda8AdapterConfiguration> configurations) {

    this(adapterId, clientFactory, workflowTaskInvoker, jobTimeoutResolver, asyncTaskLockRenewal, configurations, null);

  }

  public Camunda8DeploymentService(
      final String adapterId,
      final Camunda8ClientFactory clientFactory,
      final WorkflowTaskInvoker workflowTaskInvoker,
      final Camunda8JobTimeoutResolver jobTimeoutResolver,
      final Duration asyncTaskLockRenewal,
      final java.util.function.Function<String, io.vanillabp.camunda8.client.Camunda8AdapterConfiguration> configurations,
      final io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport scoping) {

    this(adapterId, clientFactory, workflowTaskInvoker, jobTimeoutResolver, asyncTaskLockRenewal, configurations, scoping, null);

  }

  public Camunda8DeploymentService(
      final String adapterId,
      final Camunda8ClientFactory clientFactory,
      final WorkflowTaskInvoker workflowTaskInvoker,
      final Camunda8JobTimeoutResolver jobTimeoutResolver,
      final Duration asyncTaskLockRenewal,
      final java.util.function.Function<String, io.vanillabp.camunda8.client.Camunda8AdapterConfiguration> configurations,
      final io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport scoping,
      final io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver retryBackoffResolver) {

    this.retryBackoffResolver = retryBackoffResolver;

    AdapterPlatformVersion.requireCompatiblePlatform(ADAPTER_TYPE, Camunda8DeploymentService.class);

    // which release line this application runs, once per adapter id: the client named
    // here is the LOWEST cluster version these artifacts accept, and a reader comparing
    // it to their cluster sees at a glance whether they are on the right line
    log.info(
        "Camunda8[{}]: release line {} of the adapter, built against Camunda client {}, "
            + "which is the lowest cluster version it accepts",
        adapterId,
        Camunda8ReleaseLine.id(),
        Camunda8ReleaseLine.clientVersion());

    this.adapterId = adapterId;
    this.clientFactory = clientFactory;
    this.workflowTaskInvoker = workflowTaskInvoker;
    this.jobTimeoutResolver = jobTimeoutResolver;
    this.asyncTaskLockRenewal = asyncTaskLockRenewal;
    this.configurations = configurations;
    this.scoping = scoping;
    // What the cluster's process definitions are versioned as - the version
    // travels with every job, the version TAGS come from here
    this.processVersions = new Camunda8ProcessVersions(
        adapterId, clientFactory::getClient, this::scopedProcessId, this::tenantIdOf);

  }

  /**
   * The versions of this cluster's process definitions: the catalog the core
   * resolves version TAGS through. The version itself travels with every job.
   */
  private final Camunda8ProcessVersions processVersions;

  /**
   * The BPMN process id as the CLUSTER knows it - the model carries the
   * scoped ids after {@code prepareBpmn}, while the core is keyed by the plain ones.
   */
  private String scopedProcessId(
      final String workflowModuleId,
      final String bpmnProcessId) {

    return scoping == null
        ? bpmnProcessId
        : scoping.scopedProcessId(workflowModuleId, bpmnProcessId, adapterId);

  }

  /**
   * The inverse of {@link #scopedProcessId}.
   */
  private String plainProcessId(
      final String workflowModuleId,
      final String scopedBpmnProcessId) {

    return scoping == null
        ? scopedBpmnProcessId
        : scoping.plainProcessId(workflowModuleId, scopedBpmnProcessId, adapterId);

  }

  /**
   * The identifier as the application modelled it - the model carries the scoped one
   * where the workflow module prefixes its identifiers.
   */
  private String plainIdentifier(
      final String workflowModuleId,
      final String scopedIdentifier) {

    return (scoping == null) || (scopedIdentifier == null)
        ? scopedIdentifier
        : scoping.plainIdentifier(workflowModuleId, scopedIdentifier, adapterId);

  }

  /**
   * The task definition as the core knows it - the model (and therefore the job type
   * a worker subscribes to) carries the scoped one.
   */
  private String plainTaskDefinition(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String scopedTaskDefinition) {

    return scoping == null
        ? scopedTaskDefinition
        : scoping.plainTaskDefinition(workflowModuleId, bpmnProcessId, scopedTaskDefinition, adapterId);

  }

  /**
   * Fails the boot if a tenant is configured for this adapter id although no workflow
   * module is deployed into one, i.e. the mode says {@code none} or {@code use-prefix}
   * everywhere. Whether a tenant is what only {@code by-adapter} can use is this
   * adapter's knowledge; the core answers which modes apply. Checked once per adapter
   * instance while deploying, before anything reaches the cluster.
   */
  private void validateTenantConfiguration() {

    if (tenantConfigurationValidated || (scoping == null)) {
      return;
    }
    tenantConfigurationValidated = true;
    final var configuredTenantId = clientFactory
        .getConfiguration()
        .getTenantId();
    if ((configuredTenantId == null) || configuredTenantId.isBlank()) {
      return;
    }
    scoping.validateNoneNameClashStrategy(
        adapterId,
        io.vanillabp.camunda8.client.Camunda8AdapterConfiguration.propertyKey(adapterId, "tenant-id"));

  }

  /**
   * The tenant a workflow module is deployed to, respectively its operations are
   * executed in - decided by the name-clash-avoidance mode, with the
   * adapter's configured <code>tenant-id</code> naming it under
   * {@code by-adapter}.
   *
   * @param workflowModuleId The workflow module ID
   * @return The tenant ID or <code>null</code> if no tenant is used
   */
  private String tenantIdOf(
      final String workflowModuleId) {

    return io.vanillabp.camunda8.wiring.Camunda8Scoping.tenantIdFor(
        scoping, workflowModuleId, adapterId, clientFactory
            .getConfiguration()
            .getTenantId());

  }

  /**
   * Two <code>camunda8</code> adapter ids are only distinct if they address
   * different clusters - or one cluster with different credentials/tenants (see
   * {@link io.vanillabp.camunda8.client.Camunda8InstanceIdentity}).
   */
  @Override
  public void validateDistinctAdapterInstances(
      final List<String> adapterIdsOfThisType) {

    io.vanillabp.camunda8.client.Camunda8InstanceIdentity
        .validateDistinct(adapterIdsOfThisType, configurations, scoping);

  }

  /**
   * Camunda 8 keeps the SPI's default, {@code by-adapter}, which on this BPMS means a
   * TENANT named after the workflow module. That is what VanillaBP 1 deployed when
   * nothing was configured (its {@code use-tenants} was on and the tenant id defaulted
   * to the workflow module id), so an application upgrading from version 1 without
   * touching its configuration keeps addressing the workflows it started back then.
   * <p>
   * <b>What it asks of the cluster.</b> A tenant id other than {@code <default>} needs
   * multi-tenancy enabled, and the tenant has to exist: a cluster started from the
   * stock image answers a deploy command carrying one with "multi-tenancy is
   * disabled". {@link io.vanillabp.camunda8.client.Camunda8TenantCheck} turns that into
   * a boot failure naming the properties leading out, which is the point: the
   * alternative would be a default which quietly deploys every workflow module into
   * the {@code <default>} tenant, and that is not a weaker isolation but none at all -
   * {@code none} by another name, without the warning {@code none} carries.
   * <p>
   * An application on such a cluster says so once, with
   * {@code vanillabp.adapters.<id>.name-clash-avoidance: none} (version 1's
   * {@code use-tenants: false}) or {@code use-prefix}, which needs no cluster support
   * at all. This default stood at {@code none} between 2026-08-11 and 2026-08-22,
   * which was the defect; {@code Camunda8DeploymentServiceTest} holds it now.
   */
  @Override
  public io.vanillabp.integration.adapter.spi.NameClashAvoidance defaultNameClashAvoidance() {

    return io.vanillabp.integration.adapter.spi.NameClashAvoidance.BY_ADAPTER;

  }

  /**
   * Names what Camunda 8 offers instead of {@code none}: prefixing, a tenant per
   * workflow module (which needs a cluster with multi-tenancy enabled) or a cluster
   * per workflow module.
   * <p>
   * Silent if the application accepted unscoped identifiers deliberately
   * ({@code vanillabp.adapters.<id>.accept-unscoped-identifiers}) - the point of the
   * warning is the DECISION, and once it is on record there is nothing left to ask.
   */
  @Override
  public void warnAboutUnscopedIdentifiers(
      final String workflowModuleId,
      final boolean fromDefault) {

    if (clientFactory
        .getConfiguration()
        .isAcceptUnscopedIdentifiers()) {
      log.debug(
          "Camunda8[{}]: workflow module '{}' is deployed with name-clash-avoidance 'none', accepted by "
              + "'{}'",
          adapterId,
          workflowModuleId,
          io.vanillabp.camunda8.client.Camunda8AdapterConfiguration
              .propertyKey(adapterId, "accept-unscoped-identifiers"));
      return;
    }
    log.warn(
        """
            Workflow module '{}' is deployed to Camunda 8 (adapter '{}') with name-clash-avoidance \
            'none'{}. Its identifiers reach the cluster as they are - BPMN process ids, message and \
            signal names, error codes, job types and user-task form references - so a second workflow \
            module using the same identifier addresses the very same processes and jobs, and neither \
            VanillaBP nor the cluster can tell. Keep 'none' only as long as your identifiers are \
            unique across ALL workflow modules of this application. Otherwise choose:
              vanillabp.adapters.{}.name-clash-avoidance: use-prefix   # VanillaBP prefixes the identifiers, no tenant needed
              vanillabp.adapters.{}.name-clash-avoidance: by-adapter   # a tenant per workflow module - only on a cluster with multi-tenancy enabled
            A third option is a Camunda 8 cluster per workflow module, configured as one adapter id \
            per cluster. The same key may be set per workflow module \
            (vanillabp.workflow-modules.{}.adapters.{}.name-clash-avoidance). The mode is not a \
            runtime switch - changing it once workflows are running is a BPMS migration. If the \
            identifiers ARE unique, say so once and this warning is gone:
              vanillabp.adapters.{}.accept-unscoped-identifiers: true""",
        workflowModuleId,
        adapterId,
        fromDefault
            ? " (nothing is configured, so the adapter's default applies)"
            : "",
        adapterId,
        adapterId,
        workflowModuleId,
        adapterId,
        adapterId);

  }

  @Override
  public String getAdapterId() {

    return adapterId;

  }

  @Override
  public String getAdapterType() {

    return ADAPTER_TYPE;

  }

  @Override
  public Class<BpmnModelInstance> getModelType() {

    return BpmnModelInstance.class;

  }

  @Override
  public Class<Camunda8ProcessingContext> getProcessContextType() {

    return Camunda8ProcessingContext.class;

  }

  @Override
  public List<Map.Entry<String, BpmnModelInstance>> readBpmn(
      final String workflowModuleId,
      final String filename,
      final InputStream bpmn,
      final boolean isVanillaBpBpmn) throws BpmnParseException {

    final BpmnModelInstance model;
    try {
      model = Bpmn.readModelFromStream(bpmn);
    } catch (final RuntimeException e) {
      throw new BpmnParseException(
          "Failed to parse BPMN file '%s' of workflow module '%s'!".formatted(filename, workflowModuleId), e);
    }

    // one entry per executable process; the value is always the whole model since
    // Camunda 8 deploys the entire file as one resource (a file may hold several
    // executable processes)
    final var executableProcesses = new ArrayList<Map.Entry<String, BpmnModelInstance>>();
    for (final var process : model.getModelElementsByType(Process.class)) {
      if (!process.isExecutable()) {
        continue;
      }
      executableProcesses.add(Map.entry(process.getId(), model));
    }
    return executableProcesses;

  }

  @Override
  public Camunda8ProcessingContext prepareBpmn(
      final String workflowModuleId,
      final Camunda8ProcessingContext existingContext,
      final String filename,
      final String bpmnProcessId,
      final BpmnModelInstance model) {

    // the core passes null for the first BPMN process of a workflow module
    final var context = existingContext != null
        ? existingContext
        : new Camunda8ProcessingContext(workflowModuleId);
    // Rewrite the identifiers the cluster resolves globally BEFORE wiring,
    // so everything downstream (wiring validation, listener injection, workers) sees
    // what the cluster will see. A no-op unless the mode is 'use-prefix'. The core
    // calls prepareBpmn once per executable PROCESS while all processes of a file
    // share ONE model, so scoping has to happen once per FILE - otherwise a
    // multi-process file would collect one prefix per process.
    final var modelAlreadyScoped = context
        .getResources()
        .containsKey(filename);
    if (!modelAlreadyScoped) {
      io.vanillabp.camunda8.wiring.Camunda8Scoping.apply(model, workflowModuleId, adapterId, scoping);
    }
    context.addResource(filename, model);
    context.recordDeployedProcess(bpmnProcessId);
    return context;

  }

  @Override
  public void wireBpmn(
      final String workflowModuleId,
      final String filename,
      final String bpmnProcessId,
      final BpmnModelInstance model,
      final Camunda8ProcessingContext context) {

    // the model carries the identifiers the CLUSTER will know (prepareBpmn rewrote
    // them in mode 'use-prefix'), while the core is keyed by the plain ones - so the
    // model is searched by the SCOPED process id and the invoker is called with the
    // plain one
    final var scopedBpmnProcessId = scopedProcessId(workflowModuleId, bpmnProcessId);
    // extract the job-worker tasks (zeebe:taskDefinition type = VanillaBP task
    // definition) and validate them against the registered @WorkflowTask methods;
    // throwing here honors the deployment-failure policy
    final var tasks = Camunda8TaskWiring.tasksOf(model, scopedBpmnProcessId);
    // Camunda-managed user tasks: the V1-compatible lifecycle task
    // listeners are ADDED TO THE MODEL here (wireBpmn is the BPMN-modification
    // stage of the pipeline) - the modified model is what deployResources deploys
    final var userTasks = Camunda8TaskWiring.userTasksOf(model, scopedBpmnProcessId, workflowModuleId, filename);
    final var specs = new java.util.ArrayList<io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec>();
    tasks
        .stream()
        .map(task -> new io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec(
            task.activityId(), plainTaskDefinition(workflowModuleId, bpmnProcessId, task.taskDefinition())))
        .forEach(specs::add);
    userTasks
        .stream()
        .map(userTask -> io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec.userTask(
            userTask.activityId(),
            plainTaskDefinition(workflowModuleId, bpmnProcessId, userTask.externalFormReference())))
        .forEach(specs::add);
    workflowTaskInvoker.validateTaskWiring(workflowModuleId, bpmnProcessId, specs);
    // The same extraction serves the models of OLDER versions the cluster
    // still holds, so both directions see a model the same way
    processVersions.setTasksOfModel(this::taskSpecsOf);

    // The cluster can be asked which versions of this process it has, which
    // is what a version specification naming a version TAG needs
    workflowTaskInvoker
        .registerProcessVersions(adapterId, workflowModuleId, bpmnProcessId, processVersions);

    // Which elements can put a second token into a running workflow - two
    // tokens are two writers on the workflow aggregate, and the core knows whether
    // that aggregate can survive them
    workflowTaskInvoker
        .reportConcurrentTokenElements(
            workflowModuleId,
            bpmnProcessId,
            Camunda8TaskWiring.concurrentTokenElementIdsOf(model, scopedBpmnProcessId));

    // message correlation: inject the correlation-key expression
    // '=<aggregate-ID variable>' into message subscriptions lacking one - the V2
    // convention enabling ProcessService#correlateMessage without manual model
    // tweaks (existing expressions stay untouched, V1 models deploy unchanged)
    Camunda8TaskWiring.wireMessageSubscriptions(
        model,
        scopedBpmnProcessId,
        () -> workflowTaskInvoker.resolveWorkflowAggregateIdName(workflowModuleId, bpmnProcessId));
    // multi-instance: the input mappings which make the element, the index
    // and the total of every iteration readable from a job are ADDED TO THE MODEL
    // here, and which iterations enclose which element is remembered for dispatch
    io.vanillabp.camunda8.wiring.Camunda8MultiInstance
        .wire(model, scopedBpmnProcessId, multiInstanceRegistry);
    context.getTasksToWire().addAll(tasks);
    context.getUserTasksToWire().addAll(userTasks);

    // start events the cluster fires on its own: the start execution
    // listener building the workflow aggregate is ADDED TO THE MODEL here as well
    if (bpmsInitiatedStartInvoker != null) {
      final var bpmsInitiatedStarts = Camunda8TaskWiring
          .bpmsInitiatedStartsOf(
              model,
              scopedBpmnProcessId,
              signalName -> plainIdentifier(workflowModuleId, signalName));
      bpmsInitiatedStartInvoker
          .validateBpmsInitiatedStarts(
              workflowModuleId,
              bpmnProcessId,
              bpmsInitiatedStarts
                  .stream()
                  .map(startEvent -> new io.vanillabp.integration.adapter.spi.workflowstart.BpmsInitiatedStartSpec(
                      startEvent.startEventId(), startEvent.kind(), startEvent.signalName(), null))
                  .toList());
      context.getBpmsInitiatedStartsToWire().addAll(bpmsInitiatedStarts);
    }

    // the end of a workflow is reported only where the application asked for it -
    // a model must not pay for a listener nobody wants
    if ((workflowEndedInvoker != null) && workflowEndedInvoker
        .workflowEndedHandlerExists(workflowModuleId, bpmnProcessId) && Camunda8TaskWiring
            .attachWorkflowEndedListener(model, scopedBpmnProcessId)) {
      context.getWorkflowEndedProcessesToWire().add(scopedBpmnProcessId);
    }

    log.info(
        "Camunda8[{}]: wired {} task(s) of BPMN process '{}' (file '{}', workflow module '{}')",
        adapterId,
        tasks.size(),
        bpmnProcessId,
        filename,
        workflowModuleId);

  }

  /**
   * The tasks of ONE model as the core validates them - used for the model this boot
   * deploys and for the models of older versions the cluster still holds.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The PLAIN BPMN process ID
   * @param version The version the cluster assigned, for messages
   * @param model The model of that version
   * @return The tasks of that model
   */
  private java.util.Collection<io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec> taskSpecsOf(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String version,
      final BpmnModelInstance model) {

    final var scopedBpmnProcessId = scopedProcessId(workflowModuleId, bpmnProcessId);
    final var specs = new java.util.ArrayList<io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec>();
    Camunda8TaskWiring
        .tasksOf(model, scopedBpmnProcessId)
        .stream()
        .map(task -> new io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec(
            task.activityId(), plainTaskDefinition(workflowModuleId, bpmnProcessId, task.taskDefinition())))
        .forEach(specs::add);
    Camunda8TaskWiring
        .userTasksOf(model, scopedBpmnProcessId, workflowModuleId, "version %s".formatted(version))
        .stream()
        .map(userTask -> io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec.userTask(
            userTask.activityId(),
            plainTaskDefinition(workflowModuleId, bpmnProcessId, userTask.externalFormReference())))
        .forEach(specs::add);
    return specs;

  }

  @Override
  public void deployResources(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext) throws IllegalStateException {

    if (bpmsProcessingContext == null || bpmsProcessingContext.isEmpty()) {
      log.info("No executable BPMN resources for workflow module '{}' and adapter '{}' - "
          + "nothing to deploy to Camunda 8", workflowModuleId, adapterId);
      return;
    }

    // one DeployResourceCommand per workflow module with all its models
    final var client = clientFactory.getClient();
    DeployResourceCommandStep2 command = null;
    for (final var resource : bpmsProcessingContext.getResources().entrySet()) {
      final DeployResourceCommandStep1 next = command != null ? command : client.newDeployResourceCommand();
      command = next.addProcessModel(resource.getValue(), resource.getKey());
    }

    // Which tenant a workflow module is deployed to is decided by the
    // name-clash-avoidance mode - 'by-adapter' (the default, version 1's behavior)
    // uses the workflow module id, overridable by the adapter's 'tenant-id';
    // 'use-prefix' and 'none' use no tenant at all (the identifiers were prefixed
    // respectively are unique by contract).
    validateTenantConfiguration();
    final var tenantId = tenantIdOf(workflowModuleId);
    if (tenantId != null) {
      // asking beforehand turns the cluster's "multi-tenancy is disabled" respectively an
      // unknown tenant into a message naming the property to change (GAPS G2)
      if (verifiedTenants.add(tenantId)) {
        io.vanillabp.camunda8.client.Camunda8TenantCheck
            .requireUsableTenant(adapterId, workflowModuleId, tenantId, client);
      }
      command = command.tenantId(tenantId);
    }
    // prefixing may not merge two different processes into one identifier
    if (scoping != null) {
      scoping.validateNoCollidingProcessIds(
          adapterId,
          bpmsProcessingContext
              .getDeployedProcessIds()
              .stream()
              .map(processId -> new io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport.DeployedProcess(
                  workflowModuleId, processId))
              .toList());
    }

    try {
      final var deployment = command
          .send()
          .join();
      // remember what was deployed: the viewer API serves definitions and BPMN XML
      // from these models instead of the eventually consistent query API (see
      // Camunda8DeployedProcesses)
      deployment
          .getProcesses()
          .forEach(process -> {
            final var model = bpmsProcessingContext
                .getResources()
                .get(process.getResourceName());
            if (model == null) {
              return;
            }
            // the cluster reports the id IT knows; the viewer API is keyed by the
            // PLAIN one, like every other core-facing identifier
            final var plainBpmnProcessId = scoping == null
                ? process.getBpmnProcessId()
                : scoping.plainProcessId(workflowModuleId, process.getBpmnProcessId(), adapterId);
            clientFactory
                .getDeployedProcesses()
                .record(
                    new Camunda8DeployedProcesses.DeployedProcess(
                        workflowModuleId, plainBpmnProcessId, String
                            .valueOf(process.getProcessDefinitionKey()), process.getVersion(), model));
            // The version the cluster just assigned, together with the
            // version tag of the model deployed - no query needed for either
            processVersions
                .recordDeployed(
                    workflowModuleId,
                    plainBpmnProcessId,
                    process.getVersion(),
                    Camunda8TaskWiring.versionTagOf(model, process.getBpmnProcessId()));
            // The border between the model this boot brought and the older
            // versions the cluster still holds
            workflowTaskInvoker
                .registerDeployedVersion(
                    adapterId, workflowModuleId, plainBpmnProcessId, String.valueOf(process.getVersion()));
          });

      log.info("Deployed {} BPMN resource(s) of workflow module '{}' to Camunda 8 "
          + "(adapter '{}', deployment key {}, tenant '{}'): {}",
          bpmsProcessingContext.getResources().size(),
          workflowModuleId,
          adapterId,
          deployment.getKey(),
          tenantId != null && !tenantId.isBlank() ? tenantId : "<default>",
          bpmsProcessingContext.getResources().keySet());
    } catch (final RuntimeException e) {
      throw new IllegalStateException(
          "Failed to deploy BPMN resources of workflow module '%s' to Camunda 8 (adapter '%s')!"
              .formatted(workflowModuleId, adapterId), e);
    }

    // after ALL processes of the module were wired: methods matching no task of
    // any wired process are a defect (per-module check, honors the policy)
    workflowTaskInvoker.validateNoUnwiredWorkflowTaskMethods(workflowModuleId);

    // The deployment is done, so the version tags the application's
    // annotations name can be resolved against what the cluster has now
    workflowTaskInvoker.resolveProcessVersions(workflowModuleId);

    // Two adapter ids on one cluster are told apart by the scope a workflow
    // was deployed under, and asking a KEY for its scope needs the query API
    failIfTwoAdapterIdsShareAClusterWithoutQueryApi();

  }

  /**
   * Whether the check below already ran for this adapter id.
   */
  private final java.util.concurrent.atomic.AtomicBoolean sharedClusterChecked = new java.util.concurrent.atomic.AtomicBoolean();

  /**
   * Ends the boot where two <code>camunda8</code> adapter ids address one cluster whose
   * query API is unavailable.
   * <p>
   * On a shared cluster every key is global, so the election has to ask which scope a job
   * respectively a user task belongs to before an adapter claims it, and that question can
   * only be answered by the query API. Without secondary storage the two ids are
   * indistinguishable and the first entry of <code>prioritized-adapters</code> silently
   * wins every operation of both, which routes messages into the wrong scope and writes a
   * changed aggregate into the wrong instance. An application with ONE Camunda 8 adapter is
   * not affected and keeps working without secondary storage.
   */
  private void failIfTwoAdapterIdsShareAClusterWithoutQueryApi() {

    if (!clientFactory.sharesItsCluster() || !sharedClusterChecked.compareAndSet(false, true)) {
      return;
    }
    try {
      clientFactory
          .getClient()
          .newProcessInstanceSearchRequest()
          .page(page -> page.limit(1))
          .send()
          .join();
    } catch (final RuntimeException e) {
      if (!secondaryStorageMissing(e)) {
        // any other failure is not this check's business - the cluster answered, and
        // whatever it answered is reported by whoever asked next
        return;
      }
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' shares its cluster with the adapter id(s) '%s', and that cluster \
              runs WITHOUT secondary storage. Two adapter ids on one cluster are told apart by the \
              scope they deployed under (the tenant respectively the prefixed process id), and the \
              key of a job or user task carries neither - the query API is what maps a key to its \
              scope. Without it VanillaBP would route every operation to the first entry of \
              'vanillabp.prioritized-adapters', which sends messages into the wrong scope and writes \
              a changed workflow aggregate into the wrong instance, all without an error. Configure \
              secondary storage for this cluster (camunda.data.secondaryStorage), or give each \
              adapter id a cluster of its own."""
              .formatted(
                  adapterId,
                  String.join("', '", clientFactory.getAdapterIdsSharingTheCluster())), e);
    }

  }

  /**
   * @param throwable What a query-API request failed with
   * @return Whether the cluster answered that it has no secondary storage
   */
  private static boolean secondaryStorageMissing(
      final Throwable throwable) {

    var current = throwable;
    while (current != null) {
      final var message = current.getMessage();
      if ((message != null) && message.contains("secondary storage")) {
        return true;
      }
      current = current.getCause();
    }
    return false;

  }

  /**
   * The options every worker of this adapter shares. Everything a worker inherits from the
   * client (<code>max-jobs-active</code>, <code>poll-interval</code>,
   * <code>request-timeout</code>, <code>stream-enabled</code>) is set on the CLIENT while it
   * is built, so an environment variable can still overrule it and be reported for it; only
   * <code>stream-timeout</code> has no client-wide equivalent and is set here.
   *
   * @param builder The worker builder
   * @return The same builder
   */
  io.camunda.client.api.worker.JobWorkerBuilderStep1.JobWorkerBuilderStep3 applyWorkerOptions(
      final io.camunda.client.api.worker.JobWorkerBuilderStep1.JobWorkerBuilderStep3 builder,
      final String jobType) {

    // the client's own counters: it activates and hands over jobs long before
    // the core sees a delivery, so this is where the queue in front of the execution
    // slots becomes visible
    final var withMetrics = builder.metrics(metrics.workerMetrics(adapterId, jobType));
    final var streamTimeout = clientFactory.getConfiguration().getStreamTimeout();
    return streamTimeout == null
        ? withMetrics
        : withMetrics.streamTimeout(streamTimeout);

  }

  /**
   * The lock of a worker which serves no task: the user-task lifecycle listeners, the start
   * events the cluster fires itself and the processes whose end is reported. All three run
   * application code inside a transaction exactly like a task does, so they are resolved the
   * way a task's <code>job-timeout</code> is - at adapter, workflow-module and workflow
   * level, there being no task to key them by - and they default to the same five minutes.
   * <p>
   * A worker subscribes by job type, and one user-task listener job type may belong to
   * several BPMN processes of the module. Where those resolve to different locks the
   * deployment fails guiding, the same way conflicting job timeouts of one task definition
   * do.
   *
   * @param workflowModuleId The workflow module
   * @param bpmnProcessIds The BPMN processes this worker serves (scoped ids)
   * @param kind What kind of worker it is, for the message
   * @param jobType The job type the worker subscribes to
   * @return The resolved lock
   */
  Duration listenerLockOf(
      final String workflowModuleId,
      final List<String> bpmnProcessIds,
      final String kind,
      final String jobType) {

    Duration resolved = null;
    String resolvedFor = null;
    for (final var bpmnProcessId : bpmnProcessIds) {
      final var plainBpmnProcessId = plainProcessId(workflowModuleId, bpmnProcessId);
      final var timeout = jobTimeoutResolver.jobTimeoutFor(workflowModuleId, plainBpmnProcessId, null);
      if (resolved == null) {
        resolved = timeout;
        resolvedFor = plainBpmnProcessId;
      } else if (!resolved.equals(timeout)) {
        throw new IllegalStateException(
            """
                The %s worker '%s' of workflow module '%s' serves the BPMN processes '%s' and '%s', \
                whose resolved job timeouts CONFLICT (%s vs. %s)! One worker serves a job type, so \
                its lock has to be the same for every process using it - align \
                'vanillabp.workflow-modules.%s.workflows.<workflow>.adapters.%s.job-timeout' for \
                those processes."""
                .formatted(
                    kind,
                    jobType,
                    workflowModuleId,
                    resolvedFor,
                    plainBpmnProcessId,
                    resolved,
                    timeout,
                    workflowModuleId,
                    adapterId));
      }
    }
    return resolved == null
        ? Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT
        : resolved;

  }

  /**
   * The variable a BPMN process carries the workflow aggregate's ID in - the one variable
   * every worker of this adapter reads. A process no workflow service serves cannot
   * happen after the wiring validation; if it does, the derivation gives up on the whole
   * worker rather than building a list which is missing exactly the name the handler
   * needs.
   *
   * @param workflowModuleId The workflow module
   * @param plainBpmnProcessId The BPMN process id as the core knows it
   * @return The variable's name, or <code>null</code> if the core cannot tell
   */
  private String aggregateIdNameOf(
      final String workflowModuleId,
      final String plainBpmnProcessId) {

    try {
      return workflowTaskInvoker.resolveWorkflowAggregateIdName(workflowModuleId, plainBpmnProcessId);
    } catch (final RuntimeException e) {
      log.debug(
          "Camunda8[{}]: the BPMN process '{}' of workflow module '{}' has no known workflow "
              + "aggregate - its workers ask for all variables",
          adapterId,
          plainBpmnProcessId,
          workflowModuleId,
          e);
      return null;
    }

  }

  /**
   * What one worker asks the cluster for: the union of the aggregate-ID
   * variables, multi-instance contexts and declared <code>&#64;TaskParam</code> names of
   * everything it serves, unless a level of the configuration says <code>all</code>.
   *
   * @param workflowModuleId The workflow module
   * @param served The elements this worker serves, as (scoped BPMN process id, BPMN
   *          element id, plain task definition or <code>null</code>)
   * @return The selection, never <code>null</code>
   */
  io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Selection fetchVariablesOf(
      final String workflowModuleId,
      final List<ServedElement> served) {

    final var variables = new java.util.TreeSet<String>();
    for (final var element : served) {
      final var plainBpmnProcessId = plainProcessId(workflowModuleId, element.scopedBpmnProcessId());
      final var mode = io.vanillabp.camunda8.wiring.Camunda8FetchVariablesResolver
          .resolve(fetchVariablesResolver, workflowModuleId, plainBpmnProcessId, element.taskDefinition());
      if (mode == io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Mode.ALL) {
        // one worker serves a job type, so the two values cannot both apply - and
        // fetching more than derived is never wrong, only more expensive
        return io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Selection.everything();
      }
      final var aggregateIdName = aggregateIdNameOf(workflowModuleId, plainBpmnProcessId);
      if (aggregateIdName == null) {
        return io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Selection.everything();
      }
      if (element.elementId() == null) {
        // the workflow-end listener: it reports a process rather than an element, and a
        // @WorkflowEnded method cannot declare a @TaskParam at all (the core rejects one),
        // so the aggregate's id is the complete answer here
        io.vanillabp.camunda8.wiring.Camunda8FetchVariables.collect(variables, aggregateIdName, List.of());
        continue;
      }
      io.vanillabp.camunda8.wiring.Camunda8FetchVariables.collect(
          variables,
          aggregateIdName,
          multiInstanceRegistry.chainOf(element.scopedBpmnProcessId(), element.elementId()));
      // and what the handlers of this element read with @TaskParam: the core
      // scanned those names off the methods while wiring, so the list is what the
      // application asks for rather than what the model happens to mention
      variables
          .addAll(
              workflowTaskInvoker
                  .taskParameterNames(workflowModuleId, plainBpmnProcessId, element.taskDefinition()));
    }
    return io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Selection.of(variables);

  }

  /**
   * One BPMN element a worker serves - what the fetch list is derived from.
   *
   * @param scopedBpmnProcessId The BPMN process id as the CLUSTER knows it (the
   *          multi-instance registry is keyed by it)
   * @param elementId The BPMN element id, or <code>null</code> where the worker serves a
   *          whole process rather than an element (the workflow-end listener), which is
   *          also the case where no <code>&#64;TaskParam</code> can occur
   * @param taskDefinition The task definition as the CORE knows it, or <code>null</code>
   *          where there is no task level to configure
   */
  record ServedElement(String scopedBpmnProcessId,
                       String elementId,
                       String taskDefinition) {
  }

  /**
   * Tells the worker what to ask for and says so once per worker, at DEBUG: when
   * somebody reports a variable their handler does not see any more, this line is the
   * first question answered.
   *
   * @param builder The worker builder
   * @param workflowModuleId The workflow module
   * @param kind What kind of worker it is, for the message
   * @param jobType The job type the worker subscribes to
   * @param selection What the worker asks for
   * @return The same builder
   */
  io.camunda.client.api.worker.JobWorkerBuilderStep1.JobWorkerBuilderStep3 applyFetchVariables(
      final io.camunda.client.api.worker.JobWorkerBuilderStep1.JobWorkerBuilderStep3 builder,
      final String workflowModuleId,
      final String kind,
      final String jobType,
      final io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Selection selection) {

    log.debug(
        "Camunda8[{}]: the {} worker '{}' of workflow module '{}' fetches {}",
        adapterId,
        kind,
        jobType,
        workflowModuleId,
        selection.describe());
    return selection.all()
        ? builder
        : builder.fetchVariables(selection.names());

  }

  @Override
  public void startWorkflowProcessing(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext) {

    // one polling worker per (adapter id, task definition): the job type routes
    // deliveries; tasks of DIFFERENT processes sharing a task definition are
    // served by one worker (job.getBpmnProcessId() routes to the right handlers).
    // The job timeout is resolved most-specific-wins - a task definition used by
    // several tasks with CONFLICTING configured timeouts fails guiding.
    final var timeoutsByDefinition = new LinkedHashMap<String, Duration>();
    // what each worker serves, which is what its fetch list is the union over
    final var servedByJobType = new LinkedHashMap<String, List<ServedElement>>();
    final var client = clientFactory.getClient();
    // what this module has in flight, and later whether it is going down: every handler
    // registers its delivery here, and stopWorkflowProcessing waits for them
    final var drain = freshDrainOf(workflowModuleId);
    // and the client learns that this module has workers open, so a shutdown path which
    // never reaches stopWorkflowProcessing does not close the client under them
    clientFactory.workflowModuleStarted(
        workflowModuleId,
        () -> stopWorkflowProcessing(workflowModuleId, bpmsProcessingContext));
    bpmsProcessingContext
        .getTasksToWire()
        .forEach(task -> {
          if (task.taskDefinition() == null) {
            return; // already reported by the wiring validation
          }
          // the records carry what the CLUSTER knows (the worker subscribes to it),
          // but the configuration is keyed by the PLAIN names
          final var plainBpmnProcessId = plainProcessId(workflowModuleId, task.bpmnProcessId());
          final var plainTaskDefinition = plainTaskDefinition(
              workflowModuleId,
              plainBpmnProcessId,
              task.taskDefinition());
          servedByJobType
              .computeIfAbsent(task.taskDefinition(), key -> new java.util.LinkedList<>())
              .add(new ServedElement(task.bpmnProcessId(), task.activityId(), plainTaskDefinition));
          final var timeout = jobTimeoutResolver.jobTimeoutFor(
              workflowModuleId,
              plainBpmnProcessId,
              plainTaskDefinition);
          final var previous = timeoutsByDefinition.putIfAbsent(task.taskDefinition(), timeout);
          if ((previous != null) && !previous.equals(timeout)) {
            throw new IllegalStateException(
                """
                    The task definition '%s' of workflow module '%s' is used by several tasks with \
                    CONFLICTING job timeouts (%s vs. %s)! One polling worker serves a task \
                    definition - configure the same 'job-timeout' for all its tasks (property \
                    levels: vanillabp.workflow-modules.%s.workflows.<workflow>.tasks.%s.adapters.%s.job-timeout)."""
                    .formatted(
                        task.taskDefinition(),
                        workflowModuleId,
                        previous,
                        timeout,
                        workflowModuleId,
                        task.taskDefinition(),
                        adapterId));
          }
        });
    // user-task lifecycle listeners: one worker per distinct listener
    // job type; listener jobs are consumed like normal jobs
    final var userTasksByListenerJobType = new LinkedHashMap<String, java.util.List<String>>();
    bpmsProcessingContext
        .getUserTasksToWire()
        .forEach(userTask -> {
          userTasksByListenerJobType
              .computeIfAbsent(userTask.listenerJobType(), key -> new java.util.LinkedList<>())
              .add(userTask.bpmnProcessId());
          final var plainBpmnProcessId = plainProcessId(workflowModuleId, userTask.bpmnProcessId());
          servedByJobType
              .computeIfAbsent(userTask.listenerJobType(), key -> new java.util.LinkedList<>())
              .add(new ServedElement(userTask.bpmnProcessId(), userTask.activityId(), plainTaskDefinition(
                  workflowModuleId,
                  plainBpmnProcessId,
                  userTask.externalFormReference())));
        });
    userTasksByListenerJobType.forEach((
        listenerJobType,
        bpmnProcessIds) -> {
      final var listenerFetch = fetchVariablesOf(workflowModuleId, servedByJobType.get(listenerJobType));
      var listenerWorkerBuilder = applyFetchVariables(applyWorkerOptions(client
          .newWorker()
          .jobType(listenerJobType)
          .handler(new io.vanillabp.camunda8.wiring.Camunda8UserTaskListenerHandler(
              adapterId, workflowModuleId, workflowTaskInvoker, scoping, multiInstanceRegistry, drain, listenerFetch))
          .timeout(
              listenerLockOf(workflowModuleId, bpmnProcessIds, "user-task listener", listenerJobType))
          .name("vanillabp-%s-%s".formatted(adapterId, listenerJobType)), listenerJobType),
          workflowModuleId,
          "user-task listener",
          listenerJobType,
          listenerFetch);
      final var listenerTenantId = tenantIdOf(workflowModuleId);
      if (listenerTenantId != null) {
        // with 'by-adapter': jobs of a tenant are only delivered to workers
        // subscribing for that tenant
        listenerWorkerBuilder = listenerWorkerBuilder.tenantId(listenerTenantId);
      }
      final var worker = listenerWorkerBuilder.open();
      bpmsProcessingContext.getOpenWorkers().add(worker);
      log.info(
          "Camunda8[{}]: opened user-task listener worker for '{}' of workflow module '{}'",
          adapterId,
          listenerJobType,
          workflowModuleId);
    });

    // start events the cluster fires on its own: one worker per start
    // event, since its job type carries the process and the element
    bpmsProcessingContext
        .getBpmsInitiatedStartsToWire()
        .forEach(startEvent -> {
          final var plainProcessId = plainProcessId(workflowModuleId, startEvent.bpmnProcessId());
          var startWorkerBuilder = applyFetchVariables(applyWorkerOptions(client
              .newWorker()
              .jobType(startEvent.listenerJobType())
              .handler(new io.vanillabp.camunda8.wiring.Camunda8BpmsInitiatedStartHandler(
                  adapterId, workflowModuleId, plainProcessId, startEvent.startEventId(), startEvent.kind(), startEvent
                      .signalName(), bpmsInitiatedStartInvoker, drain, retryBackoffResolver))
              .timeout(
                  listenerLockOf(workflowModuleId, List.of(startEvent.bpmnProcessId()), "start-event", startEvent
                      .listenerJobType()))
              .name("vanillabp-%s-%s".formatted(adapterId, startEvent.listenerJobType())),
              startEvent
                  .listenerJobType()),
              workflowModuleId,
              "start-event",
              startEvent.listenerJobType(),
              // nothing to derive here: VanillaBP copies every variable such a start
              // carries into the workflow aggregate it builds, so a list would decide
              // which of the application's own values survive
              io.vanillabp.camunda8.wiring.Camunda8FetchVariables.Selection.everything());
          final var startTenantId = tenantIdOf(workflowModuleId);
          if (startTenantId != null) {
            startWorkerBuilder = startWorkerBuilder.tenantId(startTenantId);
          }
          bpmsProcessingContext.getOpenWorkers().add(startWorkerBuilder.open());
          log.info(
              "Camunda8[{}]: opened start-event worker for '{}' of workflow module '{}'",
              adapterId,
              startEvent.listenerJobType(),
              workflowModuleId);
        });

    // one worker per process whose end is reported
    bpmsProcessingContext
        .getWorkflowEndedProcessesToWire()
        .forEach(scopedProcessId -> {
          final var plainProcessId = plainProcessId(workflowModuleId, scopedProcessId);
          final var endFetch = fetchVariablesOf(
              workflowModuleId,
              List.of(new ServedElement(scopedProcessId, null, null)));
          var endWorkerBuilder = applyFetchVariables(applyWorkerOptions(client
              .newWorker()
              .jobType(Camunda8TaskWiring.workflowEndedJobTypeOf(scopedProcessId))
              .handler(new io.vanillabp.camunda8.wiring.Camunda8WorkflowEndedHandler(
                  adapterId, workflowModuleId, plainProcessId, workflowTaskInvoker
                      .resolveWorkflowAggregateIdName(workflowModuleId,
                          plainProcessId), workflowEndedInvoker, drain, retryBackoffResolver))
              .timeout(
                  listenerLockOf(workflowModuleId, List.of(scopedProcessId), "workflow-end", Camunda8TaskWiring
                      .workflowEndedJobTypeOf(scopedProcessId)))
              .name("vanillabp-%s-%s".formatted(adapterId, scopedProcessId)),
              Camunda8TaskWiring
                  .workflowEndedJobTypeOf(scopedProcessId)),
              workflowModuleId,
              "workflow-end",
              Camunda8TaskWiring.workflowEndedJobTypeOf(scopedProcessId),
              endFetch);
          final var endTenantId = tenantIdOf(workflowModuleId);
          if (endTenantId != null) {
            endWorkerBuilder = endWorkerBuilder.tenantId(endTenantId);
          }
          bpmsProcessingContext.getOpenWorkers().add(endWorkerBuilder.open());
          log.info(
              "Camunda8[{}]: opened workflow-end worker for BPMN process '{}' of workflow module '{}'",
              adapterId,
              plainProcessId,
              workflowModuleId);
        });

    timeoutsByDefinition.forEach((
        taskDefinition,
        timeout) -> {
      final var taskFetch = fetchVariablesOf(workflowModuleId, servedByJobType.get(taskDefinition));
      var workerBuilder = applyFetchVariables(applyWorkerOptions(client
          .newWorker()
          .jobType(taskDefinition)
          .handler(new Camunda8JobHandler(
              adapterId, workflowModuleId, client, workflowTaskInvoker, asyncTaskLockRenewal, scoping, multiInstanceRegistry, asyncTaskMaxAgeAction(), drain, retryBackoffResolver, taskFetch))
          .timeout(timeout)
          .name("vanillabp-%s-%s".formatted(adapterId, taskDefinition)), taskDefinition),
          workflowModuleId,
          "task",
          taskDefinition,
          taskFetch);
      final var workerTenantId = tenantIdOf(workflowModuleId);
      if (workerTenantId != null) {
        workerBuilder = workerBuilder.tenantId(workerTenantId);
      }
      final var worker = workerBuilder.open();
      bpmsProcessingContext.getOpenWorkers().add(worker);
      log.info(
          "Camunda8[{}]: opened job worker for task definition '{}' of workflow module '{}' "
              + "(job timeout {})",
          adapterId,
          taskDefinition,
          workflowModuleId,
          timeout);
    });

  }

  @Override
  public void stopWorkflowProcessing(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext) {

    // From here on, a delivery which fails is the shutdown and not the
    // application - the handlers ask the drain before they report anything to the cluster
    final var drain = drainOf(workflowModuleId);
    drain.beginShutdown();

    // close this module's workers (reverse order); the CamundaClient itself is
    // closed by the Camunda8ClientFactory on application shutdown
    final var workers = bpmsProcessingContext.getOpenWorkers();
    for (var i = workers.size() - 1; i >= 0; --i) {
      workers.get(i).close();
    }

    // and then wait until the module is quiet: for the handlers, because closing a worker
    // does not drain it and the client interrupts every running handler when it goes down
    // right afterwards, and for the workers themselves, because an activation
    // request which is parked at the cluster when the client is closed stays parked and
    // swallows the first job of the next application
    final var grace = shutdownGrace();
    final var closedWorkers = workers.size();
    final var outcome = drain.awaitQuiet(
        grace,
        closedWorkers,
        () -> workers.stream().allMatch(io.camunda.client.api.worker.JobWorker::isClosed));
    drain.report(grace, outcome);

    workers.clear();
    clientFactory.workflowModuleStopped(workflowModuleId);
    log.info("Workflow processing stopped for workflow module '{}' (adapter '{}')",
        workflowModuleId, adapterId);

  }

}
