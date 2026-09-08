package io.vanillabp.camunda8.deployment;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.command.DeployResourceCommandStep1;
import io.camunda.client.api.command.DeployResourceCommandStep1.DeployResourceCommandStep2;
import io.camunda.client.api.worker.JobWorker;
import io.camunda.client.api.worker.JobWorkerBuilderStep1;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.Process;
import io.vanillabp.camunda8.Camunda8Adapter;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.Camunda8ReleaseLine;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.client.Camunda8Drain;
import io.vanillabp.camunda8.client.Camunda8InstanceIdentity;
import io.vanillabp.camunda8.client.Camunda8SearchableClusterCheck;
import io.vanillabp.camunda8.client.Camunda8TenantCheck;
import io.vanillabp.camunda8.health.Camunda8Health;
import io.vanillabp.camunda8.observability.Camunda8Metrics;
import io.vanillabp.camunda8.wiring.Camunda8BpmsInitiatedStartHandler;
import io.vanillabp.camunda8.wiring.Camunda8FetchVariables;
import io.vanillabp.camunda8.wiring.Camunda8FetchVariablesResolver;
import io.vanillabp.camunda8.wiring.Camunda8JobHandler;
import io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver;
import io.vanillabp.camunda8.wiring.Camunda8MultiInstance;
import io.vanillabp.camunda8.wiring.Camunda8RetryBackoffResolver;
import io.vanillabp.camunda8.wiring.Camunda8Scoping;
import io.vanillabp.camunda8.wiring.Camunda8TaskWiring;
import io.vanillabp.camunda8.wiring.Camunda8UserTaskListenerHandler;
import io.vanillabp.camunda8.wiring.Camunda8WorkflowEndedHandler;
import io.vanillabp.integration.adapter.spi.AdapterCollaborators;
import io.vanillabp.integration.adapter.spi.AdapterDeploymentService;
import io.vanillabp.integration.adapter.spi.AdapterPlatformVersion;
import io.vanillabp.integration.adapter.spi.BpmnParseException;
import io.vanillabp.integration.adapter.spi.NameClashAvoidance;
import io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport;
import io.vanillabp.integration.adapter.spi.health.AdapterHealth;
import io.vanillabp.integration.adapter.spi.workflowend.WorkflowEndedInvoker;
import io.vanillabp.integration.adapter.spi.workflowstart.BpmsInitiatedStartInvoker;
import io.vanillabp.integration.adapter.spi.workflowstart.BpmsInitiatedStartSpec;
import io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskInvoker;
import io.vanillabp.integration.adapter.spi.workflowtask.WorkflowTaskWiring;
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
  public static final String ADAPTER_TYPE = Camunda8Adapter.ADAPTER_TYPE;

  private final String adapterId;

  private final Camunda8ClientFactory clientFactory;

  /**
   * The core's task-processing entry point: wiring validation during
   * {@link #wireBpmn} and job dispatch at runtime.
   */
  /**
   * Everything the platform hands over. An adapter which is registered incompletely does
   * not come into existence (see {@link AdapterCollaborators}).
   */
  private final AdapterCollaborators collaborators;

  private final WorkflowTaskWiring workflowTaskWiring;

  /**
   * The runtime half of the split SPI. This service does not only wire: it opens the job
   * workers at {@code startWorkflowProcessing}, and those hand every delivery to the
   * core - so it holds both halves and passes this one on.
   */
  private final WorkflowTaskInvoker workflowTaskInvoker;

  /**
   * What this adapter knows about the multi-instance elements of the processes it
   * deployed. Filled while wiring a model, read while dispatching a job - a job
   * carries the ID of its own element and nothing about the iterations enclosing it.
   */
  private final Camunda8MultiInstance.Registry multiInstanceRegistry = new Camunda8MultiInstance.Registry();

  /**
   * The core's entry point for workflows the cluster starts on its own:
   * the start events of a process are reported here while wiring, and the start
   * execution-listener workers dispatch through it. May be <code>null</code> (tests).
   */
  private final BpmsInitiatedStartInvoker bpmsInitiatedStartInvoker;

  /**
   * The core's entry point for workflows which ended. May be
   * <code>null</code> (tests) - no end listener is attached then.
   */
  private final WorkflowEndedInvoker workflowEndedInvoker;

  /**
   * Whether a worker asks the cluster for the variables this adapter derived or for all
   * of them. Handed in by the platform module after construction rather than
   * through the constructor, whose parameter list is long enough; <code>null</code>
   * (tests) means the default, which is the derived list.
   */
  private Camunda8FetchVariablesResolver fetchVariablesResolver;

  /**
   * Hands over how <code>fetch-variables</code> resolves for this adapter instance.
   *
   * @param fetchVariablesResolver The resolver, or <code>null</code> for the default
   */
  public void setFetchVariablesResolver(
      final Camunda8FetchVariablesResolver fetchVariablesResolver) {

    this.fetchVariablesResolver = fetchVariablesResolver;

  }

  /**
   * What this adapter instance measures on top of what the core measures.
   * Handed in by the platform module after construction, because it exists once per
   * application while deployment services exist per adapter id;
   * {@link Camunda8Metrics#NONE} for an application without a metrics backend.
   */
  private Camunda8Metrics metrics = Camunda8Metrics.NONE;

  /**
   * Hands over what to measure into, and registers the execution slots of this adapter
   * instance right away - the client is built before this, so there is nothing to wait
   * for.
   *
   * @param metrics What to measure into, never <code>null</code>
   */
  public void setMetrics(
      final Camunda8Metrics metrics) {

    this.metrics = metrics == null
        ? Camunda8Metrics.NONE
        : metrics;
    registerExecutionSlots();

  }

  /**
   * Publishes how many handlers this adapter instance may run, how many of them run right
   * now and how many jobs wait for a slot.
   * <p>
   * All three come from the adapter's own executor, which both execution models now build,
   * so the picture of a stalled application is the same whichever one is configured. An
   * adapter which booted without a connection has no client and therefore no executor; there
   * only the configured number is published.
   */
  private void registerExecutionSlots() {

    final var executionModel = clientFactory.getExecutionModel();
    final var executor = clientFactory.getExecutor();
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
  public AdapterHealth checkHealth() {

    return Camunda8Health.check(adapterId, clientFactory);

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
  private Camunda8AdapterConfiguration.AsyncTaskMaxAgeAction asyncTaskMaxAgeAction() {

    if (configurations == null) {
      return Camunda8AdapterConfiguration.AsyncTaskMaxAgeAction.REPORT;
    }
    final var configuration = configurations.apply(adapterId);
    return (configuration == null) || (configuration.getAsyncTaskMaxAgeAction() == null)
        ? Camunda8AdapterConfiguration.AsyncTaskMaxAgeAction.REPORT
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
      return Camunda8AdapterConfiguration.DEFAULT_SHUTDOWN_GRACE;
    }
    final var configuration = configurations.apply(adapterId);
    return configuration == null
        ? Camunda8AdapterConfiguration.DEFAULT_SHUTDOWN_GRACE
        : configuration.resolvedShutdownGrace();

  }

  /**
   * What each workflow module of this adapter instance has in flight, and whether it is
   * going down. One per workflow module: stopping one module must not make the
   * handlers of another one believe they were cut off.
   */
  private final Map<String, Camunda8Drain> drains = new ConcurrentHashMap<>();

  /**
   * @param workflowModuleId The workflow module
   * @return The drain of that module, created on first use
   */
  Camunda8Drain drainOf(
      final String workflowModuleId) {

    return drains.computeIfAbsent(
        workflowModuleId,
        moduleId -> new Camunda8Drain(adapterId, moduleId));

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
  private Camunda8Drain freshDrainOf(
      final String workflowModuleId) {

    final var drain = new Camunda8Drain(adapterId, workflowModuleId);
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
  private final Camunda8RetryBackoffResolver retryBackoffResolver;

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
  private final Function<String, Camunda8AdapterConfiguration> configurations;

  /**
   * The core's name-clash-avoidance model: decides whether a workflow
   * module is isolated by a TENANT ({@code by-adapter}, version 1's behavior), by
   * PREFIXING the identifiers ({@code use-prefix} - no tenant, which is what makes
   * tenant licenses avoidable) or not at all ({@code none}, this adapter's default).
   * May be <code>null</code> (tests): nothing is scoped then.
   */
  private final NameClashAvoidanceSupport scoping;

  /**
   * The tenants already verified against the cluster - asked once per tenant, not once
   * per workflow module (several modules may share a configured tenant).
   */
  private final Set<String> verifiedTenants = ConcurrentHashMap.newKeySet();

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
      final AdapterCollaborators collaborators,
      final Camunda8JobTimeoutResolver jobTimeoutResolver,
      final Duration asyncTaskLockRenewal) {

    this(adapterId, clientFactory, collaborators, jobTimeoutResolver, asyncTaskLockRenewal, null, null);

  }

  /**
   * Convenience constructor without the name-clash-avoidance support (tests).
   */
  public Camunda8DeploymentService(
      final String adapterId,
      final Camunda8ClientFactory clientFactory,
      final AdapterCollaborators collaborators,
      final Camunda8JobTimeoutResolver jobTimeoutResolver,
      final Duration asyncTaskLockRenewal,
      final Function<String, Camunda8AdapterConfiguration> configurations) {

    this(adapterId, clientFactory, collaborators, jobTimeoutResolver, asyncTaskLockRenewal, configurations, null);

  }

  public Camunda8DeploymentService(
      final String adapterId,
      final Camunda8ClientFactory clientFactory,
      final AdapterCollaborators collaborators,
      final Camunda8JobTimeoutResolver jobTimeoutResolver,
      final Duration asyncTaskLockRenewal,
      final Function<String, Camunda8AdapterConfiguration> configurations,
      final NameClashAvoidanceSupport scoping) {

    this(adapterId, clientFactory, collaborators, jobTimeoutResolver, asyncTaskLockRenewal, configurations, scoping, null);

  }

  public Camunda8DeploymentService(
      final String adapterId,
      final Camunda8ClientFactory clientFactory,
      final AdapterCollaborators collaborators,
      final Camunda8JobTimeoutResolver jobTimeoutResolver,
      final Duration asyncTaskLockRenewal,
      final Function<String, Camunda8AdapterConfiguration> configurations,
      final NameClashAvoidanceSupport scoping,
      final Camunda8RetryBackoffResolver retryBackoffResolver) {

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
    this.collaborators = collaborators;
    this.workflowTaskWiring = collaborators.workflowTaskWiring();
    this.workflowTaskInvoker = collaborators.workflowTaskInvoker();
    this.bpmsInitiatedStartInvoker = collaborators.bpmsInitiatedStartInvoker().orElse(null);
    this.workflowEndedInvoker = collaborators.workflowEndedInvoker().orElse(null);
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
   * What the cluster holds for a BPMN process this application declares without deploying a
   * model under it - the old id of a renamed process, which the cluster keeps with every
   * version ever deployed under it and with the workflows still running on them.
   * <p>
   * It is the same catalog every deployed process of this adapter is registered with: it
   * searches by the process id as the cluster knows it, so a prefix and a tenant reach the
   * old id like any other.
   */
  @Override
  public io.vanillabp.integration.adapter.spi.version.ProcessVersionCatalog processVersionCatalogOf(
      final String workflowModuleId,
      final String bpmnProcessId) {

    return processVersions;

  }

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
        Camunda8AdapterConfiguration.propertyKey(adapterId, "tenant-id"));

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

    return Camunda8Scoping.tenantIdFor(
        scoping, workflowModuleId, adapterId, clientFactory
            .getConfiguration()
            .getTenantId());

  }

  /**
   * Two <code>camunda8</code> adapter ids are only distinct if they address
   * different clusters - or one cluster with different credentials/tenants (see
   * {@link Camunda8InstanceIdentity}).
   */
  @Override
  public void validateDistinctAdapterInstances(
      final List<String> adapterIdsOfThisType) {

    Camunda8InstanceIdentity
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
   * disabled". {@link Camunda8TenantCheck} turns that into
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
  public NameClashAvoidance defaultNameClashAvoidance() {

    return NameClashAvoidance.BY_ADAPTER;

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
          Camunda8AdapterConfiguration
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
      // the file is read for what it demands of the cluster while it is still the
      // model somebody wrote, before this adapter rewrote a single element of it
      refuseAFileTheClusterWouldReject(workflowModuleId, filename, model);
      Camunda8Scoping.apply(model, workflowModuleId, adapterId, scoping);
    }
    context.addResource(filename, model);
    context.recordDeployedProcess(bpmnProcessId);
    return context;

  }

  @Override
  public Camunda8ProcessingContext readDmn(
      final String workflowModuleId,
      final Camunda8ProcessingContext existingContext,
      final String filename,
      final java.io.InputStream dmn) {

    // the decision travels as bytes: the cluster reads it, this adapter only has to make
    // sure the id it is deployed under matches what the business rule task points at
    final var file = io.vanillabp.integration.adapter.spi.DmnDecisionIds.bytesOf(dmn);
    final var prefixes = Camunda8Scoping.prefixes(workflowModuleId, adapterId, scoping);
    final var toDeploy = prefixes
        ? io.vanillabp.integration.adapter.spi.DmnDecisionIds
            .rewrite(file, id -> scoping.scopedIdentifier(workflowModuleId, id, adapterId))
        : file;
    if (prefixes) {
      log.debug(
          "Camunda8[{}]: the decisions of '{}' are deployed under prefixed ids ({}), matching the "
              + "'zeebe:calledDecision' of the business rule tasks calling them",
          adapterId,
          filename,
          io.vanillabp.integration.adapter.spi.DmnDecisionIds.of(toDeploy));
    }
    existingContext.addDecision(filename, toDeploy);
    return existingContext;

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
    final var specs = new ArrayList<BpmnTaskSpec>();
    tasks
        .stream()
        .map(task -> new BpmnTaskSpec(
            task.activityId(), plainTaskDefinition(workflowModuleId, bpmnProcessId, task.taskDefinition())))
        .forEach(specs::add);
    userTasks
        .stream()
        .map(userTask -> BpmnTaskSpec.userTask(
            userTask.activityId(),
            plainTaskDefinition(workflowModuleId, bpmnProcessId, userTask.externalFormReference())))
        .forEach(specs::add);
    workflowTaskWiring.validateTaskWiring(workflowModuleId, bpmnProcessId, specs);
    // The same extraction serves the models of OLDER versions the cluster
    // still holds, so both directions see a model the same way
    processVersions.setTasksOfModel(this::taskSpecsOf);

    // The cluster can be asked which versions of this process it has, which
    // is what a version specification naming a version TAG needs
    workflowTaskWiring
        .registerProcessVersions(adapterId, workflowModuleId, bpmnProcessId, processVersions);

    // Which elements can put a second token into a running workflow - two
    // tokens are two writers on the workflow aggregate, and the core knows whether
    // that aggregate can survive them
    workflowTaskWiring
        .reportConcurrentTokenElements(
            workflowModuleId,
            bpmnProcessId,
            Camunda8TaskWiring.concurrentTokenElementIdsOf(model, scopedBpmnProcessId));

    // the user tasks version 1 modelled up to its release 1.6.3 are served by
    // nothing here and would be silent - so they are counted and named
    reportLegacyUserTasks(
        workflowModuleId,
        bpmnProcessId,
        scopedBpmnProcessId,
        Camunda8TaskWiring.legacyUserTaskIdsOf(model, scopedBpmnProcessId));

    // message correlation: inject the correlation-key expression
    // '=<aggregate-ID variable>' into message subscriptions lacking one - the V2
    // convention enabling ProcessService#correlateMessage without manual model
    // tweaks (existing expressions stay untouched, V1 models deploy unchanged).
    // Asking the core outright is safe here: prepareBpmn refused the whole file
    // where a process waiting for a message has no workflow aggregate to name
    Camunda8TaskWiring.wireMessageSubscriptions(
        model,
        scopedBpmnProcessId,
        () -> workflowTaskWiring.resolveWorkflowAggregateIdName(workflowModuleId, bpmnProcessId));
    // multi-instance: the input mappings which make the element, the index
    // and the total of every iteration readable from a job are ADDED TO THE MODEL
    // here, and which iterations enclose which element is remembered for dispatch
    Camunda8MultiInstance
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
                  .map(startEvent -> new BpmsInitiatedStartSpec(
                      startEvent.startEventId(), startEvent.kind(), startEvent.signalName(), null))
                  .toList());
      context.getBpmsInitiatedStartsToWire().addAll(bpmsInitiatedStarts);
    }

    // the end of a workflow is reported only where the application asked for it -
    // a model must not pay for a listener nobody wants. A process this application
    // serves no workflow of is left out even where the end IS wanted, which a workflow
    // module releasing its delivery records on workflow end wants for every process it
    // deploys: the worker answering that listener's job reads the aggregate-ID variable,
    // so a listener without one would stop the workflow at its own end
    final var theEndIsReported = (workflowEndedInvoker != null) && workflowEndedInvoker
        .workflowEndedHandlerExists(workflowModuleId, bpmnProcessId) && (aggregateIdNameOf(
            workflowModuleId,
            bpmnProcessId) != null);
    if (theEndIsReported && Camunda8TaskWiring.attachWorkflowEndedListener(model, scopedBpmnProcessId)) {
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
   * Ends the deployment of a BPMN file whose executable process waits for a message
   * without saying what to correlate it by, where this application serves no workflow of
   * that process.
   * <p>
   * Camunda 8 accepts no message catch element whose message carries no
   * <code>zeebe:subscription</code>, and it answers with the rejection of the whole FILE,
   * so the processes next to that one would not be deployed either. Where a workflow
   * service claims the process, <code>wireBpmn</code> writes the subscription and
   * correlates by the workflow aggregate's ID, which is decision 5 in the repository's
   * DECISIONS.md. Where none does, there is no aggregate to name, and writing a
   * substitute would change a model this application does not own and hide from the
   * modeller that their process is incomplete. So the boot ends here instead, which is
   * the earlier and clearer half of a failure which happens either way.
   * <p>
   * Asked once per file and before anything of it is rewritten: a message element belongs
   * to the file rather than to one process, so an injection for a process wired earlier
   * would otherwise decide the verdict about a process wired later.
   *
   * @param workflowModuleId The workflow module
   * @param filename The BPMN file, which is what the cluster accepts or rejects
   * @param model The model as it was read
   */
  private void refuseAFileTheClusterWouldReject(
      final String workflowModuleId,
      final String filename,
      final BpmnModelInstance model) {

    for (final var process : model.getModelElementsByType(Process.class)) {
      if (!process.isExecutable()) {
        continue;
      }
      // the model is asked first because it answers for free, while the core has to
      // resolve the ID property of an aggregate to answer at all
      final var elementsWaitingForACorrelationKey = Camunda8TaskWiring
          .messagesWithoutACorrelationKey(model, process.getId());
      if (elementsWaitingForACorrelationKey.isEmpty()) {
        continue;
      }
      // this application serves a workflow of the process, so wireBpmn writes the
      // subscription and what reaches the cluster is complete
      if (aggregateIdNameOf(workflowModuleId, process.getId()) != null) {
        continue;
      }
      throw new IllegalStateException(
          """
              Camunda 8 adapter '%s' does not deploy BPMN file '%s' of workflow module '%s': the \
              cluster would reject the file as a whole, and with it every process the file \
              declares. Its executable process '%s' waits for a message at %s, and Camunda 8 \
              demands a 'zeebe:subscription' with a correlation key on the message of every \
              executable process it is given, whether or not VanillaBP runs that process. \
              VanillaBP writes that subscription for a process one of its @WorkflowService \
              classes claims and correlates by that process' workflow aggregate. No class of \
              this application claims '%s', so there is no aggregate to name. Two ways out: model \
              the correlation key of the message(s) named above (in the modeler: 'Subscription \
              correlation key' on the message), or set isExecutable="false" on process '%s' where \
              nothing is meant to run it."""
              .formatted(
                  adapterId,
                  filename,
                  workflowModuleId,
                  process.getId(),
                  elementsWaitingForACorrelationKey
                      .stream()
                      .map(waiting -> "'%s' (message '%s')"
                          .formatted(waiting.catchElementId(), waiting.messageName()))
                      .collect(Collectors.joining(", ")),
                  process.getId(),
                  process.getId()));
    }

  }

  /**
   * Says that a BPMN process still carries user tasks in the shape VanillaBP 1 modelled
   * them up to its release 1.6.3, and how many of them are open on the cluster right
   * now.
   * <p>
   * A WARN rather than a failed deployment. The model itself is valid, the workflow runs,
   * and an application may deliberately serve such a task with a job worker of its own -
   * what does NOT happen is anything by VanillaBP: no CREATED notification, and
   * <code>completeUserTask</code> cannot complete the task, because version 1 handed out
   * the job's key while the cluster expects a user-task key. Being silent about that was
   * the defect; ending the boot over it would be the other one.
   * <p>
   * Two numbers are reported and only the first one is certain. The elements come from
   * the model this boot deploys and are what has to reach zero; the count of open tasks is
   * a search, and this report runs while the module is wired, which is before the start
   * has waited for its cluster - so a cluster which is not up yet costs the number.
   *
   * @param workflowModuleId The workflow module id
   * @param bpmnProcessId The plain BPMN process id
   * @param scopedBpmnProcessId The process id as the cluster knows it
   * @param elementIds The user tasks found, empty for every model written since 1.7.0
   */
  private void reportLegacyUserTasks(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String scopedBpmnProcessId,
      final List<String> elementIds) {

    if (elementIds.isEmpty()) {
      return;
    }
    final var openTasks = countOpenLegacyUserTasks(scopedBpmnProcessId);
    log.warn(
        """
            Camunda8[{}]: {} user task(s) of BPMN process '{}' (workflow module '{}') are modelled \
            the way VanillaBP 1 modelled them up to its release 1.6.3 - a plain BPMN user task whose \
            'zeebe:formDefinition' names a formKey, served by a job worker on '{}': {}. This version \
            does not serve them, and it says so rather than failing the deployment, because the \
            process itself is fine. What you lose for each of them: no notification when the task is \
            created or canceled, and 'ProcessService#completeUserTask' cannot complete it, because \
            the id such a task hands out is a job key while the cluster expects a user-task key. \
            VanillaBP 1.7.0 replaced this construction, so the way out is the model: make the user \
            task a Camunda-managed one ('zeebe:userTask') and set 'External form reference' \
            (zeebe:formDefinition externalReference) to what the formKey said - VanillaBP then wires \
            its lifecycle listeners itself. {} Finish or cancel the tasks which are still open BEFORE \
            you rely on this application to complete them, because afterwards nothing can.""",
        adapterId,
        elementIds.size(),
        bpmnProcessId,
        workflowModuleId,
        Camunda8TaskWiring.TASKDEFINITION_USERTASK_WORKER_V1,
        String.join("', '", elementIds.stream().map("'%s'"::formatted).toList()),
        openTasks == null
            ? "The cluster did not answer how many of them are open right now."
            : "Open right now: %d.".formatted(openTasks));

  }

  /**
   * How many jobs of version 1's user-task type the cluster still holds for one process -
   * the number which goes to zero as those tasks are finished.
   *
   * @param scopedBpmnProcessId The process id as the cluster knows it
   * @return The count, or <code>null</code> where the cluster did not answer
   */
  private Long countOpenLegacyUserTasks(
      final String scopedBpmnProcessId) {

    try {
      // the TOTAL rather than the page which came back, and one item fetched because
      // only the number is wanted
      final var found = clientFactory
          .getClient()
          .newJobSearchRequest()
          .filter(filter -> filter
              .processDefinitionId(scopedBpmnProcessId)
              .type(Camunda8TaskWiring.TASKDEFINITION_USERTASK_WORKER_V1))
          .page(page -> page.limit(1))
          .send()
          .join();
      return found.page().totalItems();
    } catch (final RuntimeException e) {
      // a diagnostic never fails a deployment, and a cluster which cannot answer
      // says so in the message instead
      log.debug(
          "Camunda8[{}]: the cluster did not answer how many version-1 user tasks of '{}' are open",
          adapterId,
          scopedBpmnProcessId,
          e);
      return null;
    }

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
  private Collection<BpmnTaskSpec> taskSpecsOf(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String version,
      final BpmnModelInstance model) {

    final var scopedBpmnProcessId = scopedProcessId(workflowModuleId, bpmnProcessId);
    final var specs = new ArrayList<BpmnTaskSpec>();
    Camunda8TaskWiring
        .tasksOf(model, scopedBpmnProcessId)
        .stream()
        .map(task -> new BpmnTaskSpec(
            task.activityId(), plainTaskDefinition(workflowModuleId, bpmnProcessId, task.taskDefinition())))
        .forEach(specs::add);
    Camunda8TaskWiring
        .userTasksOf(model, scopedBpmnProcessId, workflowModuleId, "version %s".formatted(version))
        .stream()
        .map(userTask -> BpmnTaskSpec.userTask(
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

    // A cluster booting together with the application lets every round of the start fail,
    // so it is waited for here: once per adapter instance, and right before the first
    // round which decides anything - the searchable-cluster check below, which would end
    // the boot of an application whose cluster is merely coming up with it (see
    // io.vanillabp.camunda8.client.Camunda8ClusterWait). An adapter with nothing to
    // deploy makes no such round and therefore waits for nothing
    clientFactory.waitUntilTheClusterAnswers();

    // and now that a cluster HAS answered, whether it answers a SEARCH: this adapter
    // serves no other kind. Per module rather than once per adapter id, because the
    // throw is what the deployment-failure policy of THIS module reads
    Camunda8SearchableClusterCheck
        .requireAClusterWhichCanBeSearched(adapterId, clientFactory.getQueryApi());

    // one DeployResourceCommand per workflow module with all its models
    final var client = clientFactory.getClient();
    DeployResourceCommandStep2 command = null;
    for (final var resource : bpmsProcessingContext.getResources().entrySet()) {
      final DeployResourceCommandStep1 next = command != null ? command : client.newDeployResourceCommand();
      command = next.addProcessModel(resource.getValue(), resource.getKey());
    }
    // the module's decision tables go into the SAME command: a business rule task binding
    // its decision to the deployment finds it, and both are versioned together
    for (final var decision : bpmsProcessingContext.getDecisions().entrySet()) {
      final DeployResourceCommandStep1 next = command != null ? command : client.newDeployResourceCommand();
      command = next.addResourceBytes(decision.getValue(), decision.getKey());
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
        Camunda8TenantCheck
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
              .map(processId -> new NameClashAvoidanceSupport.DeployedProcess(
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
            processVersions.recordDeployedScoped(process.getBpmnProcessId(), process.getVersion());
            processVersions
                .recordDeployed(
                    workflowModuleId,
                    plainBpmnProcessId,
                    process.getVersion(),
                    Camunda8TaskWiring.versionTagOf(model, process.getBpmnProcessId()));
            // The border between the model this boot brought and the older
            // versions the cluster still holds
            workflowTaskWiring
                .registerDeployedVersion(
                    adapterId, workflowModuleId, plainBpmnProcessId, String.valueOf(process.getVersion()));
          });

      final var deployedDecisions = deployment.getDecisions();
      if ((deployedDecisions != null) && !deployedDecisions.isEmpty()) {
        // the ids the CLUSTER knows, which is what a business rule task has to name
        log.info(
            "Deployed {} decision(s) of workflow module '{}' to Camunda 8 (adapter '{}'): {}",
            deployedDecisions.size(),
            workflowModuleId,
            adapterId,
            deployedDecisions
                .stream()
                .map(decision -> "%s (version %d)".formatted(decision.getDmnDecisionId(), decision.getVersion()))
                .toList());
      }
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
  JobWorkerBuilderStep1.JobWorkerBuilderStep3 applyWorkerOptions(
      final JobWorkerBuilderStep1.JobWorkerBuilderStep3 builder,
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
   * every worker of this adapter reads. A BPMN file may carry a process no
   * <code>&#64;WorkflowService</code> class claims, and the wiring validation lets such a
   * process pass rather than ending the boot over a model somebody else owns, so this can
   * be asked about a process the core knows no aggregate for.
   * <p>
   * Answering <code>null</code> is what lets each caller decide what to do about it. A
   * worker asks for every variable instead of building a list which is missing exactly the
   * name its handler reads, and the end of such a workflow is not reported at all, because
   * an execution listener whose job nobody activates would stop the workflow at its own
   * end. Where the missing name is not this adapter's to work around it refuses the file
   * instead, which is what a message subscription without a correlation key gets: the
   * cluster demands one of every executable process, and a substitute would rewrite a
   * process this application does not serve.
   *
   * @param workflowModuleId The workflow module
   * @param plainBpmnProcessId The BPMN process id as the core knows it
   * @return The variable's name, or <code>null</code> if the core cannot tell
   */
  private String aggregateIdNameOf(
      final String workflowModuleId,
      final String plainBpmnProcessId) {

    try {
      return workflowTaskWiring.resolveWorkflowAggregateIdName(workflowModuleId, plainBpmnProcessId);
    } catch (final RuntimeException e) {
      log.debug(
          "Camunda8[{}]: the BPMN process '{}' of workflow module '{}' has no known workflow "
              + "aggregate, so nothing which needs its aggregate-ID variable is wired for it",
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
  Camunda8FetchVariables.Selection fetchVariablesOf(
      final String workflowModuleId,
      final List<ServedElement> served) {

    final var variables = new TreeSet<String>();
    for (final var element : served) {
      final var plainBpmnProcessId = plainProcessId(workflowModuleId, element.scopedBpmnProcessId());
      final var mode = Camunda8FetchVariablesResolver
          .resolve(fetchVariablesResolver, workflowModuleId, plainBpmnProcessId, element.taskDefinition());
      if (mode == Camunda8FetchVariables.Mode.ALL) {
        // one worker serves a job type, so the two values cannot both apply - and
        // fetching more than derived is never wrong, only more expensive
        return Camunda8FetchVariables.Selection.everything();
      }
      final var aggregateIdName = aggregateIdNameOf(workflowModuleId, plainBpmnProcessId);
      if (aggregateIdName == null) {
        return Camunda8FetchVariables.Selection.everything();
      }
      if (element.elementId() == null) {
        // the workflow-end listener: it reports a process rather than an element, and a
        // @WorkflowEnded method cannot declare a @TaskParam at all (the core rejects one),
        // so the aggregate's id is the complete answer here
        Camunda8FetchVariables.collect(variables, aggregateIdName, List.of());
        continue;
      }
      Camunda8FetchVariables.collect(
          variables,
          aggregateIdName,
          multiInstanceRegistry.chainOf(element.scopedBpmnProcessId(), element.elementId()));
      // and what the handlers of this element read with @TaskParam: the core
      // scanned those names off the methods while wiring, so the list is what the
      // application asks for rather than what the model happens to mention
      variables
          .addAll(
              workflowTaskWiring
                  .taskParameterNames(workflowModuleId, plainBpmnProcessId, element.taskDefinition()));
    }
    return Camunda8FetchVariables.Selection.of(variables);

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
  JobWorkerBuilderStep1.JobWorkerBuilderStep3 applyFetchVariables(
      final JobWorkerBuilderStep1.JobWorkerBuilderStep3 builder,
      final String workflowModuleId,
      final String kind,
      final String jobType,
      final Camunda8FetchVariables.Selection selection) {

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
              .computeIfAbsent(task.taskDefinition(), key -> new LinkedList<>())
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
    final var userTasksByListenerJobType = new LinkedHashMap<String, List<String>>();
    bpmsProcessingContext
        .getUserTasksToWire()
        .forEach(userTask -> {
          userTasksByListenerJobType
              .computeIfAbsent(userTask.listenerJobType(), key -> new LinkedList<>())
              .add(userTask.bpmnProcessId());
          final var plainBpmnProcessId = plainProcessId(workflowModuleId, userTask.bpmnProcessId());
          servedByJobType
              .computeIfAbsent(userTask.listenerJobType(), key -> new LinkedList<>())
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
          .handler(Camunda8UserTaskListenerHandler
              .builder()
              .adapterId(adapterId)
              .workflowModuleId(workflowModuleId)
              .workflowTaskInvoker(workflowTaskInvoker)
              .scoping(scoping)
              .multiInstanceRegistry(multiInstanceRegistry)
              .drain(drain)
              .fetchVariables(listenerFetch)
              .build())
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
              .handler(new Camunda8BpmsInitiatedStartHandler(
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
              Camunda8FetchVariables.Selection.everything());
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
              // asking the core outright is safe here: wireBpmn put only processes
              // with a known workflow aggregate into this list
              .handler(new Camunda8WorkflowEndedHandler(
                  adapterId, workflowModuleId, plainProcessId, workflowTaskWiring
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
          .handler(Camunda8JobHandler
              .builder()
              .adapterId(adapterId)
              .workflowModuleId(workflowModuleId)
              .camundaClient(client)
              .workflowTaskInvoker(workflowTaskInvoker)
              .asyncTaskLockRenewal(asyncTaskLockRenewal)
              .scoping(scoping)
              .multiInstanceRegistry(multiInstanceRegistry)
              .asyncTaskMaxAgeAction(asyncTaskMaxAgeAction())
              .drain(drain)
              .retryBackoffResolver(retryBackoffResolver)
              .fetchVariables(taskFetch)
              .predatesDeployedVersion(processVersions::predatesDeployedVersion)
              .build())
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

    openTheWorkersOfTheProcessesNobodyDeployed(
        workflowModuleId,
        bpmsProcessingContext,
        client,
        drain,
        jobTypesAWorkerIsAlreadyOpenFor(servedByJobType.keySet(), bpmsProcessingContext));

  }

  /**
   * The job types this workflow module already has a worker for - the task definitions and
   * user-task listeners of its deployed processes, plus the ends of the processes whose end
   * is reported. What is in here needs no second worker for a declared BPMN process id: the
   * name is what a worker subscribes to, so wherever the name does not carry the process id,
   * the workers of the deployed processes reach the workflows of the old id as well.
   *
   * @param servedJobTypes The job types the tasks and user tasks produced
   * @param bpmsProcessingContext The context of the module being started
   * @return The job types, in no particular order
   */
  private static Set<String> jobTypesAWorkerIsAlreadyOpenFor(
      final Set<String> servedJobTypes,
      final Camunda8ProcessingContext bpmsProcessingContext) {

    final var jobTypes = new java.util.HashSet<>(servedJobTypes);
    bpmsProcessingContext
        .getWorkflowEndedProcessesToWire()
        .forEach(scopedProcessId -> jobTypes.add(Camunda8TaskWiring.workflowEndedJobTypeOf(scopedProcessId)));
    return jobTypes;

  }

  /**
   * Opens the workers which reach the workflows of a BPMN process this application DECLARES
   * without deploying a model under it - the old id of a renamed process.
   * <p>
   * A worker subscribes to a job type, and under <code>use-prefix</code> a job type carries
   * the id of the process it was deployed with: the jobs of the workflows under the old id
   * are named after the OLD id, so no worker of the deployed processes asks for them and
   * nobody notices, because an unfetched job is not a failed one. What the workflows need is
   * therefore one more subscription per name they produce, and the names are composed the
   * same way the deployed ones were: the task definitions the application serves for that id,
   * scoped by it.
   * <p>
   * Where a job type is already served nothing is opened, which is every mode but
   * <code>use-prefix</code> and <code>use-prefix</code> with
   * <code>prefix-task-definitions-per-process: false</code>. So an application which does not
   * scope task definitions by their process notices none of this, as it did before.
   *
   * @param workflowModuleId The workflow module which is about to process workflows
   * @param bpmsProcessingContext The context whose open workers are closed on shutdown
   * @param client The client of this adapter id
   * @param drain What the module has in flight, handed to every handler
   * @param jobTypesAlreadyServed The job types the deployed processes opened a worker for
   */
  private void openTheWorkersOfTheProcessesNobodyDeployed(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext,
      final CamundaClient client,
      final Camunda8Drain drain,
      final Set<String> jobTypesAlreadyServed) {

    workflowTaskWiring
        .taskWiringOfProcessesNobodyDeployed(workflowModuleId)
        .forEach((
            bpmnProcessId,
            taskDefinitions) -> {
          // whatever scoping decides about extra workers below, the message check of
          // correlateMessage has to know that models of this module live in the
          // cluster only - so the declared id is recorded in every mode
          clientFactory
              .getDeployedProcesses()
              .recordDeclaredWithoutDeployment(workflowModuleId, bpmnProcessId);
          final var openedJobTypes = new TreeSet<String>();
          taskDefinitions
              .forEach(taskDefinition -> {
                final var jobType = scoping == null
                    ? taskDefinition
                    : scoping.scopedTaskDefinition(workflowModuleId, bpmnProcessId, taskDefinition, adapterId);
                if (jobTypesAlreadyServed.contains(jobType)) {
                  return;
                }
                openTaskWorker(workflowModuleId, bpmnProcessId, jobType, bpmsProcessingContext, client, drain);
                openedJobTypes.add(jobType);
                // a served task definition is either a service task's or a user task's,
                // and which of the two cannot be told without the model this application
                // no longer has. Both subscriptions are opened therefore, and the one
                // whose kind the task never was stays idle - see decision 19 in the
                // repository's DECISIONS.md
                final var listenerJobType = Camunda8TaskWiring.TASKDEFINITION_USERTASK_ZEEBE + jobType;
                openUserTaskListenerWorker(
                    workflowModuleId,
                    bpmnProcessId,
                    listenerJobType,
                    bpmsProcessingContext,
                    client,
                    drain);
                openedJobTypes.add(listenerJobType);
              });
          openWorkflowEndWorkerOfADeclaredId(
              workflowModuleId,
              bpmnProcessId,
              bpmsProcessingContext,
              client,
              drain,
              jobTypesAlreadyServed,
              openedJobTypes);
          reportWhatADeclaredIdIsServedWith(workflowModuleId, bpmnProcessId, taskDefinitions, openedJobTypes);
        });

  }

  /**
   * One polling worker for the tasks of a declared BPMN process id. It asks for every
   * variable rather than a derived list: deriving one needs the elements of the model, and
   * the model of that id is what this application does not have.
   */
  private void openTaskWorker(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String jobType,
      final Camunda8ProcessingContext bpmsProcessingContext,
      final CamundaClient client,
      final Camunda8Drain drain) {

    final var plainTaskDefinition = plainTaskDefinition(workflowModuleId, bpmnProcessId, jobType);
    var workerBuilder = applyFetchVariables(applyWorkerOptions(client
        .newWorker()
        .jobType(jobType)
        .handler(Camunda8JobHandler
            .builder()
            .adapterId(adapterId)
            .workflowModuleId(workflowModuleId)
            .camundaClient(client)
            .workflowTaskInvoker(workflowTaskInvoker)
            .asyncTaskLockRenewal(asyncTaskLockRenewal)
            .scoping(scoping)
            .multiInstanceRegistry(multiInstanceRegistry)
            .asyncTaskMaxAgeAction(asyncTaskMaxAgeAction())
            .drain(drain)
            .retryBackoffResolver(retryBackoffResolver)
            .fetchVariables(Camunda8FetchVariables.Selection.everything())
            .predatesDeployedVersion(processVersions::predatesDeployedVersion)
            .build())
        .timeout(jobTimeoutResolver.jobTimeoutFor(workflowModuleId, bpmnProcessId, plainTaskDefinition))
        .name("vanillabp-%s-%s".formatted(adapterId, jobType)), jobType),
        workflowModuleId,
        "task",
        jobType,
        Camunda8FetchVariables.Selection.everything());
    final var tenantId = tenantIdOf(workflowModuleId);
    if (tenantId != null) {
      workerBuilder = workerBuilder.tenantId(tenantId);
    }
    bpmsProcessingContext.getOpenWorkers().add(workerBuilder.open());

  }

  /**
   * One worker for the user-task lifecycle listeners of a declared BPMN process id, opened
   * next to the task worker of the same task definition because nothing outside the model
   * says which of the two kinds that definition belonged to. Whichever of the pair the task
   * never was stays idle, which costs one activation request.
   */
  private void openUserTaskListenerWorker(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String listenerJobType,
      final Camunda8ProcessingContext bpmsProcessingContext,
      final CamundaClient client,
      final Camunda8Drain drain) {

    var workerBuilder = applyFetchVariables(applyWorkerOptions(client
        .newWorker()
        .jobType(listenerJobType)
        .handler(Camunda8UserTaskListenerHandler
            .builder()
            .adapterId(adapterId)
            .workflowModuleId(workflowModuleId)
            .workflowTaskInvoker(workflowTaskInvoker)
            .scoping(scoping)
            .multiInstanceRegistry(multiInstanceRegistry)
            .drain(drain)
            .fetchVariables(Camunda8FetchVariables.Selection.everything())
            .build())
        .timeout(
            listenerLockOf(
                workflowModuleId,
                List.of(scopedProcessId(workflowModuleId, bpmnProcessId)),
                "user-task listener",
                listenerJobType))
        .name("vanillabp-%s-%s".formatted(adapterId, listenerJobType)), listenerJobType),
        workflowModuleId,
        "user-task listener",
        listenerJobType,
        Camunda8FetchVariables.Selection.everything());
    final var tenantId = tenantIdOf(workflowModuleId);
    if (tenantId != null) {
      workerBuilder = workerBuilder.tenantId(tenantId);
    }
    bpmsProcessingContext.getOpenWorkers().add(workerBuilder.open());

  }

  /**
   * The worker which reports the end of a workflow running under a declared BPMN process
   * id, opened only where the application has a <code>&#64;WorkflowEnded</code> method for
   * that id. The job type is composed from the process id alone, so this one is exact
   * without any model.
   */
  private void openWorkflowEndWorkerOfADeclaredId(
      final String workflowModuleId,
      final String bpmnProcessId,
      final Camunda8ProcessingContext bpmsProcessingContext,
      final CamundaClient client,
      final Camunda8Drain drain,
      final Set<String> jobTypesAlreadyServed,
      final Set<String> openedJobTypes) {

    if ((workflowEndedInvoker == null) || !workflowEndedInvoker
        .workflowEndedHandlerExists(workflowModuleId, bpmnProcessId)) {
      return;
    }
    final var scopedBpmnProcessId = scopedProcessId(workflowModuleId, bpmnProcessId);
    final var jobType = Camunda8TaskWiring.workflowEndedJobTypeOf(scopedBpmnProcessId);
    if (jobTypesAlreadyServed.contains(jobType)) {
      return;
    }
    final var aggregateIdName = aggregateIdNameOf(workflowModuleId, bpmnProcessId);
    if (aggregateIdName == null) {
      return;
    }
    var workerBuilder = applyFetchVariables(applyWorkerOptions(client
        .newWorker()
        .jobType(jobType)
        .handler(new Camunda8WorkflowEndedHandler(
            adapterId, workflowModuleId, bpmnProcessId, aggregateIdName, workflowEndedInvoker, drain, retryBackoffResolver))
        .timeout(listenerLockOf(workflowModuleId, List.of(scopedBpmnProcessId), "workflow-end", jobType))
        .name("vanillabp-%s-%s".formatted(adapterId, scopedBpmnProcessId)), jobType),
        workflowModuleId,
        "workflow-end",
        jobType,
        Camunda8FetchVariables.Selection.everything());
    final var tenantId = tenantIdOf(workflowModuleId);
    if (tenantId != null) {
      workerBuilder = workerBuilder.tenantId(tenantId);
    }
    bpmsProcessingContext.getOpenWorkers().add(workerBuilder.open());
    openedJobTypes.add(jobType);

  }

  /**
   * Says what the workflows of a declared BPMN process id are served with, once per start
   * and per id: the job types which were opened for it, or that nothing had to be opened
   * because the deployed processes already reach them.
   * <p>
   * A declared id whose methods name no task definition at all is the one case worth a
   * warning. A <code>&#64;WorkflowTask</code> method wired to a BPMN element id
   * (<code>&#64;WorkflowTask(id = ...)</code>) is matched through the model, and the model
   * of that id is what this application does not have, so its job type cannot be composed
   * and those workflows stand still without an incident.
   */
  private void reportWhatADeclaredIdIsServedWith(
      final String workflowModuleId,
      final String bpmnProcessId,
      final Collection<String> taskDefinitions,
      final Set<String> openedJobTypes) {

    if (openedJobTypes.isEmpty() && !taskDefinitions.isEmpty()) {
      log.info(
          "Camunda8[{}]: the workflows of the declared BPMN process '{}' (workflow module '{}') are "
              + "served by the workers of the deployed processes - the task definitions of this module "
              + "do not carry the BPMN process id, so a job of the old id is named like any other",
          adapterId,
          bpmnProcessId,
          workflowModuleId);
      return;
    }
    if (!openedJobTypes.isEmpty()) {
      log.info(
          "Camunda8[{}]: opened {} worker(s) for the declared BPMN process '{}' of workflow module "
              + "'{}', so the workflows still running under that id keep being served: {}",
          adapterId,
          openedJobTypes.size(),
          bpmnProcessId,
          workflowModuleId,
          String.join(", ", openedJobTypes));
    }
    if (!taskDefinitions.isEmpty()) {
      return;
    }
    log.warn(
        """
            Camunda8[{}]: workflow module '{}' declares BPMN process '{}' without deploying a model \
            under it, and no @WorkflowTask method serving that id names a task definition - every one \
            of them is wired to a BPMN element id instead. A worker subscribes to a task definition, \
            and composing one needs the model of that process, which this application does not bring \
            any more. The workflows still running under that id therefore stand still at their next \
            task, without an incident, because an unfetched job is not a failed one. Either wire those \
            methods by task definition ('@WorkflowTask(taskDefinition = ...)', which is what the \
            model's 'zeebe:taskDefinition' carries), or keep deploying the old model under its old id \
            until those workflows have ended.""",
        adapterId,
        workflowModuleId,
        bpmnProcessId);

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
        () -> workers.stream().allMatch(JobWorker::isClosed));
    drain.report(grace, outcome);

    workers.clear();
    clientFactory.workflowModuleStopped(workflowModuleId);
    log.info("Workflow processing stopped for workflow module '{}' (adapter '{}')",
        workflowModuleId, adapterId);

  }

}
