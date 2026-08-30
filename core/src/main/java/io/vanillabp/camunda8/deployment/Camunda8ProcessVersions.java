package io.vanillabp.camunda8.deployment;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.enums.ProcessDefinitionState;
import io.camunda.client.api.search.filter.ProcessDefinitionFilter;
import io.vanillabp.integration.adapter.spi.version.CachingProcessVersionCatalog;
import io.vanillabp.integration.adapter.spi.version.DeployedProcessVersion;
import lombok.extern.slf4j.Slf4j;

/**
 * The versions of the process definitions of ONE Camunda 8 cluster (= one adapter id):
 * what the core matches <code>&#64;WorkflowTask(version = ...)</code> and its siblings
 * against.
 * <p>
 * The version itself travels with every job ({@code ActivatedJob#getProcessDefinitionVersion}),
 * so nothing here is needed for version specifications made of numbers. Version TAGS
 * are a different matter: a job does not carry one, and the cluster only tells which
 * version carries which tag through the query API. Where a cluster refuses to be
 * searched, the tag stays unknown and is reported once - version specifications made of
 * numbers keep working.
 * <p>
 * What the deployment reported is recorded without any query
 * ({@link #recordDeployed(String, String, int, String)}): the deploy command
 * names the version the cluster assigned, and the model carries its
 * {@code zeebe:versionTag}.
 */
@Slf4j
// see decision 4 in the repository's DECISIONS.md
@SuppressWarnings("LombokSetterMayBeUsed")
public class Camunda8ProcessVersions extends CachingProcessVersionCatalog {

  private final String adapterId;

  private final java.util.function.Supplier<CamundaClient> client;

  /**
   * Whether this adapter's cluster answers query-API requests at all - settled once
   * while the adapter starts processing, and the reason a failed search here is either
   * "this cluster cannot tell" or a failure worth throwing.
   */
  private final io.vanillabp.camunda8.client.Camunda8QueryApi queryApi;

  /**
   * The BPMN process id as the CLUSTER knows it for a (workflow module, plain BPMN
   * process id) - the identifiers may be prefixed.
   */
  private final BiFunction<String, String, String> scopedProcessIds;

  /**
   * The tenant a workflow module is deployed to, or <code>null</code>.
   */
  private final Function<String, String> tenants;

  private final AtomicBoolean noQueryApiWarned = new AtomicBoolean();

  /**
   * The cluster's process definition key per (workflow module, BPMN process, version).
   * <p>
   * The startup check for old versions asks two things about every version older than the
   * one this boot deployed, its model and how many workflows still run on it, and both are
   * addressed by this key. Searching for it per question meant three searches per version
   * where one already held the answer: {@link #fetchDeployedVersions} reads the definitions
   * and used to keep nothing but their version numbers. So it keeps the keys as well, and
   * the search below runs only for a version deployed after that list was read - see
   * decision 13 in the repository's DECISIONS.md.
   * <p>
   * A key stays here for the life of the application, so a version deleted while it runs
   * is still answered from here. Nothing is invalidated for that: the questions these
   * keys serve are asked while an application boots, and the boot after the deletion
   * reads the list again.
   */
  private final java.util.Map<String, Long> definitionKeysByVersion = new java.util.concurrent.ConcurrentHashMap<>();

  /**
   * Reads the tasks of a model the cluster holds - the deployment service' own
   * extraction.
   */
  @FunctionalInterface
  public interface TasksOfModel {

    java.util.Collection<io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec> of(
        String workflowModuleId,
        String bpmnProcessId,
        String version,
        io.camunda.zeebe.model.bpmn.BpmnModelInstance model);

  }

  private TasksOfModel tasksOfModel;

  /**
   * @param tasksOfModel How the deployment service reads a model
   */
  public void setTasksOfModel(
      final TasksOfModel tasksOfModel) {

    this.tasksOfModel = tasksOfModel;

  }

  @Override
  public java.util.Collection<io.vanillabp.integration.adapter.spi.workflowtask.BpmnTaskSpec> tasksOfVersion(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String version) {

    if (tasksOfModel == null) {
      return null;
    }
    final var definitionKey = definitionKeyOf(workflowModuleId, bpmnProcessId, version);
    if (definitionKey == null) {
      // no query API, or the cluster does not hold that version any more - the core
      // says once that this BPMS cannot tell
      return null;
    }
    try {
      final var xml = client.get().newProcessDefinitionGetXmlRequest(definitionKey).send().join();
      if (xml == null) {
        return java.util.List.of();
      }
      final var model = io.camunda.zeebe.model.bpmn.Bpmn
          .readModelFromStream(new java.io.ByteArrayInputStream(xml.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      return tasksOfModel.of(workflowModuleId, bpmnProcessId, version, model);
    } catch (final RuntimeException e) {
      log.warn(
          "Camunda8[{}]: the model of version {} of BPMN process '{}' (workflow module '{}') could not be read, "
              + "so VanillaBP cannot tell whether this application still serves it",
          adapterId,
          version,
          bpmnProcessId,
          workflowModuleId,
          e);
      return null;
    }

  }

  @Override
  public Long activeInstanceCountOf(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String version) {

    final var definitionKey = definitionKeyOf(workflowModuleId, bpmnProcessId, version);
    if (definitionKey == null) {
      return null;
    }
    try {
      // the TOTAL, not the page: a search answers one page of items, so counting what
      // came back would cap every answer at the page size and quietly turn "5000 still
      // run on this version" into the page size. One item is fetched because the count
      // is what is wanted, not the instances
      final var found = client
          .get()
          .newProcessInstanceSearchRequest()
          .filter(filter -> filter
              .state(io.camunda.client.api.search.enums.ProcessInstanceState.ACTIVE)
              .processDefinitionKey(definitionKey))
          .page(page -> page.limit(1))
          .send()
          .join();
      return found.page().totalItems();
    } catch (final RuntimeException e) {
      if (!queryApi.answers()) {
        return null;
      }
      throw e;
    }

  }

  /**
   * The cluster's process definition key of ONE version of a process - the handle both
   * the model and the instance count are read by. From what the version list already
   * brought back, and only otherwise from a search of its own, which needs the query API.
   */
  private Long definitionKeyOf(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String version) {

    if (!version.matches("\\d+")) {
      return null;
    }
    final var known = definitionKeysByVersion.get(versionKey(workflowModuleId, bpmnProcessId, version));
    if (known != null) {
      return known;
    }
    return askTheClusterForTheDefinitionKey(workflowModuleId, bpmnProcessId, version);

  }

  private static String versionKey(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String version) {

    return "%s|%s|%s".formatted(workflowModuleId, bpmnProcessId, version);

  }

  /**
   * Keeps what a definition search brought back, so the next question about the same
   * version is answered without searching again.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The PLAIN BPMN process ID
   * @param definition What the cluster reported
   * @return The definition key of that version
   */
  private Long remember(
      final String workflowModuleId,
      final String bpmnProcessId,
      final io.camunda.client.api.search.response.ProcessDefinition definition) {

    final var definitionKey = definition.getProcessDefinitionKey();
    definitionKeysByVersion
        .put(
            versionKey(workflowModuleId, bpmnProcessId, String.valueOf(definition.getVersion())),
            definitionKey);
    return definitionKey;

  }

  /**
   * Which process definitions this adapter counts as existing, applied to every search
   * it runs for them.
   * <p>
   * A definition an operator deleted stays in the query API and is answered with the
   * state <code>DELETED</code>, so a search leaving this out reports versions the
   * cluster runs nothing on any more - and the startup check would keep demanding
   * methods for them, restart after restart, with deleting the definition being the one
   * remedy an operator has already tried. See decision 15 in the repository's
   * DECISIONS.md.
   *
   * @param filter The filter of a process definition search
   */
  private static void onlyDefinitionsWhichStillCount(
      final ProcessDefinitionFilter filter) {

    filter.state(ProcessDefinitionState.ACTIVE);

  }

  private Long askTheClusterForTheDefinitionKey(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String version) {

    final var scopedProcessId = scopedProcessIds.apply(workflowModuleId, bpmnProcessId);
    final var tenantId = tenants.apply(workflowModuleId);
    try {
      return client
          .get()
          .newProcessDefinitionSearchRequest()
          .filter(filter -> {
            onlyDefinitionsWhichStillCount(filter);
            filter.processDefinitionId(scopedProcessId);
            filter.version(Integer.valueOf(version));
            if (tenantId != null) {
              filter.tenantId(tenantId);
            }
          })
          .send()
          .join()
          .items()
          .stream()
          .findFirst()
          .map(definition -> remember(workflowModuleId, bpmnProcessId, definition))
          .orElse(null);
    } catch (final RuntimeException e) {
      if (!queryApi.answers()) {
        return null;
      }
      throw e;
    }

  }

  public Camunda8ProcessVersions(
      final String adapterId,
      final java.util.function.Supplier<CamundaClient> client,
      final io.vanillabp.camunda8.client.Camunda8QueryApi queryApi,
      final BiFunction<String, String, String> scopedProcessIds,
      final Function<String, String> tenants) {

    this.adapterId = adapterId;
    this.client = client;
    this.queryApi = queryApi;
    this.scopedProcessIds = scopedProcessIds;
    this.tenants = tenants;

  }

  /**
   * Remembers a version the deploy command reported.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The PLAIN BPMN process ID
   * @param version The version the cluster assigned
   * @param versionTag The <code>zeebe:versionTag</code> of the model or
   *          <code>null</code>
   */
  public void recordDeployed(
      final String workflowModuleId,
      final String bpmnProcessId,
      final int version,
      final String versionTag) {

    record(workflowModuleId, bpmnProcessId, DeployedProcessVersion.of(String.valueOf(version), versionTag));

  }

  /**
   * The version this boot deployed, per process id as the CLUSTER knows it - which is
   * the id an activated job carries, so a job worker can compare without translating
   * anything.
   */
  private final java.util.Map<String, Integer> deployedVersionByScopedProcessId = new java.util.concurrent.ConcurrentHashMap<>();

  /**
   * Remembers which version this boot deployed for a process, under the id the cluster
   * uses.
   *
   * @param scopedBpmnProcessId The process id as the cluster knows it
   * @param version The version the cluster assigned
   */
  public void recordDeployedScoped(
      final String scopedBpmnProcessId,
      final int version) {

    deployedVersionByScopedProcessId.put(scopedBpmnProcessId, version);

  }

  /**
   * Whether a workflow running on the given version of the given process was started
   * before the version this boot deployed.
   * <p>
   * Answers <code>false</code> where this boot deployed nothing for that process, which
   * is the honest answer of a node that only opened workers: it cannot know where the
   * boundary is, and a lower bound claimed without knowing would be worse than the exact
   * number it replaces.
   *
   * @param scopedBpmnProcessId The process id as the cluster knows it
   * @param version The version a job reported
   * @return <code>true</code> where the version is older than the deployed one
   */
  public boolean predatesDeployedVersion(
      final String scopedBpmnProcessId,
      final int version) {

    final var deployed = deployedVersionByScopedProcessId.get(scopedBpmnProcessId);
    return (deployed != null) && (version < deployed);

  }

  @Override
  protected List<DeployedProcessVersion> fetchDeployedVersions(
      final String workflowModuleId,
      final String bpmnProcessId) {

    final var scopedProcessId = scopedProcessIds.apply(workflowModuleId, bpmnProcessId);
    final var tenantId = tenants.apply(workflowModuleId);
    try {
      final var definitions = client
          .get()
          .newProcessDefinitionSearchRequest()
          .filter(filter -> {
            onlyDefinitionsWhichStillCount(filter);
            filter.processDefinitionId(scopedProcessId);
            if (tenantId != null) {
              filter.tenantId(tenantId);
            }
          })
          .sort(sort -> sort.version().asc())
          .send()
          .join()
          .items();
      // this one search holds what every later question about an older version needs, and
      // keeping the keys is what spares those questions a search each
      definitions.forEach(definition -> remember(workflowModuleId, bpmnProcessId, definition));
      return definitions
          .stream()
          .map(definition -> DeployedProcessVersion
              .of(String.valueOf(definition.getVersion()), definition.getVersionTag()))
          .toList();
    } catch (final RuntimeException e) {
      if (!queryApi.answers()) {
        if (noQueryApiWarned.compareAndSet(false, true)) {
          log.warn(
              """
                  Camunda8[{}]: the cluster REFUSES to be searched, so the versions of BPMN \
                  process '{}' (workflow module '{}') cannot be asked for. Version specifications \
                  made of numbers (e.g. '>2') keep working - specifications naming a version TAG \
                  match nothing until the query API answers ({}).""",
              adapterId,
              bpmnProcessId,
              workflowModuleId,
              io.vanillabp.camunda8.client.Camunda8QueryApi.WHY_THE_CLUSTER_CANNOT_BE_SEARCHED);
        }
        return List.of();
      }
      throw e;
    }

  }

  @Override
  public String whatOlderVersionsMiss(
      final String workflowModuleId,
      final String bpmnProcessId) {

    // this adapter brings VanillaBP's behaviour by writing into the model it deploys,
    // and a running workflow stays on the version it was started on - so everything
    // listed here reaches the version deployed now and no earlier one. Camunda 7
    // answers nothing to the same question, because it attaches while the engine parses
    // a definition, which reaches every version the engine holds
    return "the end of a workflow is not reported to a @WorkflowEnded method, user-task lifecycle "
        + "notifications do not arrive where the listeners were added by this deployment, and a "
        + "message catch event correlates only by a correlation key its own model already carried";

  }

}
