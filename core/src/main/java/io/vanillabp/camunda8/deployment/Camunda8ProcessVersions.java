package io.vanillabp.camunda8.deployment;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.camunda.client.CamundaClient;
import io.vanillabp.integration.adapter.spi.version.CachingProcessVersionCatalog;
import io.vanillabp.integration.adapter.spi.version.DeployedProcessVersion;
import lombok.extern.slf4j.Slf4j;

/**
 * The versions of the process definitions of ONE Camunda 8 cluster (= one adapter id):
 * what the core matches <code>&#64;WorkflowTask(version = ...)</code> and its siblings
 * against (story 48).
 * <p>
 * The version itself travels with every job ({@code ActivatedJob#getProcessDefinitionVersion}),
 * so nothing here is needed for version specifications made of numbers. Version TAGS
 * are a different matter: a job does not carry one, and the cluster only tells which
 * version carries which tag through the query API (secondary storage). Where a cluster
 * runs without it, the tag stays unknown and is reported once - version specifications
 * made of numbers keep working.
 * <p>
 * What the deployment reported is recorded without any query
 * ({@link #recordDeployed(String, String, int, String)}): the deploy command
 * names the version the cluster assigned, and the model carries its
 * {@code zeebe:versionTag}.
 */
@Slf4j
public class Camunda8ProcessVersions extends CachingProcessVersionCatalog {

  private final String adapterId;

  private final java.util.function.Supplier<CamundaClient> client;

  /**
   * The BPMN process id as the CLUSTER knows it for a (workflow module, plain BPMN
   * process id) - the identifiers may be prefixed (story 35).
   */
  private final BiFunction<String, String, String> scopedProcessIds;

  /**
   * The tenant a workflow module is deployed to, or <code>null</code> (story 35).
   */
  private final Function<String, String> tenants;

  private final AtomicBoolean noQueryApiWarned = new AtomicBoolean();

  public Camunda8ProcessVersions(
      final String adapterId,
      final java.util.function.Supplier<CamundaClient> client,
      final BiFunction<String, String, String> scopedProcessIds,
      final Function<String, String> tenants) {

    this.adapterId = adapterId;
    this.client = client;
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

  @Override
  protected List<DeployedProcessVersion> fetchDeployedVersions(
      final String workflowModuleId,
      final String bpmnProcessId) {

    final var scopedProcessId = scopedProcessIds.apply(workflowModuleId, bpmnProcessId);
    final var tenantId = tenants.apply(workflowModuleId);
    try {
      return client
          .get()
          .newProcessDefinitionSearchRequest()
          .filter(filter -> {
            filter.processDefinitionId(scopedProcessId);
            if (tenantId != null) {
              filter.tenantId(tenantId);
            }
          })
          .sort(sort -> sort.version().asc())
          .send()
          .join()
          .items()
          .stream()
          .map(definition -> DeployedProcessVersion
              .of(String.valueOf(definition.getVersion()), definition.getVersionTag()))
          .toList();
    } catch (final RuntimeException e) {
      if (isSecondaryStorageMissing(e)) {
        if (noQueryApiWarned.compareAndSet(false, true)) {
          log.warn(
              """
                  Camunda8[{}]: the cluster runs WITHOUT secondary storage, so the versions of BPMN \
                  process '{}' (workflow module '{}') cannot be asked for. Version specifications \
                  made of numbers (e.g. '>2') keep working - specifications naming a version TAG \
                  match nothing until the query API is configured (camunda.database.type / \
                  secondary storage).""",
              adapterId,
              bpmnProcessId,
              workflowModuleId);
        }
        return List.of();
      }
      throw e;
    }

  }

  private static boolean isSecondaryStorageMissing(
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

}
