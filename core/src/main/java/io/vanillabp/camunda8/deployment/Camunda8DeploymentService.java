package io.vanillabp.camunda8.deployment;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.integration.adapter.spi.AdapterDeploymentService;
import io.vanillabp.integration.adapter.spi.BpmnParseException;
import lombok.RequiredArgsConstructor;

/**
 * Camunda 8 implementation of the {@link AdapterDeploymentService}. One instance is
 * created per configured adapter ID (not per adapter type) because the same BPMS type
 * may be configured multiple times (BPMS migration).
 * <p>
 * The BPMN model type is {@link BpmnModelInstance}, shipped with the Camunda 8 client
 * via {@code io.camunda:zeebe-bpmn-model} (a transitive dependency of
 * {@code io.camunda:camunda-client-java}). The processing context is
 * {@link Camunda8ProcessingContext}.
 * <p>
 * <b>Skeleton stage:</b> only the identity/type getters are implemented. All pipeline
 * methods throw {@link UnsupportedOperationException} - they are implemented in later
 * stories. Silent no-op stubs are deliberately avoided so wiring bugs surface loudly.
 */
@RequiredArgsConstructor
public class Camunda8DeploymentService implements AdapterDeploymentService<BpmnModelInstance, Camunda8ProcessingContext> {

  /**
   * The adapter type of the Camunda 8 adapter. Constant across all instances; the
   * adapter ID (see {@link #getAdapterId()}) distinguishes instances.
   */
  public static final String ADAPTER_TYPE = "camunda8";

  private final String adapterId;

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

    throw new UnsupportedOperationException("readBpmn is implemented in a later story");

  }

  @Override
  public Camunda8ProcessingContext prepareBpmn(
      final String workflowModuleId,
      final Camunda8ProcessingContext existingContext,
      final String filename,
      final String bpmnProcessId,
      final BpmnModelInstance model) {

    throw new UnsupportedOperationException("prepareBpmn is implemented in a later story");

  }

  @Override
  public void wireBpmn(
      final String workflowModuleId,
      final String filename,
      final String bpmnProcessId,
      final BpmnModelInstance model,
      final Camunda8ProcessingContext context) {

    throw new UnsupportedOperationException("wireBpmn is implemented in a later story");

  }

  @Override
  public void deployResources(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext) throws IllegalStateException {

    throw new UnsupportedOperationException("deployResources is implemented in a later story");

  }

  @Override
  public void startWorkflowProcessing(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext) {

    throw new UnsupportedOperationException("startWorkflowProcessing is implemented in a later story");

  }

  @Override
  public void stopWorkflowProcessing(
      final String workflowModuleId,
      final Camunda8ProcessingContext bpmsProcessingContext) {

    throw new UnsupportedOperationException("stopWorkflowProcessing is implemented in a later story");

  }

}
