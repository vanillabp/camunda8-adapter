package io.vanillabp.camunda8;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Adapter-specific processing context accumulated across all BPMN files of a workflow
 * module during the deployment pipeline
 * ({@code readBpmn} &rarr; {@code prepareBpmn} &rarr; {@code wireBpmn} &rarr;
 * {@code deployResources} &rarr; {@code startWorkflowProcessing}).
 * <p>
 * In this skeleton it only carries the workflow-module ID. Wiring information (job
 * workers, deployment resources, etc.) will be added by later feature stories.
 */
@Getter
@RequiredArgsConstructor
public class Camunda8ProcessingContext {

  private final String workflowModuleId;

}
