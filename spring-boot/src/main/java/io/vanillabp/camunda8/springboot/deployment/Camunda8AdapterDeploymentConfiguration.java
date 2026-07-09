package io.vanillabp.camunda8.springboot.deployment;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;

import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.vanillabp.camunda8.Camunda8ProcessingContext;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.deployment.Camunda8DeploymentService;
import io.vanillabp.camunda8.springboot.Camunda8AdapterConfiguration;
import io.vanillabp.integration.adapter.migration.config.MigrationAdapterProperties;
import io.vanillabp.integration.adapter.spi.AdapterDeploymentService;
import io.vanillabp.integration.processservice.SpringBootMigrationAdapterAutoConfiguration;
import io.vanillabp.integration.workflowmodule.WorkflowModule;
import io.vanillabp.integration.workflowmodule.WorkflowModules;

/**
 * Builds one {@link Camunda8DeploymentService} per configured adapter ID of type
 * {@code camunda8}. Deployment services exist per adapter ID (not per type) because the
 * same BPMS type may be configured multiple times (BPMS migration).
 */
@AutoConfiguration(after = SpringBootMigrationAdapterAutoConfiguration.class)
public class Camunda8AdapterDeploymentConfiguration {

  @Bean
  public List<AdapterDeploymentService<BpmnModelInstance, Camunda8ProcessingContext>> camunda8DeploymentServices(
      final WorkflowModules allWorkflowModules,
      final MigrationAdapterProperties properties,
      final Camunda8ClientFactoryRegistry clientFactoryRegistry) {

    final List<AdapterDeploymentService<BpmnModelInstance, Camunda8ProcessingContext>> deploymentServices = new ArrayList<>();
    final Set<String> adaptersBuilt = new HashSet<>();

    // walk through all workflow modules
    allWorkflowModules
        .getWorkflowModules()
        .stream()
        .map(WorkflowModule::getId)
        // for each adapter configured...
        .forEach(workflowModuleId -> properties
            .getPrioritizedAdaptersFor(workflowModuleId)
            .stream()
            // ...find adapters of the Camunda 8 type...
            .filter(adapterId -> properties
                .getAdapters()
                .get(adapterId)
                .equals(Camunda8AdapterConfiguration.ADAPTER_TYPE))
            .forEach(adapterId -> {

              // avoid building the same adapter more than once
              if (adaptersBuilt.contains(adapterId)) {
                return;
              }

              deploymentServices.add(new Camunda8DeploymentService(
                  adapterId, clientFactoryRegistry.getFactory(adapterId)));
              adaptersBuilt.add(adapterId);

            }));

    return deploymentServices;

  }

}
