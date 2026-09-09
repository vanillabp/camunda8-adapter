package io.vanillabp.camunda8;

import java.util.Collection;

import io.vanillabp.integration.adapter.spi.NameClashAvoidance;
import io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport;

/**
 * The core's name-clash avoidance reduced to what an adapter test needs of it: one mode for
 * every workflow module, and the prefixing the core would do in that mode. The adapter is
 * tested against the SPI rather than against the core's implementation, which this module
 * deliberately does not depend on.
 */
public final class TestScoping {

  private TestScoping() {
  }

  /**
   * @param mode What every workflow module's mode is
   * @return A support answering that mode and prefixing accordingly
   */
  public static NameClashAvoidanceSupport of(
      final NameClashAvoidance mode) {

    return new NameClashAvoidanceSupport() {

      private boolean prefixes() {

        return mode == NameClashAvoidance.USE_PREFIX;

      }

      @Override
      public NameClashAvoidance modeFor(
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
            ? String.join(SEPARATOR, workflowModuleId, bpmnProcessId)
            : bpmnProcessId;

      }

      @Override
      public String scopedIdentifier(
          final String workflowModuleId,
          final String identifier,
          final String adapterId) {

        return prefixes() && (identifier != null)
            ? String.join(SEPARATOR, workflowModuleId, identifier)
            : identifier;

      }

      @Override
      public String scopedTaskDefinition(
          final String workflowModuleId,
          final String bpmnProcessId,
          final String taskDefinition,
          final String adapterId) {

        return prefixes() && (taskDefinition != null)
            ? String.join(SEPARATOR, workflowModuleId, bpmnProcessId, taskDefinition)
            : taskDefinition;

      }

      @Override
      public String plainProcessId(
          final String workflowModuleId,
          final String scopedBpmnProcessId,
          final String adapterId) {

        final var prefix = workflowModuleId + SEPARATOR;
        return scopedBpmnProcessId.startsWith(prefix)
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

        final var prefix = String.join(SEPARATOR, workflowModuleId, bpmnProcessId, "");
        return scopedTaskDefinition.startsWith(prefix)
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
          final Collection<DeployedProcess> deployedProcesses) {

      }

    };

  }

}
