package io.vanillabp.camunda8.processservice;

import java.util.Map;

import io.camunda.client.api.search.filter.ProcessDefinitionFilter;
import io.camunda.client.api.search.filter.ProcessInstanceFilter;

/**
 * The filters this adapter searches its cluster with, in one place.
 * <p>
 * A search for a workflow is always the same three conditions: the BPMN process id AS THE
 * CLUSTER KNOWS IT, the tenant the workflow module runs under, and the workflow aggregate's
 * id as the JSON the cluster stores a variable in. Each of the three is a rule of this
 * adapter rather than of the Camunda client: the process id may have been rewritten by
 * name-clash avoidance, the tenant may be absent, and the variable value carries quotes
 * (see {@link Camunda8VariableFilters}).
 * <p>
 * Public because an extension searching the same cluster for the same workflow has to ask
 * the same question. A search built one condition differently does not fail: it answers
 * nothing, and nothing reads exactly like a workflow which was never started. Everything
 * here only ADDS conditions to the filter it is handed, so a caller who needs a narrower
 * search keeps calling the client after it.
 */
public final class Camunda8Searches {

  private Camunda8Searches() {
  }

  /**
   * Narrows a process-instance search to the workflow of one aggregate, over every process
   * and every tenant the cluster holds.
   * <p>
   * That is the search of an awareness probe: a probe is asked about a workflow and not
   * about a process, and on a cluster two adapter ids share it may find the other id's
   * instance as well. Sorting those out is the caller's, which is why this does not scope.
   *
   * @param filter The filter of the search request
   * @param aggregateIdVariableName The name of the variable the aggregate's id travels in -
   *          it is named after the aggregate's id attribute, so it comes from the
   *          persistence of the call at hand
   * @param workflowAggregateId The aggregate's id
   */
  public static void byAggregateId(
      final ProcessInstanceFilter filter,
      final String aggregateIdVariableName,
      final Object workflowAggregateId) {

    filter
        .variables(
            Map
                .of(
                    aggregateIdVariableName,
                    Camunda8VariableFilters.aggregateIdSearchValue(workflowAggregateId)));

  }

  /**
   * Narrows a process-instance search to the workflow of one aggregate in ONE process of
   * ONE tenant - the search of a caller which knows which process it is asking about.
   *
   * @param filter The filter of the search request
   * @param scopedBpmnProcessId The BPMN process id as the CLUSTER knows it, which is what
   *          {@code NameClashAvoidanceSupport#scopedProcessId} answers
   * @param tenantId The tenant of the workflow module, or <code>null</code> where the
   *          module uses none
   * @param aggregateIdVariableName The name of the variable the aggregate's id travels in
   * @param workflowAggregateId The aggregate's id
   */
  public static void scopedTo(
      final ProcessInstanceFilter filter,
      final String scopedBpmnProcessId,
      final String tenantId,
      final String aggregateIdVariableName,
      final Object workflowAggregateId) {

    filter.processDefinitionId(scopedBpmnProcessId);
    if (tenantId != null) {
      filter.tenantId(tenantId);
    }
    byAggregateId(filter, aggregateIdVariableName, workflowAggregateId);

  }

  /**
   * Narrows a process-DEFINITION search to one process of one tenant - the same scope
   * without the aggregate, because a definition belongs to no workflow.
   *
   * @param filter The filter of the search request
   * @param scopedBpmnProcessId The BPMN process id as the CLUSTER knows it
   * @param tenantId The tenant of the workflow module, or <code>null</code> where the
   *          module uses none
   */
  public static void scopedTo(
      final ProcessDefinitionFilter filter,
      final String scopedBpmnProcessId,
      final String tenantId) {

    filter.processDefinitionId(scopedBpmnProcessId);
    if (tenantId != null) {
      filter.tenantId(tenantId);
    }

  }

}
