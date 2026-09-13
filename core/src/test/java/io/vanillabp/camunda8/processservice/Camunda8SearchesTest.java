package io.vanillabp.camunda8.processservice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import io.camunda.client.api.search.filter.ProcessDefinitionFilter;
import io.camunda.client.api.search.filter.ProcessInstanceFilter;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The three conditions a search for a workflow of this adapter is built from, asked of the
 * class an EXTENSION calls.
 * <p>
 * The one which is easy to get wrong is the variable value, which travels as the JSON the
 * cluster stores rather than as the plain id. A search built one condition differently
 * answers nothing, and nothing reads like a workflow nobody started - which is why this is
 * one class and not a rule written down twice.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8SearchesTest {

  @Test
  @DisplayName("A search by aggregate id compares the quoted JSON the cluster stores")
  public void aSearchByAggregateIdIsQuoted() {

    final var filter = mock(ProcessInstanceFilter.class);

    Camunda8Searches.byAggregateId(filter, "orderId", "4711");

    final var variables = ArgumentCaptor.forClass(Map.class);
    verify(filter).variables(variables.capture());
    assertEquals(
        Map.of("orderId", "\"4711\""),
        variables.getValue(),
        "the cluster compares the variable's JSON verbatim, so the id travels with its quotes");

  }

  @Test
  @DisplayName("A search by aggregate id alone names no process and no tenant")
  public void aSearchByAggregateIdDoesNotScope() {

    final var filter = mock(ProcessInstanceFilter.class);

    Camunda8Searches.byAggregateId(filter, "orderId", "4711");

    verify(filter, never()).processDefinitionId(org.mockito.ArgumentMatchers.anyString());
    verify(filter, never()).tenantId(org.mockito.ArgumentMatchers.anyString());

  }

  @Test
  @DisplayName("A scoped search names the process the CLUSTER knows, the tenant and the quoted id")
  public void aScopedSearchNamesAllThree() {

    final var filter = mock(ProcessInstanceFilter.class);

    Camunda8Searches.scopedTo(filter, "test-module__TestProcess", "test-module", "orderId", "4711");

    verify(filter).processDefinitionId("test-module__TestProcess");
    verify(filter).tenantId("test-module");
    verify(filter).variables(Map.of("orderId", "\"4711\""));

  }

  @Test
  @DisplayName("A workflow module without a tenant is searched without one")
  public void aModuleWithoutATenantIsNotFilteredByOne() {

    final var filter = mock(ProcessInstanceFilter.class);

    Camunda8Searches.scopedTo(filter, "TestProcess", null, "orderId", "4711");

    verify(filter).processDefinitionId("TestProcess");
    verify(filter, never()).tenantId(org.mockito.ArgumentMatchers.anyString());

  }

  @Test
  @DisplayName("A definition search is the same scope without the aggregate")
  public void aDefinitionSearchCarriesNoAggregate() {

    final var filter = mock(ProcessDefinitionFilter.class);

    Camunda8Searches.scopedTo(filter, "test-module__TestProcess", "test-module");

    verify(filter).processDefinitionId("test-module__TestProcess");
    verify(filter).tenantId("test-module");

  }

}
