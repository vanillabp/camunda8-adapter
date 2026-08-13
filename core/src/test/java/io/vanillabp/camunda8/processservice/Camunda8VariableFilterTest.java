package io.vanillabp.camunda8.processservice;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * How an aggregate ID has to look in a variable filter (story 52). Camunda 8 compares
 * a variable against its stored JSON and the client passes the filter value through
 * verbatim, so a value which is not JSON matches nothing - and a search returning
 * nothing is indistinguishable from a workflow which does not exist. This defect cost
 * every operation electing its BPMS by probing, on every cluster WITH secondary
 * storage.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8VariableFilterTest {

  @Test
  @DisplayName("An aggregate ID is searched for as the JSON string the cluster holds")
  public void aggregateIdsAreSearchedForAsJsonStrings() {

    // VanillaBP writes the ID as a string whatever its Java type is, so the filter
    // quotes it whatever its Java type is - the two decisions belong together
    assertEquals("\"4711\"", Camunda8VariableFilters.aggregateIdSearchValue("4711"));
    assertEquals("\"4711\"", Camunda8VariableFilters.aggregateIdSearchValue(4711L));

    final var uuid = UUID.fromString("2949c750-88eb-422c-8663-9ead9be36319");
    assertEquals(
        "\"2949c750-88eb-422c-8663-9ead9be36319\"",
        Camunda8VariableFilters.aggregateIdSearchValue(uuid));

  }

  @Test
  @DisplayName("Quotes and backslashes in an ID stay valid JSON")
  public void oddIdsStayValidJson() {

    assertEquals("\"a\\\"b\"", Camunda8VariableFilters.aggregateIdSearchValue("a\"b"));
    assertEquals("\"a\\\\b\"", Camunda8VariableFilters.aggregateIdSearchValue("a\\b"));

  }

}
