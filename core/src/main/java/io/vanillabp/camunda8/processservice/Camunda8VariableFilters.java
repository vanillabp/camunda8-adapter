package io.vanillabp.camunda8.processservice;

/**
 * Builds the values Camunda 8's search API compares variables against.
 * <p>
 * The cluster stores every variable as JSON and the query API compares against that
 * JSON, verbatim: the client passes a filter value through without encoding it. A
 * variable holding the string <code>4711</code> therefore matches the filter value
 * <code>"4711"</code> - with the quotes - and never the plain <code>4711</code>.
 * Getting this wrong is invisible: the search simply returns nothing, which reads
 * exactly like "no such workflow".
 * <p>
 * VanillaBP writes the workflow aggregate's ID as a STRING (see
 * {@code Camunda8ProcessService#variablesOf}, which sends
 * <code>workflowAggregateId.toString()</code>), no matter which type the aggregate's
 * ID attribute has. The filter has to follow that decision rather than the Java type
 * at hand, which is why this class quotes unconditionally - the two belong together
 * and a test pins them.
 */
final class Camunda8VariableFilters {

  private Camunda8VariableFilters() {
  }

  /**
   * The JSON representation of an aggregate ID as the cluster holds it.
   *
   * @param workflowAggregateId The aggregate's ID
   * @return The quoted, escaped JSON string
   */
  static String aggregateIdSearchValue(
      final Object workflowAggregateId) {

    return "\"%s\"".formatted(
        String
            .valueOf(workflowAggregateId)
            .replace("\\", "\\\\")
            .replace("\"", "\\\""));

  }

}
