package io.vanillabp.camunda8.quarkus.test;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * The workflow aggregate of the workflow the CLUSTER starts on its own. Its
 * id is a String, so the <code>&#64;WorkflowStartedByBpms</code> method can write the
 * trigger time into it - nobody starts this workflow through the
 * {@code ProcessService}, so there is no other moment at which it gets a name.
 */
@Entity
@Table(name = "C8_E2E_TIMER_AGGREGATE")
@Getter
@Setter
public class C8TimerAggregate {

  @Id
  private String id;

  private String processedBy;

  /**
   * Set by the <code>&#64;WorkflowEnded</code> method.
   */
  private String endedAs;

}
