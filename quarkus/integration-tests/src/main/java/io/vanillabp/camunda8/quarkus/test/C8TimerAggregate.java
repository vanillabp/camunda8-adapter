package io.vanillabp.camunda8.quarkus.test;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * The workflow aggregate of the workflow the CLUSTER starts on its own (story 41). Its
 * id is a String assigned by VanillaBP - nobody starts this workflow through the
 * {@code ProcessService}, so no application code could assign one.
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
   * Set by the <code>&#64;WorkflowEnded</code> method (story 43).
   */
  private String endedAs;

}
