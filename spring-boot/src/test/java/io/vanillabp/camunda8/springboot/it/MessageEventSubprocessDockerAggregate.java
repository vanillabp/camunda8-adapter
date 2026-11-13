package io.vanillabp.camunda8.springboot.it;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * JPA workflow aggregate of the integration test of event subprocesses which start on a
 * message.
 */
@Entity
@Table(name = "C8_MESSAGE_EVENT_SUBPROCESS_AGGREGATE")
@Getter
@Setter
public class MessageEventSubprocessDockerAggregate {

  @Id
  private String id;

  /**
   * One entry per run of the event subprocess which does not interrupt the workflow. A text
   * and not a counter, because a number of the aggregate would have to be declared.
   */
  private String notes = "";

  /** Set by the event subprocess which interrupts the workflow. */
  private String takenOverBy;

}
