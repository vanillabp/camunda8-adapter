package io.vanillabp.camunda8.springboot.listeners;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * JPA workflow aggregate of the modelled-listener integration test. Each flag is set by one
 * method, so the test can tell from the database which of the three elements reached the
 * application: the task, the listener of the task and the listener of the end event.
 * <p>
 * The flags survive although the cluster discards what a listener job sends back: VanillaBP
 * saves the aggregate in the application's own transaction, and only the cluster's copy of the
 * values is lost.
 */
@Entity
@Table(name = "C8_LISTENER_AGGREGATE")
@Getter
@Setter
public class ListenerDockerAggregate {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private boolean theWorkWasDone;

  private boolean theWorkWasAudited;

  private boolean theOrderWasArchived;

}
