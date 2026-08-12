package io.vanillabp.camunda8.springboot.it;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * JPA workflow aggregate of the timer-start integration test (story 41). Its ID is a
 * String assigned by VanillaBP - nobody starts this workflow through the
 * {@code ProcessService}, so no application code could assign one.
 */
@Entity
@Table(name = "C8_TIMER_START_AGGREGATE")
@Getter
@Setter
public class TimerStartDockerAggregate {

  @Id
  private String id;

  private String processedBy;

}
