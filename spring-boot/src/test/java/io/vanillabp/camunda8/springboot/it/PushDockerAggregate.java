package io.vanillabp.camunda8.springboot.it;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * JPA workflow aggregate of the aggregateChanged integration test (story 44). Every
 * attribute is shared with Camunda 8 (this adapter's sync mode is FULL), so the
 * cluster holds {@link #note} as a process variable - which is what the test reads
 * back to see WHERE a push landed.
 */
@Entity
@Table(name = "C8_PUSH_AGGREGATE")
@Getter
@Setter
public class PushDockerAggregate {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private String note;

  /**
   * The parked jobs of the multi-instance activity, comma-separated.
   */
  private String taskIds;

}
