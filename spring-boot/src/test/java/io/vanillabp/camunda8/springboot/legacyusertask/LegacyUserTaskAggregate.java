package io.vanillabp.camunda8.springboot.legacyusertask;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * The workflow aggregate of {@link Camunda8LegacyUserTaskIT}. The test never completes a task,
 * so the aggregate holds nothing but its id.
 */
@Entity
@Table(name = "C8_LEGACY_USER_TASK_AGGREGATE")
@Getter
@Setter
public class LegacyUserTaskAggregate {

  @Id
  private String id;

}
