package io.vanillabp.camunda8.springboot.openjobs;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * The workflow aggregate of {@link Camunda8OpenJobsWithoutARecordIT}. It holds nothing but its
 * id.
 */
@Entity
@Table(name = "C8_OPEN_JOBS_AGGREGATE")
@Getter
@Setter
public class OpenJobsAggregate {

  @Id
  private String id;

}
