package io.vanillabp.camunda8.springboot.foreigncluster;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * The workflow aggregate of {@link Camunda8ForeignClusterAtStartupIT}. It holds nothing but its
 * id.
 */
@Entity
@Table(name = "C8_FOREIGN_CLUSTER_AGGREGATE")
@Getter
@Setter
public class ForeignClusterAggregate {

  @Id
  private String id;

}
