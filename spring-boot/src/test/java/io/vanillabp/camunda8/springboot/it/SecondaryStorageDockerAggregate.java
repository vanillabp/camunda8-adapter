package io.vanillabp.camunda8.springboot.it;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * JPA workflow aggregate of the secondary-storage integration test (story 52).
 */
@Entity
@Table(name = "C8_SECONDARY_STORAGE_AGGREGATE")
@Getter
@Setter
public class SecondaryStorageDockerAggregate {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private String processedBy;

}
