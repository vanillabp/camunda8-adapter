package io.vanillabp.camunda8.springboot.it;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * The aggregate of the old-process-versions integration test (story 57).
 */
@Entity
@Table(name = "C8_STORY57_TEST")
@Getter
@Setter
public class Story57DockerAggregate {

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "c8Story57Seq")
  @SequenceGenerator(name = "c8Story57Seq", initialValue = 900000, allocationSize = 1)
  private Long id;

  private String servedBy;

}
