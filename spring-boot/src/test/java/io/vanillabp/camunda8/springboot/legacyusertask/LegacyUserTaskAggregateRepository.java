package io.vanillabp.camunda8.springboot.legacyusertask;

import org.springframework.data.jpa.repository.JpaRepository;

public interface LegacyUserTaskAggregateRepository extends JpaRepository<LegacyUserTaskAggregate, String> {
}
