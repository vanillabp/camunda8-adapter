package io.vanillabp.camunda8.springboot.foreigncluster;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ForeignClusterAggregateRepository extends JpaRepository<ForeignClusterAggregate, String> {
}
