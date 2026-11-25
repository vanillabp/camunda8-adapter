package io.vanillabp.camunda8.springboot.processtest.it;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Where the reviews live.
 */
public interface ReviewRepository extends JpaRepository<Review, String> {
}
