package io.vanillabp.camunda8.springboot.processtest.it;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Where the orders live.
 */
public interface OrderRepository extends JpaRepository<Order, String> {
}
