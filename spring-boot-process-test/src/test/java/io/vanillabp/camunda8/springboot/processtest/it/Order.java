package io.vanillabp.camunda8.springboot.processtest.it;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * The workflow aggregate. The service task of the workflow sets its status.
 */
@Entity
@Table(name = "PROCESS_TEST_ORDER")
public class Order {

  @Id
  private String id;

  private String status;

  /**
   * For JPA.
   */
  protected Order() {
  }

  /**
   * A new order, which no workflow has seen yet.
   *
   * @param id The id of the order
   */
  public Order(
      final String id) {

    this.id = id;
    this.status = "NEW";

  }

  public String getId() {

    return id;

  }

  public String getStatus() {

    return status;

  }

  public void setStatus(
      final String status) {

    this.status = status;

  }

}
