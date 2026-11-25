package io.vanillabp.camunda8.springboot.processtest.it;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * The workflow aggregate of the review. The user task of the workflow notes its task id, and
 * the listener at the end of the workflow marks it as archived.
 */
@Entity
@Table(name = "PROCESS_TEST_REVIEW")
public class Review {

  @Id
  private String id;

  private String taskId;

  private boolean archived;

  /**
   * For JPA.
   */
  protected Review() {
  }

  /**
   * A new review, which no workflow has seen yet.
   *
   * @param id The id of the review
   */
  public Review(
      final String id) {

    this.id = id;

  }

  public String getId() {

    return id;

  }

  public String getTaskId() {

    return taskId;

  }

  public void setTaskId(
      final String taskId) {

    this.taskId = taskId;

  }

  public boolean isArchived() {

    return archived;

  }

  public void setArchived(
      final boolean archived) {

    this.archived = archived;

  }

}
