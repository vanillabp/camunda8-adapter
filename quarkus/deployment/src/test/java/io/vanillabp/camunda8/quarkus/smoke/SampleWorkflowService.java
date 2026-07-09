package io.vanillabp.camunda8.quarkus.smoke;

import io.vanillabp.integration.spi.AggregatePersistenceAware;
import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * A minimal workflow service so a {@link ProcessService} bean is built and the adapter
 * wiring is exercised. No BPMN file is provided, so deployment never runs. Persistence
 * is implemented inline but never used (no workflow is started).
 */
@Singleton
@WorkflowService(workflowAggregateClass = Aggregate.class)
public class SampleWorkflowService implements AggregatePersistenceAware<Aggregate> {

  @Inject
  ProcessService<Aggregate> processService;

  @Override
  public Class<Aggregate> getAggregateClass() {
    return Aggregate.class;
  }

  @Override
  public Aggregate save(
      final Aggregate aggregate) {
    return null; // not necessary for this test
  }

  @Override
  public Object getAggregateId(
      final Aggregate aggregate) {
    return null; // not necessary for this test
  }

  @WorkflowTask
  public void doSomething() {
    // no-op; only used to trigger process-service wiring
  }

}
