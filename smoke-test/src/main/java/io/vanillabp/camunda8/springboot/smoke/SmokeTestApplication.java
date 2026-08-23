package io.vanillabp.camunda8.springboot.smoke;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.repository.CrudRepository;

import io.vanillabp.integration.utils.SpringDataUtil;

/**
 * Minimal Spring Boot application used by {@link Camunda8AdapterDiscoveryTest} to prove
 * the Camunda 8 adapter is discovered when configured. It lives in a MAVEN MODULE of its
 * own, and so does its workflow module {@code smoke-app}: a workflow module is a
 * classpath entry carrying {@code META-INF/workflow-module}, so a scenario sharing that
 * entry with another one shares its workflow module - and every application is then asked
 * for the persistence of the other scenario's aggregates, which since story 114 of the
 * platform ends the startup (story 118). It provides a
 * {@link SpringDataUtil} stub so the test does not need a real database - no workflow is
 * ever started, so persistence is never actually used.
 * <p>
 * It also says who owns {@link Aggregate}. Since story 114 of the platform an aggregate
 * without a persistence ends the startup, because the fallback would look for a Spring
 * Data repository and this application has no reason to have one. The double below is
 * that answer, and every method of it fails loudly so a test which starts persisting for
 * real hears about it.
 */
@SpringBootApplication
public class SmokeTestApplication {

  @Bean
  public io.vanillabp.integration.spi.AggregatePersistenceAware<Object> noPersistenceForTheAggregate() {

    return new io.vanillabp.integration.spi.AggregatePersistenceAware<>() {

      @Override
      public Class<Object> getAggregateClass() {
        // every aggregate is an Object, and at the greatest inheritance distance there is,
        // so a double declared for a specific class would always win over this one
        return Object.class;
      }

      @Override
      public Object save(
          final Object aggregate) {
        throw new UnsupportedOperationException("no persistence in this test");
      }

      @Override
      public Object getAggregateId(
          final Object aggregate) {
        throw new UnsupportedOperationException("no persistence in this test");
      }

      @Override
      public Object loadById(
          final Object aggregateId) {
        throw new UnsupportedOperationException("no persistence in this test");
      }

      @Override
      public Class<?> getAggregateIdType() {
        // the contract's "not determinable": this double owns the serialized form, as far
        // as it owns anything at all
        return null;
      }

    };

  }

  @Bean
  public SpringDataUtil testSpringDataUtil() {

    return new SpringDataUtil() {

      @Override
      public <O> CrudRepository<? super O, Object> getRepository(
          final O entity) {
        throw new UnsupportedOperationException("no persistence in this test");
      }

      @Override
      public <O> CrudRepository<O, Object> getRepository(
          final Class<O> type) {
        throw new UnsupportedOperationException("no persistence in this test");
      }

      @Override
      public <I> I getId(
          final Object entity) {
        throw new UnsupportedOperationException("no persistence in this test");
      }

      @Override
      public String getIdName(
          final Class<?> type) {
        throw new UnsupportedOperationException("no persistence in this test");
      }

      @Override
      public Class<?> getIdType(
          final Class<?> type) {
        throw new UnsupportedOperationException("no persistence in this test");
      }

      @Override
      public <O> O unproxy(
          final O entity) {
        throw new UnsupportedOperationException("no persistence in this test");
      }

      @Override
      public <O> boolean isPersistedEntity(
          final Class<O> entityClass,
          final O entity) {
        throw new UnsupportedOperationException("no persistence in this test");
      }

    };

  }

}
