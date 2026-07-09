package io.vanillabp.camunda8.springboot.smoke;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.repository.CrudRepository;

import io.vanillabp.integration.utils.SpringDataUtil;

/**
 * Minimal Spring Boot application used by {@link Camunda8AdapterDiscoveryTest} to prove
 * the Camunda 8 adapter is discovered when configured. It provides a
 * {@link SpringDataUtil} stub so the test does not need a real database - no workflow is
 * ever started, so persistence is never actually used.
 */
@SpringBootApplication
public class SmokeTestApplication {

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
