package io.vanillabp.camunda8.springboot.client;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;

/**
 * Binds the provisional flat Camunda 8 connection configuration
 * ({@code camunda8-adapter.<adapter-id>.*}, see {@link Camunda8AdapterConfiguration}) and
 * exposes a {@link Camunda8ClientFactoryRegistry}. The registry owns one lazily built
 * {@code CamundaClient} per adapter ID; it is a managed bean so its {@code close()} is
 * called on application shutdown, closing all clients.
 * <p>
 * The configuration is bound programmatically through the {@link Binder} (rather than a
 * {@code @ConfigurationProperties} map) so an application configuring a Camunda 8 adapter
 * but without any connection properties still boots - a missing property surfaces only on
 * first use of the client.
 */
@AutoConfiguration
public class Camunda8ClientAutoConfiguration {

  @Bean(destroyMethod = "close")
  public Camunda8ClientFactoryRegistry camunda8ClientFactoryRegistry(
      final Environment environment) {

    final Map<String, Camunda8AdapterConfiguration> configurations = Binder
        .get(environment)
        .bind(
            Camunda8AdapterConfiguration.CONFIGURATION_PREFIX,
            Bindable.mapOf(String.class, Camunda8AdapterConfiguration.class))
        .orElseGet(HashMap::new);

    return new Camunda8ClientFactoryRegistry(configurations);

  }

}
