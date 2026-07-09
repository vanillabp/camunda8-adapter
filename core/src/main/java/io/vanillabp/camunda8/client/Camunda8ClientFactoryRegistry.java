package io.vanillabp.camunda8.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds the {@link Camunda8ClientFactory} of every configured Camunda 8 adapter instance,
 * keyed by adapter ID. Created once per application by the platform integration
 * (Spring Boot / Quarkus) from the bound configuration and registered as a managed bean
 * so its {@link #close()} is invoked on application shutdown, closing all clients.
 * <p>
 * Factories are created lazily on first {@link #getFactory(String)}: an adapter ID
 * without any {@code camunda8-adapter.<id>.*} configuration still yields a factory (built
 * from an empty {@link Camunda8AdapterConfiguration}) so the application boots; the
 * missing configuration surfaces only when the client is actually used.
 */
public class Camunda8ClientFactoryRegistry implements AutoCloseable {

  private final Map<String, Camunda8AdapterConfiguration> configurationsByAdapterId;

  private final Map<String, Camunda8ClientFactory> factories = new ConcurrentHashMap<>();

  public Camunda8ClientFactoryRegistry(
      final Map<String, Camunda8AdapterConfiguration> configurationsByAdapterId) {

    this.configurationsByAdapterId = configurationsByAdapterId;

  }

  /**
   * @param adapterId The adapter ID
   * @return The (lazily created) client factory of the given adapter instance
   */
  public Camunda8ClientFactory getFactory(
      final String adapterId) {

    return factories.computeIfAbsent(
        adapterId,
        id -> new Camunda8ClientFactory(
            id, configurationsByAdapterId.getOrDefault(id, new Camunda8AdapterConfiguration())));

  }

  @Override
  public void close() {

    factories
        .values()
        .forEach(Camunda8ClientFactory::close);

  }

}
