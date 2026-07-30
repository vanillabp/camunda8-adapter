package io.vanillabp.camunda8.springboot.client;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import lombok.Getter;
import lombok.Setter;

/**
 * The Camunda 8 adapter's OVERLAY of the shared <code>vanillabp.*</code> configuration
 * tree: the adapter's connection settings live at the canonical per-adapter location
 * <code>vanillabp.adapters.&lt;id&gt;.*</code> (keys documented in
 * {@link Camunda8AdapterConfiguration}: <code>mode</code>, <code>rest-address</code>,
 * <code>grpc-address</code>, <code>prefer-rest-over-grpc</code>, <code>tenant-id</code>,
 * <code>cluster-id</code>, <code>region</code>, <code>client-id</code>,
 * <code>client-secret</code>). A second {@code @ConfigurationProperties} class over the
 * same prefix coexists with the platform's binding of the core model; keys unknown to
 * either view are ignored by the JavaBean binding.
 * <p>
 * The adapter-id set is NEVER derived from this overlay map - it always comes from the
 * platform's core properties ({@code adapterTypes()} filtered by type
 * {@code camunda8}); the overlay is a per-known-id lookup only (environment-variable
 * overrides can materialize phantom map entries in the overlay).
 */
@ConfigurationProperties("vanillabp")
@Getter
@Setter
public class VanillaBpCamunda8Properties {

  /**
   * The adapter sections of the shared tree, keyed by adapter ID - only the
   * Camunda 8 connection keys are modeled here (bound directly onto the
   * platform-neutral {@link Camunda8AdapterConfiguration}).
   */
  private Map<String, Camunda8AdapterConfiguration> adapters = Map.of();

}
