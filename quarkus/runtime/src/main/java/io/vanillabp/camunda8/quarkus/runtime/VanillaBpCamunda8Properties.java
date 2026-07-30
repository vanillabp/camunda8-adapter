package io.vanillabp.camunda8.quarkus.runtime;

import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;

/**
 * The Camunda 8 adapter's OVERLAY of the shared <code>vanillabp.*</code> configuration
 * tree: the adapter's connection settings live at the canonical per-adapter location
 * <code>vanillabp.adapters.&lt;id&gt;.*</code> (see
 * {@link Camunda8AdapterConfiguration}). A second RUN_TIME {@code @ConfigMapping} over
 * the same prefix coexists with the platform's mapping; since the platform dropped the
 * blanket {@code withMappingIgnore}, this overlay doubles as the unknown-key
 * validation coverage for the adapter's keys.
 * <p>
 * The adapter-id set is NEVER derived from this overlay map - it always comes from the
 * platform's core properties ({@code adapterTypes()} filtered by type
 * {@code camunda8}); the overlay is a per-known-id lookup only.
 */
@StaticInitSafe
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
@ConfigMapping(prefix = "vanillabp")
public interface VanillaBpCamunda8Properties {

  /**
   * The adapter sections of the shared tree, keyed by adapter ID - only the
   * Camunda 8 connection keys are modeled here.
   */
  Map<String, Camunda8AdapterKeys> adapters();

  /**
   * The Camunda 8 connection keys of one <code>vanillabp.adapters.&lt;id&gt;</code>
   * section (see {@link Camunda8AdapterConfiguration} for the semantics).
   */
  interface Camunda8AdapterKeys {

    /**
     * Connection mode: <code>self-managed</code> (default) or <code>saas</code>.
     */
    Optional<Camunda8AdapterConfiguration.Mode> mode();

    /**
     * REST API address of a self-managed cluster (e.g.
     * <code>http://localhost:8080</code>).
     */
    Optional<String> restAddress();

    /**
     * gRPC address of a self-managed cluster (required when
     * <code>prefer-rest-over-grpc</code> is <code>false</code>).
     */
    Optional<String> grpcAddress();

    /**
     * Whether the client uses the REST API (recommended, default) or gRPC for its
     * commands.
     */
    Optional<Boolean> preferRestOverGrpc();

    /**
     * The Camunda 8 multi-tenancy tenant (optional, both modes).
     */
    Optional<String> tenantId();

    /**
     * SaaS cluster ID.
     */
    Optional<String> clusterId();

    /**
     * SaaS region.
     */
    Optional<String> region();

    /**
     * SaaS OAuth client ID.
     */
    Optional<String> clientId();

    /**
     * SaaS OAuth client secret.
     */
    Optional<String> clientSecret();

  }

}
