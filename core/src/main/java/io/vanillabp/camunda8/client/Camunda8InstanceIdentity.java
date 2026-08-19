package io.vanillabp.camunda8.client;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * What makes two configured <code>camunda8</code> adapter ids DIFFERENT clusters
 * (story 34). Camunda 8 is remote, so two ids are distinct if they address
 * different systems - or the same system with different credentials:
 * <ul>
 * <li><b>self-managed:</b> different <code>rest-address</code>/
 * <code>grpc-address</code>;</li>
 * <li><b>SaaS:</b> a different COMBINATION of <code>cluster-id</code> and
 * <code>client-id</code> - addressing one cluster with two OAuth clients is a
 * legitimate setup (separated permissions or quotas per workflow module), so the
 * client id counts;</li>
 * <li>additionally the <code>tenant-id</code>: the same cluster with different
 * multi-tenancy tenants are different systems from the application's view;</li>
 * <li>and - since story 35 - a different <code>name-clash-avoidance</code> mode: the
 * identifiers of the SAME workflow module look different in each of them (a tenant
 * versus prefixed identifiers), which is exactly the setup used to MIGRATE from
 * tenants to prefixes: two ids on one cluster, differing only in that mode, the new
 * one first in <code>prioritized-adapters</code>.</li>
 * </ul>
 * Two ids whose relevant keys are identical are the same instance - configuring
 * them as separate adapters is an error.
 */
public final class Camunda8InstanceIdentity {

  private Camunda8InstanceIdentity() {
  }

  /**
   * The comparable identity of an adapter id's connection configuration.
   *
   * @param configuration The adapter id's connection configuration
   * @return The identity (never <code>null</code>)
   */
  static String identityOf(
      final Camunda8AdapterConfiguration configuration) {

    return identityOf(configuration, null);

  }

  /**
   * The comparable identity of an adapter id's connection configuration, including
   * the name-clash-avoidance mode it uses (story 35).
   *
   * @param configuration The adapter id's connection configuration
   * @param nameClashAvoidance The mode configured for the adapter or
   *          <code>null</code> if unknown
   * @return The identity (never <code>null</code>)
   */
  static String identityOf(
      final Camunda8AdapterConfiguration configuration,
      final io.vanillabp.integration.adapter.spi.NameClashAvoidance nameClashAvoidance) {

    final var scoping = nameClashAvoidance == null
        ? ""
        : ", name-clash avoidance '%s'".formatted(nameClashAvoidance.name().toLowerCase().replace('_', '-'));
    return connectionIdentityOf(configuration) + scoping;

  }

  private static String connectionIdentityOf(
      final Camunda8AdapterConfiguration configuration) {

    if (configuration == null) {
      return "<unconfigured>";
    }
    if (configuration.getMode() == Camunda8AdapterConfiguration.Mode.SAAS) {
      return "SaaS cluster '%s' (region '%s') accessed by client '%s', tenant '%s'".formatted(
          configuration.getClusterId(),
          configuration.getRegion(),
          configuration.getClientId(),
          configuration.getTenantId());
    }
    // who the adapter authenticates as is part of the identity since story 88: one
    // self-managed cluster serving two applications with separate accounts is the same
    // legitimate setup the SaaS client id already stood for
    return "self-managed cluster (rest-address '%s', grpc-address '%s'), tenant '%s', authenticated as '%s'"
        .formatted(
            configuration.getRestAddress(),
            configuration.getGrpcAddress(),
            configuration.getTenantId(),
            configuration.getAuth().principal(configuration));

  }

  /**
   * Fails the boot if two of the given adapter ids address the same cluster with
   * the same credentials and tenant (see the class comment). Called through the
   * adapter SPI hook
   * {@code AdapterDeploymentService#validateDistinctAdapterInstances}.
   *
   * @param adapterIds The configured adapter ids of type <code>camunda8</code>
   * @param configurationResolver Resolves an adapter id's connection configuration
   */
  public static void validateDistinct(
      final List<String> adapterIds,
      final Function<String, Camunda8AdapterConfiguration> configurationResolver) {

    validateDistinct(adapterIds, configurationResolver, null);

  }

  /**
   * Fails the boot if two of the given adapter ids address the same cluster with the
   * same credentials, tenant AND name-clash-avoidance mode (see the class comment).
   *
   * @param adapterIds The configured adapter ids of type <code>camunda8</code>
   * @param configurationResolver Resolves an adapter id's connection configuration
   * @param scoping The core's name-clash-avoidance support or <code>null</code>
   */
  public static void validateDistinct(
      final List<String> adapterIds,
      final Function<String, Camunda8AdapterConfiguration> configurationResolver,
      final io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport scoping) {

    if ((adapterIds == null) || (adapterIds.size() < 2) || (configurationResolver == null)) {
      return;
    }

    final var idsByIdentity = new LinkedHashMap<String, List<String>>();
    adapterIds.forEach(
        adapterId -> idsByIdentity
            .computeIfAbsent(
                identityOf(
                    configurationResolver.apply(adapterId),
                    scoping == null
                        ? null
                        : scoping.modeFor(null, null, adapterId)),
                identity -> new LinkedList<>())
            .add(adapterId));

    idsByIdentity.forEach((
        identity,
        idsSharingIt) -> {
      if (idsSharingIt.size() < 2) {
        return;
      }
      throw new IllegalStateException(
          """
              The Camunda 8 adapters '%s' address the SAME cluster with the same credentials \
              (%s)! Two adapter ids of one BPMS type only make sense if they address different \
              systems - the BPMS election would otherwise ask the same cluster twice. Make them \
              distinguishable:
                - self-managed: give each id its own 'vanillabp.adapters.<id>.rest-address' \
              (respectively 'grpc-address'), or - to address one cluster with separated \
              permissions - its own credentials below 'vanillabp.adapters.<id>.auth',
                - SaaS: give each id its own 'vanillabp.adapters.<id>.cluster-id' or - to \
              address one cluster with separated permissions - its own 'client-id',
                - or, when MIGRATING from tenants to prefixed identifiers on ONE cluster: give \
              each id its own 'vanillabp.adapters.<id>.name-clash-avoidance',
              or remove all but one of these adapters."""
              .formatted(String.join("', '", idsSharingIt), identity));
    });

  }

}
