# VanillaBP adapter for Camunda 8

This is the [VanillaBP](https://www.vanillabp.io) adapter for
[Camunda 8](https://camunda.com/platform/) (Version 2). It lets a VanillaBP business
application run its workflows on a Camunda 8 cluster without the business code depending
on the Camunda API.

## Status

**Skeleton.** This repository currently contains only the structural skeleton: the
Maven modules, the adapter SPI implementations as stubs and the platform registration
(Spring Boot auto-configuration and Quarkus extension). It is enough to boot an
application with the adapter configured and to have it discovered on both platforms, but
it does **not** yet connect to a cluster, deploy BPMN or start workflows. The
BPMS-specific behavior (client construction and configuration, BPMN deployment,
workflow start, job workers, awareness/election, variable handling) is added by later
feature stories; the stub methods throw `UnsupportedOperationException` until then.

## Dependencies

All artifacts use the groupId `org.camunda.community.vanillabp` and version
`2.0.0-SNAPSHOT`.

### Spring Boot

Add a single dependency; it transitively pulls in the platform-neutral core and the
required VanillaBP platform integration:

```xml
<dependency>
  <groupId>org.camunda.community.vanillabp</groupId>
  <artifactId>camunda8-adapter-spring-boot</artifactId>
  <version>2.0.0-SNAPSHOT</version>
</dependency>
```

### Quarkus

Both VanillaBP and the adapter are Quarkus extensions, so both must be added explicitly:

```xml
<dependency>
  <groupId>io.vanillabp</groupId>
  <artifactId>vanillabp-quarkus-integration</artifactId>
  <version>2.0.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>org.camunda.community.vanillabp</groupId>
  <artifactId>camunda8-adapter-quarkus</artifactId>
  <version>2.0.0-SNAPSHOT</version>
</dependency>
```

## Configuration

The adapter is a *type* named `camunda8`. Configure one or more adapter *instances* of
that type and reference them in the prioritized-adapters list:

```yaml
vanillabp:
  adapters:
    myengine:
      type: camunda8
  prioritized-adapters:
    - myengine
```

The adapter ID (`myengine` above) identifies an adapter *instance*; the same BPMS type
may be configured multiple times with different IDs (the central migration scenario:
e.g. an old on-prem cluster and a new SaaS cluster side by side). Client connection
settings are added by a later story.

## Camunda 8 client

The adapter uses the plain Java client `io.camunda:camunda-client-java` (8.8.x), **not**
Camunda's Spring SDK / Spring Zeebe: VanillaBP does platform wiring and configuration
itself, so a client that carries its own platform integration would conflict with it.
(The deprecated `io.camunda:zeebe-client-java` is deliberately avoided.)

Camunda 8 is a remote, eventually consistent engine that cannot join the application's
local database transaction. Starting a workflow therefore uses VanillaBP's two-phase
commit (`needsTwoPhaseCommitForStartingWorkflows() == true`): phase one only validates,
the actual process-instance creation runs in phase two through the core phase-two
outbox.

## Building

Prerequisites (built and installed into the local Maven repository first, in this
order): `spi-for-java`, then `adapter-platform-integration`. Then:

```bash
mvn install verify
```

