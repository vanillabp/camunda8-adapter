# VanillaBP adapter for Camunda 8

This is the [VanillaBP](https://www.vanillabp.io) adapter for
[Camunda 8](https://camunda.com/platform/) (Version 2). It lets a VanillaBP business
application run its workflows on a Camunda 8 cluster without the business code depending
on the Camunda API.

## Status

**Early.** This repository connects to a Camunda 8 cluster, deploys the BPMN resources of
each workflow module on startup and starts workflow instances end-to-end via the
two-phase outbox (see [Behavior](#behavior)). Not yet implemented (later stories): job
workers / `@WorkflowTask` execution, message correlation, awareness/election,
`@SyncWithBPMS` variable sync and the viewer/history API - those SPI methods still throw
`UnsupportedOperationException`.

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
e.g. an old on-prem cluster and a new SaaS cluster side by side).

### Connecting to a Camunda 8 cluster

Each adapter instance is connected to a cluster through the **canonical per-adapter
configuration location** `vanillabp.adapters.<adapter-id>.*` - the adapter contributes
its own keys to the shared VanillaBP tree via platform OVERLAYS (Spring Boot: a second
`@ConfigurationProperties("vanillabp")` class; Quarkus: a second RUN_TIME
`@ConfigMapping(prefix = "vanillabp")`, which also provides the unknown-key validation
coverage for these keys). The values are turned into a plain-Java `CamundaClient` built
lazily on first use. The adapter-id set always comes from the platform's core
properties (ids of type `camunda8`); the overlay maps are per-known-id lookups only.

|                    Property                     |  Applies to  |                  Required                  |                  Description                   |
|-------------------------------------------------|--------------|--------------------------------------------|------------------------------------------------|
| `vanillabp.adapters.<id>.mode`                  | both         | no (default `self-managed`)                | `self-managed` or `saas`                       |
| `vanillabp.adapters.<id>.rest-address`          | self-managed | yes (unless `prefer-rest-over-grpc=false`) | REST API address, e.g. `http://localhost:8080` |
| `vanillabp.adapters.<id>.grpc-address`          | self-managed | only if `prefer-rest-over-grpc=false`      | gRPC address, e.g. `http://localhost:26500`    |
| `vanillabp.adapters.<id>.prefer-rest-over-grpc` | self-managed | no (default `true`)                        | use the REST API (recommended) or gRPC         |
| `vanillabp.adapters.<id>.cluster-id`            | saas         | yes                                        | SaaS cluster ID                                |
| `vanillabp.adapters.<id>.region`                | saas         | yes                                        | SaaS region                                    |
| `vanillabp.adapters.<id>.client-id`             | saas         | yes                                        | OAuth client ID                                |
| `vanillabp.adapters.<id>.client-secret`         | saas         | yes                                        | OAuth client secret                            |
| `vanillabp.adapters.<id>.tenant-id`             | both         | no                                         | Camunda 8 multi-tenancy tenant                 |

Example (self-managed):

```yaml
vanillabp:
  adapters:
    myengine:
      type: camunda8
      mode: self-managed
      rest-address: http://localhost:8080
```

**Boot behavior:** An application which configures a Camunda 8 adapter but leaves the
connection properties out still boots. The client is built lazily and the configuration
is validated on first use; a missing property fails with a message naming the exact
property (e.g. `vanillabp.adapters.myengine.rest-address`). If a workflow module has BPMN
files but the client is unconfigured, deployment on startup fails with that message.

### Behavior

- **Deployment (on startup):** the BPMN resources of each workflow module are deployed in
  a single `DeployResourceCommand` per module. Camunda 8 has no Camunda-7-style
  tenant-per-module; the configured `tenant-id` (if any) is used, otherwise the default
  tenant. **Workflow-module isolation therefore relies on unique BPMN process IDs across
  modules for now.**
- **Starting a workflow (two-phase):** Camunda 8 is remote and eventually consistent and
  cannot join the application's database transaction, so
  `needsTwoPhaseCommitForStartingWorkflows()` is `true`.
  - *Phase one* runs inside the caller's transaction and only **validates** (resolves the
    aggregate ID, verifies the client is configured). It never contacts the cluster - a
    remote call here would reintroduce ghost workflows on rollback.
  - *Phase two* runs after the commit (through the core phase-two outbox) and creates the
    process instance of the latest version with a single process variable holding the
    workflow aggregate's ID (as a string). The variable is named after the aggregate's ID
    property (`AggregatePersistenceAware.getAggregateIdName()`) - how the aggregate's ID
    is stored in the BPMS is the adapter's decision, and Camunda 8 stores the aggregate
    as process variables. No other variables are set (aggregate attribute sync is the
    `@SyncWithBPMS` story).

### Idempotency limitation

The phase-two outbox has at-least-once semantics: a crash between a successful
`CreateProcessInstance` and the removal of the outbox entry can start the same workflow
**twice** (at-least-once, duplicates possible). Strict deduplication needs the core-side
`WorkflowInstanceRegistry` (a separate story); no Camunda-8-side workaround is attempted
here.

### Testing

- **Core unit tests** (no Docker): BPMN parsing / executable-process extraction, client
  configuration validation (missing-property messages, self-managed/SaaS), and the
  process-service phase behavior.
- **Spring Boot** `Camunda8DeploymentAndStartIT` (real Camunda 8 via Testcontainers,
  image `camunda/zeebe:8.8.31`, standalone broker without Elasticsearch): boots the
  application (deploying the BPMN to the cluster on startup) and drives the full two-phase
  start through `ProcessService#startWorkflow` inside a JPA transaction with the gruelbox
  outbox. It asserts that the process instance appears only **after** the transaction
  commits, carrying the aggregate's ID as the `id` variable (named after the test
  aggregate's ID property; observed by a raw Camunda 8 job worker on
  the service task), and **never** after a rollback (the outbox entry is gone and no job
  is ever activated). Skipped automatically when Docker is unavailable
  (`@Testcontainers(disabledWithoutDocker = true)`).
- **Spring Boot / Quarkus discovery tests:** the adapter is discovered and the deployment
  service, process service and client-factory registry beans are created (no cluster
  needed). On Quarkus this is the extent of the coverage: the Quarkus platform integration
  does not yet run the deployment pipeline on startup (its own later story), so deployment
  and start are covered against a real cluster only on Spring Boot.

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

## Test coverage

An aggregated JaCoCo report over all modules is generated by `mvn install verify`
into `test-coverage-report/report`. Baseline recorded with the hardening story
(2026-07-29): **79.9% line coverage**. The feature stories' definition of done
requires >90% - gaps are filled by the stories touching the respective code.
