![Header](./readme/vanillabp-headline.png)

# VanillaBP adapter for Camunda 8

[![](https://img.shields.io/badge/Lifecycle-Incubating-blue)](https://github.com/Camunda-Community-Hub/community/blob/main/extension-lifecycle.md#incubating-)
[![Apache License V.2](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE)

This is the [VanillaBP](https://www.vanillabp.io) adapter for
[Camunda 8](https://camunda.com/platform/) (Version 2). It lets a VanillaBP business
application run its workflows on a Camunda 8 cluster without the business code depending
on the Camunda API.

Developers who want to **use** this adapter should refer to the
[Wiki](https://github.com/camunda-community-hub/vanillabp-camunda8-adapter/wiki); the VanillaBP concepts it builds
on are documented in the [VanillaBP Wiki](https://github.com/vanillabp/adapter-platform-integration/wiki). This
`README.md` is aimed at contributors.

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
EAGERLY at startup for every completely configured adapter instance. The adapter-id
set always comes from the platform's core properties (ids of type `camunda8`); the
overlay maps are per-known-id lookups only.

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

**Boot behavior (validated at startup):** Every configured adapter instance's
connection configuration is validated AT STARTUP:

- entirely unconfigured → the application still boots; a guiding WARN names the
  adapter id and the exact keys to add (e.g. `vanillabp.adapters.myengine.rest-address`);
- inconsistent (e.g. `mode: saas` without `cluster-id`) → the boot FAILS naming the
  missing keys - unless the adapter is nowhere first in any prioritized-adapters list
  and its `deployment-failure` policy is `warn` (then the application boots DEGRADED
  with a warning; the migration scenario's old BPMS must not block the boot);
- fully configured → the client is built eagerly (building never contacts the
  cluster).

Messages name property KEYS only - values, especially credentials like
`client-secret`, are never echoed. Using an unconfigured adapter at runtime keeps a
guiding failure message as backstop.

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

The phase-two outbox has at-least-once semantics. The duplicate-start window is
**minimized** by several layers: the outbox entry's unique idempotency key (one entry
per workflow module, BPMN process and aggregate), the DONE-retention of dispatched
entries, and — since the election story — a probe before every RE-dispatched start
(`awarenessOfWorkflowForRedispatch`: an entry dispatched before checks whether the
workflow already exists via the process-instance search; if so, the entry is consumed
without a second `CreateProcessInstance`). A **residual window remains and is
accepted** as an eventual-consistency property: after a hard crash between a
successful `CreateProcessInstance` and recording the dispatch, the retry's probe may
not see the instance yet (query-API lag), and without secondary storage the probe
cannot run at all (it then answers honestly "unknown" and the idempotent start
proceeds — deliberately NOT the optimistic ACTIVE of the election probe, which would
skip and thereby LOSE workflows). Do not build on exactly-once semantics.

### Task processing (story 21c)

`@WorkflowTask` methods are served by **polling job workers**: at
`startWorkflowProcessing` the adapter opens ONE worker per distinct task definition
(the `zeebe:taskDefinition` type) found in the workflow module's BPMN files. Task
wiring is validated during `wireBpmn` (every BPMN task needs a matching
`@WorkflowTask` method - service, send, business-rule and script tasks are
scanned), and unwired `@WorkflowTask` methods are reported at the end of
`deployResources` (per module; classes whose processes are served by another
adapter are not reported - the migration policy).

Execution model per delivered job (at-least-once ordering):

1. open a NEW local transaction, load the aggregate by the ID variable
   (named after `AggregatePersistenceAware.getAggregateIdName()`),
2. invoke the `@WorkflowTask` method through the core's `WorkflowTaskInvoker`,
3. save the aggregate and COMMIT,
4. only then report the outcome to the cluster:
   - normal return → `CompleteJob`; a `NOT_FOUND` answer is tolerated with a WARN
     (the job was already completed by an earlier delivery - the documented
     at-least-once residual, the handler must be idempotent);
   - `TaskException` → `ThrowError` with the error code (BPMN error; the
     aggregate changes stay COMMITTED - the V1 contract);
   - any other exception → the local transaction is rolled back and the job is
     failed with decremented retries (Camunda 8 redelivers).

**Asynchronous tasks (`@TaskId`) and dormancy:** a handler receiving the task ID
completes the task later via `ProcessService#completeTask`. Such a
job must not be redelivered while it waits, so after the commit the adapter extends
the job's lock ONCE via `UpdateJobTimeout` to the `async-task-timeout` (default 14
days). The worker's own job timeout stays SHORT - it is the crash-recovery horizon
for synchronous handlers.

Task-scoped configuration (see the four-level pattern of the VanillaBP
configuration model - the most specific configured value wins):

```yaml
vanillabp:
  adapters:
    myengine:
      type: camunda8
      job-timeout: PT5M           # adapter level (default PT5M)
      async-task-timeout: P14D    # adapter level only (default P14D)
  workflow-modules:
    loan-approval:
      adapters:
        myengine:
          job-timeout: PT2M       # per workflow module
      workflows:
        LoanApproval:
          adapters:
            myengine:
              job-timeout: PT1M   # per workflow (BPMN process ID)
          tasks:
            assessRisk:
              adapters:
                myengine:
                  job-timeout: PT10S  # per task (task definition)
```

Limitation: Camunda 8 workers subscribe by job type only. If the SAME task
definition appears with DIFFERENT resolved job timeouts within one module, the
startup fails with a guiding message (one worker per job type - give the
definitions distinct names or align the timeouts).

**Completing/canceling async tasks (`ProcessService#completeTask`/`#cancelTask`,
story 22):** the adapter locates the job by its key (the `@TaskId` value). The
awareness probe and the phase-one check are the same NON-ADVANCING command -
`UpdateJobTimeout` to the `async-task-timeout` (which conveniently refreshes the
dormant job's lock): success means the job exists, `NOT_FOUND` maps to
"unknown", a connection failure to "BPMS unavailable" (never falls back to
another adapter). The phase-one check runs as a PRE-COMMIT transaction
synchronization - as late as possible, minimizing the window between check and
the phase-two dispatch (fewer stale outbox entries). Phase two (after the
commit, through the outbox) sends `CompleteJob` respectively `ThrowError` (the
BPMN error code routes boundary events); a `NOT_FOUND` answer is tolerated with
a WARN (at-least-once residual). Camunda 8 cannot deliver `@TaskEvent CANCELED`
- Zeebe does not notify workers about canceled jobs.

**User tasks (story 24):** Camunda-managed user tasks (`zeebe:userTask`) with an
EXTERNAL form reference - the reference IS the task definition (V1 convention).
During `wireBpmn` the adapter adds the V1-COMPATIBLE lifecycle task listeners to
the BPMN model: per user task `creating` (→ `@TaskEvent CREATED`) and `canceling`
(→ CANCELED), type `io.vanillabp.userTask:<external form reference>`,
`retries="0"`; the VanillaBP `creating` listener is inserted as the FIRST and the
`canceling` listener as the LAST listener (modeller-defined ones stay in
between). Upgrading a V1 application produces a byte-identical BPMN - no new
process version. Listener jobs are consumed like normal jobs (one worker per
listener job type), ALWAYS completed, and deliver the USER-TASK KEY as `@TaskId`;
a failing notification fails the listener job (retries 0 → incident). The
notification handler is OPTIONAL. `completeUserTask` sends `CompleteUserTask` by
the user-task key after the commit (phase one re-checks existence pre-commit via
an empty `UpdateUserTask` carrying only an audit `action` - also the awareness
probe; note: modeller-defined `updating` listeners would fire on probes).
**`cancelUserTask` is NOT supported on Camunda 8.8:** the engine offers no
command to cancel a Camunda-managed user task by BPMN error (ThrowError is
job-based) and V1's marker-variable workaround is broken by V1's own admission -
a guiding error explains it; expected to arrive with the Camunda 8.10 listener
support (see the prepared follow-up prompt).

**Message correlation (story 23):** `correlateMessage` publishes AFTER the commit
(outbox) with `correlationKey = correlationId ?? aggregate ID` and NO variables
(payload doctrine). During `wireBpmn` the adapter INJECTS the `zeebe:subscription`
correlation-key expression `=<aggregate-ID variable>` into message subscriptions
lacking one - catch events correlate via the aggregate ID without manual model
tweaks (existing expressions stay untouched; V1 models deploy byte-identically).
WITH a correlation id the outbox idempotency key doubles as the Zeebe `messageId`,
so redelivered dispatches are rejected engine-side WITHIN THE MESSAGE TTL (engine
default; a redelivery after the TTL could correlate again - the documented
uniqueness window). WITHOUT one, deduplication is deliberately absent.
`startWorkflowByMessage` publishes with an empty correlation key, the start's
idempotency key as `messageId` and ONLY the aggregate-ID variable.
`awarenessOfWorkflow` uses the process-instance search (query API): without
secondary storage the adapter answers OPTIMISTICALLY (one-time guiding WARN) -
fine for single-BPMS setups, configure secondary storage for migration scenarios.

### Viewing workflows (story 26)

`ProcessService#getProcessDefinitions`, `#getBpmnXml` and `#getWorkflowHistory` are served
from two sources:

1. **What this application version deployed** - VanillaBP's deployment pipeline reads every
   workflow module's BPMN at each boot, so the adapter keeps those models (per adapter id,
   with the process definition key and version the CLUSTER assigned at deployment) and serves
   definitions and BPMN XML from them: no cluster round trip, no consistency lag, and it works
   on clusters WITHOUT secondary storage.
2. **The cluster's query API** (secondary storage) for everything instance-related: which
   version a running workflow actually uses, the element history, and definitions deployed by
   PREVIOUS application versions (a long-running workflow surviving a redeployment).

**Consistency caveats - by design, never errors:**

- Without secondary storage the element history is reported as `null` (the SPI's "not
  supported by the underlying BPMS") and the definitions of the currently deployed version
  are reported; a guiding WARN naming the reason is logged once per adapter id.
- The query API is eventually consistent: a workflow started moments ago may not be visible
  yet. The adapter reports what is visible - a viewer polling shortly after sees the data.
- Definitions of previous application versions are only resolvable through the cluster;
  without the query API `getBpmnXml` answers with the core's guiding
  `ProcessDefinitionNotFoundException`.

The adapter-native process definition id is the **process definition key**, the history
context of a call activity its called **process instance key**, and the XML returned is the
model AS DEPLOYED (VanillaBP's wiring modifications included).

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
  service (one per configured adapter id), process service and client-factory registry
  beans are created (no cluster needed).
- **Quarkus deployment-pipeline test** (`Camunda8DeploymentPipelineTest`, no Docker):
  since story 26b the Quarkus platform integration runs the deployment pipeline at
  boot. The test provides a BPMN below the configured `resources-location` and a REST
  address pointing to a closed port: the pipeline reads/parses the BPMN and attempts
  the deployment, whose connection failure aborts the boot (the adapter is
  first-priority) - proving the pipeline mechanics without a cluster. A real-cluster
  deployment on Quarkus is not additionally tested: the deployment logic is shared
  `core` code, covered against a real cluster by the Spring Boot
  `Camunda8DeploymentAndStartIT` above.

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

## Noteworthy & Contributors

[VanillaBP](https://www.github.com/vanillabp/spi-for-java) was developed by [Phactum](https://www.phactum.at) with the
intention of giving back to the community as it has benefited the community in the past.

![Phactum](./readme/phactum.png)

## License

Copyright 2026 Phactum Softwareentwicklung GmbH

Licensed under the Apache License, Version 2.0
