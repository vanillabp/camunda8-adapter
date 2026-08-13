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

**Feature-complete against the VanillaBP 2 adapter SPI.** The adapter connects to a
Camunda 8 cluster, deploys each workflow module's BPMN on startup, starts workflows
through the two-phase outbox (see [Behavior](#behavior)), executes `@WorkflowTask`
methods through polling job workers, completes and cancels asynchronous tasks, serves
user tasks incl. their lifecycle notifications, correlates messages, pushes the
aggregate's shared attributes as process variables and answers the viewer/history API.

What this adapter cannot deliver is listed under [Known deviations](#known-deviations),
`cancelUserTask` being the most prominent one. Everything that cannot be answered honestly
(e.g. workflow awareness on a cluster without secondary storage) is documented as such
rather than guessed.

## Documentation and supported platforms

This adapter runs on both platforms VanillaBP supports:

1. **Spring Boot**<br>[![Coverage](https://img.shields.io/badge/dynamic/regex?url=https%3A%2F%2Fvanillabp.github.io%2Fcamunda8-adapter%2Fspring-boot-report%2Findex.html&search=Total.*%3F.([0-9]%2B)[^0-9]*%3F%25&replace=%241%25&flags=m&label=Coverage&color=green&cacheSeconds=60)](https://vanillabp.github.io/camunda8-adapter/spring-boot-report)
2. **Quarkus**<br>[![Coverage](https://img.shields.io/badge/dynamic/regex?url=https%3A%2F%2Fvanillabp.github.io%2Fcamunda8-adapter%2Fquarkus-report%2Findex.html&search=Total.*%3F.([0-9]%2B)[^0-9]*%3F%25&replace=%241%25&flags=m&label=Coverage&color=green&cacheSeconds=60)](https://vanillabp.github.io/camunda8-adapter/quarkus-report)

Coverage is measured separately per platform - a platform's tests never cover the other
platform's code. Click a badge to open the respective report.

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
  a single `DeployResourceCommand` per module. Which scope they land in is decided by the
  name-clash-avoidance mode, see
  [Keeping workflow modules apart](#keeping-workflow-modules-apart).
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

### Keeping workflow modules apart

The [name-clash-avoidance mode](https://github.com/vanillabp/adapter-platform-integration/wiki/Workflow-modules#how-name-clashes-are-avoided)
decides where a workflow module's models land. `by-adapter` deploys into a multi-tenancy
tenant named after the module (`tenant-id` overrides the name) and the job workers
subscribe for that tenant; `use-prefix` deploys into the default tenant with prefixed
identifiers instead, process ids, message names, error codes, signal and escalation names,
JOB TYPES and the user-task form reference, the latter two additionally scoped by their
BPMN process; `none`, the default, scopes nothing.

Prefixing is what makes tenants avoidable, which matters because Camunda licenses per
tenant, and it is transparent: BPMN, business code and configuration keep the plain
identifiers while the adapter translates at every boundary. The default is `none` rather
than `by-adapter` because a cluster started from the stock image has multi-tenancy switched
off and would reject a tenant id, so an application configuring nothing has to boot and
deploy. While `none` applies, a WARN per workflow module names the alternatives until
`accept-unscoped-identifiers` acknowledges that the identifiers are unique.

Where `by-adapter` applies, the adapter looks the tenant up in the cluster BEFORE deploying,
so the two ways this can go wrong are named as VanillaBP properties instead of as the
engine's rejection: multi-tenancy switched off (the deploy command would answer `Failed with
code 400 ... but multi-tenancy is disabled`, true but naming no property to change) and a
tenant which does not exist. Only an answer of the cluster counts; an unreachable cluster is
left to the deployment, which runs into it right after and reports it as the connection
problem it is.

### Sharing the workflow aggregate

The cluster can only evaluate what it was given, so the default of this adapter is that
everything is shared unless `@NoSyncWithBPMS` excludes it. The shared attributes travel at
every sync point: starting a workflow (also by message), completing the job at the end of a
`@WorkflowTask` method (a `TaskException` becoming a BPMN error included), completing or
canceling an asynchronous task, completing a user task and correlating a message.

The push at the end of a `@WorkflowTask` is what makes a gateway directly behind a service
task work. The values are read AFTER the method's local transaction committed, in an own
transaction, which keeps the at-least-once order of the worker untouched; if that read
fails, the job is still completed, with the aggregate-ID variable only and a warning naming
the workflow. User-task lifecycle listener jobs push nothing, because they gate a transition
of a user task which stays in the cluster: after `creating` nothing downstream is evaluated
yet, and `canceling` means the task is being removed.

`aggregateChanged(aggregate)` sends `SetVariables` for the process instance,
`aggregateChanged(aggregate, taskId)` sends it with `local(true)` for the element instance
of the scope the task RUNS in, never for the task's own element instance: in Camunda 8 every
element instance is a variable scope, and the one belonging to a task disappears with the
task, so nothing would ever read what was written there. Finding that scope takes a few
queries, since the API reports the children of a scope but never the parent of one, so the
adapter walks down from the process instance until the task's element instance shows up. The
operation carries no idempotency key at all, because the values are read when the push is
dispatched and a retry is therefore harmless.

Independent of the annotations the workflow aggregate's ID is written as a process variable
named after the aggregate's ID attribute, always as a string, because Camunda 8 has no
business key. A cluster stores variables as JSON and compares against that JSON, so an
instance search has to quote the value (`{"name":"id","value":"\"4711\""}`); an unquoted
filter finds nothing, which is what `Camunda8VariableFilters` encodes for the process
service and the viewer alike.

### Signals

`sendSignal(name)` broadcasts through the cluster's `BroadcastSignal` command after the
local transaction was committed, riding an outbox entry, so a rolled-back transaction never
reaches the cluster. The command carries no payload, and there is nothing to deduplicate a
signal by (unlike a message, which VanillaBP can give a message id), so a redelivered outbox
entry broadcasts a second time.

### Workflows the cluster starts itself, and the end of a workflow

A process with a timer or signal start event runs without anybody calling `startWorkflow`.
While deploying, the adapter adds an execution listener to that start event with event type
`end`: the cluster rejects `start` listeners on start events, and an `end` listener still
gates the transition, so nothing of the process runs before the listener job is completed.
The listener job builds the workflow aggregate and completes with the aggregate-ID variable
plus the shared values, the same variables a start through `ProcessService` would write. The
aggregate's ID is the PROCESS INSTANCE KEY rather than the timer's scheduled time, which the
cluster does not report to the listener; the instance key survives a retried listener job, so
a redelivery finds the aggregate instead of building a second one.

Where a workflow service declares a `@WorkflowEnded` method, the adapter adds an `end`
execution listener to the PROCESS element and opens a worker for it. The job is activated
after the last element completed, and its completion lets the instance disappear.

### Versions of a process

The cluster counts a process definition's version upwards per BPMN process id, and every
activated job carries the version of the definition its instance runs on, which the adapter
reports with every task, user-task listener job, BPMS-initiated start and workflow end. A
version made of numbers therefore costs no query.

A boundary naming the model's `zeebe:versionTag` is a different matter, since a job never
carries the tag: the adapter asks the query API which version carries which tag
(`newProcessDefinitionSearchRequest`). The queries are few by design, one per process while
the application starts (after the deployment) and one for a version this application never
deployed itself, which is what a rolling deployment produces while another node is already
ahead. The version of the model deployed by this very start needs no query at all: the deploy
command reports it and the tag is read from the model.

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

## Known deviations

What this adapter does not deliver, mirrored in one sentence each on the wiki's
[Deviations](https://github.com/camunda-community-hub/vanillabp-camunda8-adapter/wiki/Deviations)
page. The two-phase start and the at-least-once dispatch are not among them: that is what a
remote BPMS looks like in VanillaBP, see [Behavior](#behavior).

### What needs secondary storage

Finding a workflow by its aggregate's ID is a query-API search
(`newProcessInstanceSearchRequest` filtered by the aggregate-ID variable), and the query API
exists only on a cluster WITH secondary storage. Four capabilities depend on it:

1. `awarenessOfWorkflow`, the BPMS-election probe, which also carries `completeTask`,
   `cancelTask`, the user-task operations, message correlation, `aggregateChanged` and the
   viewer. Without the query API the adapter answers OPTIMISTICALLY with a one-time guiding
   WARN, which is honest for a single-BPMS setup and unsafe in a migration setup, where a
   wrong "yes" routes the operation to the wrong BPMS.
2. `aggregateChanged`, which needs the process-instance respectively element-instance key
   `SetVariables` addresses. It fails with a guiding message instead of pretending the push
   happened, and a task completion remains the way to push the shared values.
3. Version boundaries naming a `zeebe:versionTag`, since resolving a tag to a version is a
   definition search. The adapter says so once, and boundaries made of numbers keep working.
4. The viewer's instance-related answers: which version a running workflow uses, the element
   history and the definitions of previous application versions. Without them the adapter
   reports what THIS application version deployed plus a `null` element history.

The redispatch probe of a start (`awarenessOfWorkflowForRedispatch`) is the deliberate
exception: it answers "unknown" rather than optimistically, because an optimistic answer
would skip the start and thereby LOSE the workflow, see
[Idempotency limitation](#idempotency-limitation).

### Eventual consistency of the query API

The query API lags behind the engine, which everything in the list above inherits. The viewer
tolerates it by design, since a viewer polling shortly after sees the data. The awareness
probe does not: a workflow started moments ago is not searchable yet, the probe reports
`UNKNOWN_TO_BPMS` and the core raises `WorkflowNotFoundException` with causes that all do not
apply. Starting a workflow and correlating a message to it in quick succession is the
everyday case. Story 54 puts a retry into the core with the adapter naming the time window,
and fills VanillaBP's workflow-adapter cache where the answer is known for certain.

### Cancel user task

Camunda 8.8 offers no command to cancel a Camunda-managed user task by BPMN error: *throw
error* is job-based, and a user task is not a job. Version 1's marker-variable workaround is
broken by Version 1's own admission, so `cancelUserTask` throws a guiding error rather than
pretending to work. Expected to arrive with the Camunda 8.10 task-listener capabilities.

### Task cancellation is not reported

`@TaskEvent CANCELED` cannot be delivered for service tasks, because Zeebe does not notify
workers about canceled jobs, so a handler subscribing to lifecycle events never learns that
an open asynchronous task's activity was canceled. Camunda 8.10 announces the event type
`canceling`, which the prepared follow-up will verify before anything is reported.

### The end of a workflow

The cluster runs end listeners of COMPLETED instances only, so `@WorkflowEnded` methods see
the kind `COMPLETED` and never `TERMINATED`: a cancelled instance is removed without running
them. This waits on the same `canceling` event type as above. Independently of that the
notification names no end event, because the listener sits on the process element rather than
on an end event, which is structural rather than a gap to close.

### Conditional events

Camunda 8 has no conditional start, catch or boundary events, and a model carrying one is
rejected by the cluster while deploying. `aggregateChanged` is still useful, since the cluster
evaluates a gateway behind the current element against the values it holds, but there is
nothing which reacts to a variable change on its own.

### Message deduplication lasts for the message TTL

A correlation carrying a correlation id deduplicates engine-side, because the outbox
idempotency key travels as the Zeebe message id, and the engine remembers a message id for
the message TTL only. A redelivery after the TTL could correlate a second time. Without a
correlation id there is no deduplication at all, on purpose: the same message may legitimately
arrive several times over a workflow's lifetime.

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

`mvn install verify` builds one aggregated JaCoCo report per platform:

1. **Spring Boot** (core + Spring Boot integration) - into `test-coverage-report/spring-boot/report`
2. **Quarkus** (core + Quarkus extension) - into `test-coverage-report/quarkus/report`

Both are published to GitHub Pages by the *Publish to GitHub Packages* workflow on every push to
the default branch. Click the [platform's badge](#documentation-and-supported-platforms) to open
the respective report.

Baseline recorded with the hardening story (2026-07-29): **79.9% line coverage**. The feature
stories' definition of done requires >90% - gaps are filled by the stories touching the respective
code.

## Noteworthy & Contributors

[VanillaBP](https://www.github.com/vanillabp/spi-for-java) was developed by [Phactum](https://www.phactum.at) with the
intention of giving back to the community as it has benefited the community in the past.

![Phactum](./readme/phactum.png)

## License

Copyright 2026 Phactum Softwareentwicklung GmbH

Licensed under the Apache License, Version 2.0
