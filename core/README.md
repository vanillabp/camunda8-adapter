# Camunda 8 adapter - core

Contributor documentation for the platform-neutral core of the VanillaBP Camunda 8
adapter. User-facing documentation lives in
[this adapter's wiki](https://github.com/camunda-community-hub/vanillabp-camunda8-adapter/wiki); the
repository-root `README.md` documents the repository for contributors.

The `core` module is **plain Java** - no Spring or Quarkus dependencies. It holds the
adapter SPI implementations and all Camunda 8 client logic. The platform modules
(`spring-boot`, `quarkus`) only construct and register these core objects (read
configuration, create beans, run the bean lifecycle).

## Client construction

- `Camunda8AdapterConfiguration` - resolved, platform-neutral connection configuration of
  one adapter instance (mode, REST/gRPC address, SaaS credentials, tenant). Populated by
  the platform modules from `vanillabp.adapters.<adapter-id>.*` (see the root `README.md`).
  Validated lazily; `validate(adapterId)` throws naming the exact missing property.
- `Camunda8ClientFactory` - owns the single `CamundaClient` of one adapter instance, built
  **eagerly at startup** (for completely configured instances) and closed on `close()`. Building never contacts the cluster
  (that happens on the first command). `newClientBuilder()` is used for self-managed,
  `newCloudClientBuilder()` for SaaS.
- `Camunda8ClientFactoryRegistry` - map adapter ID &rarr; factory, registered as a managed
  bean so all clients are closed on shutdown.

## What a worker sends back to the cluster

Four classes share the way back, so the rules live in one place rather than in four
handlers:

- `Camunda8Errors` classifies a failure. `permanentFailure` answers the outbox (story 73),
  `repeatableJobCommandFailure` answers a job command and adds the one case which is
  permanent only there, a job which is gone. `incidentMessage` builds the text an operator
  reads in Operate, with the exception's type in front of its message.
- `Camunda8CommandRetry` repeats a rejected command (story 91). It is bounded by the job's
  remaining lock (`ActivatedJob#getDeadline()`), by five attempts and by the shutdown, and
  its waits are the client's own activation backoff numbers. Nothing about the outcome
  changes once the bound is reached: the original failure is rethrown and the caller does
  what it did before.
- `Camunda8RetryBackoffResolver` resolves `retry-backoff` over the four configuration
  levels, per command rather than per worker. It travels with every fail command which
  leaves the job retries.
- `Camunda8Drain` decides whether a failure belongs to the shutdown (story 90), in which
  case no command is sent at all and the job is left to its lock.

## Adapter SPI implementations

- `Camunda8DeploymentService implements AdapterDeploymentService<BpmnModelInstance,
  Camunda8ProcessingContext>` - one instance per configured adapter ID (not per type).
  `readBpmn` parses with `io.camunda.zeebe.model.bpmn.Bpmn.readModelFromStream` and returns
  one entry per executable `<process>`; `prepareBpmn` accumulates the deployable resources
  (deduplicated per filename) into the context; `deployResources` sends one
  `DeployResourceCommand` per workflow module (configured tenant or default). `wireBpmn`
  validates the task wiring against the core's `WorkflowTaskInvoker` and injects what V1
  injected (user-task listeners, subscription correlation keys);
  `startWorkflowProcessing` opens one polling job worker per task definition plus one per
  user-task listener type, `stopWorkflowProcessing` closes them again.
- `Camunda8ProcessService<A> implements MigratableProcessService<A>` -
  `needsTwoPhaseCommitForStartingWorkflows()` returns `true`. Phase one validates only
  (resolve aggregate ID, verify client configured; no cluster call). Phase two creates the
  instance via `createProcessInstance(bpmnProcessId, variables, aggregateId)` (latest
  version), carrying the values the aggregate shares plus the technical variable named
  after the aggregate's ID property.
- `Camunda8ProcessingContext` - the adapter-specific processing context threaded through
  the deployment pipeline: workflow-module ID, deployable resources (per filename) and the
  discovered BPMN process IDs.

The adapter SPI is served completely - deployment, workflow start (two-phase), task
processing, user tasks, message correlation, aggregate sync and the viewer/history API.
The ONE deliberate gap is `cancelUserTask`, which Camunda 8.8 offers no command for: it
throws a guiding `UnsupportedOperationException` instead of pretending to work (expected
to arrive with the 8.10 listener support). The election awareness probes are
implemented: `awarenessOfTask` (job-timeout refresh), `awarenessOfUserTask` (empty
user-task update), `awarenessOfWorkflow` (instance search; optimistic ACTIVE without
secondary storage) and the stricter `awarenessOfWorkflowForRedispatch` (instance
search without state filter; honest UNKNOWN without secondary storage - never
optimistic, see the root README's idempotency section).

## BPMN model type

The BPMN model type is `io.camunda.zeebe.model.bpmn.BpmnModelInstance`, shipped in the
artifact `io.camunda:zeebe-bpmn-model`. Against the resolved Camunda 8 client
`io.camunda:camunda-client-java:8.8.31`, `zeebe-bpmn-model:8.8.31` is a transitive
dependency and the class/artifact are **unchanged** from Camunda 7-era Zeebe (no rename
in 8.8). `readBpmn` parses with it, `prepareBpmn`/`wireBpmn` modify the model (listener
and subscription injection) and `deployResources` serializes it back.

## Client artifact

The core depends on `io.camunda:camunda-client-java` (which brings `zeebe-bpmn-model`
transitively). The plain Java client is used deliberately instead of Camunda's Spring
SDK - see the root `README.md`.

## Platform version guard

`META-INF/vanillabp/adapter-camunda8.properties` carries this adapter's version and the
version of the VanillaBP platform integration it was built against
(`platform.version=${adapter-platform.version}`, filled by resource filtering configured
in `pom.xml`). The `Camunda8DeploymentService` constructor passes it to
`AdapterPlatformVersion.requireCompatiblePlatform(...)`, which aborts the startup with a
guiding message if the platform integration on the classpath is older — Maven does not
report that as a conflict, because a version managed by the application always wins over
the version required transitively by this adapter, even as a downgrade. See
`migration-adapter/README.md`, section "Adapter/platform version guard".
