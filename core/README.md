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
  bean so all clients are closed on shutdown. It is also the only place which sees every
  configured id at once, so it answers which ids address the SAME cluster:
  keys are unique per cluster, and where two ids share one, the awareness probes have to
  ask which scope a key belongs to before they claim it. The factory knows which workflow modules
  have workers open and closes the ones which never stopped themselves before it closes
  the client, so the order holds on every shutdown path and not only on the
  one each platform's lifecycle takes.

## What a worker sends back to the cluster

Four classes share the way back, so the rules live in one place rather than in four
handlers:

- `Camunda8Errors` classifies a failure. `permanentFailure` answers the outbox,
  `repeatableJobCommandFailure` answers a job command and adds the one case which is
  permanent only there, a job which is gone. `incidentMessage` builds the text an operator
  reads in Operate, with the exception's type in front of its message.
- `Camunda8CommandRetry` repeats a rejected command. It is bounded by the job's
  remaining lock (`ActivatedJob#getDeadline()`), by five attempts and by the shutdown, and
  its waits are the client's own activation backoff numbers. Nothing about the outcome
  changes once the bound is reached: the original failure is rethrown and the caller does
  what it did before.
- `Camunda8RetryBackoffResolver` resolves `retry-backoff` over the four configuration
  levels, per command rather than per worker. It travels with every fail command which
  leaves the job retries.
- `Camunda8Drain` decides whether a failure belongs to the shutdown, in which
  case no command is sent at all and the job is left to its lock. It also holds what a
  shutdown waits for and the line it writes about it: the handlers of the
  module, and the workers reporting themselves closed, because an activation request which
  is parked at the cluster when the client is closed stays parked and swallows the first
  job of the next application.

## What a worker asks the cluster for

`Camunda8FetchVariables` holds both halves of it: the list a worker names, and the two
messages a delivery writes when it is asked for a variable outside that list.

The derivation runs in `Camunda8DeploymentService#fetchVariablesOf`, once per worker while
`startWorkflowProcessing` opens them. Its input is a `ServedElement` per BPMN element the
worker serves, which is why the four worker kinds share one method: a task worker serves
the tasks of a job type across the module's processes, a user-task listener worker the
user tasks of its listener job type, and the workflow-end worker one process. Three
sources feed it, and all three are the core or this adapter's own bookkeeping:
`resolveWorkflowAggregateIdName` per BPMN process, the multi-instance registry filled during
`wireBpmn` and keyed by the process id the CLUSTER knows plus the element id, and
`taskParameterNames` per served task definition.

That last one replaced a scan of the model. The scan collected the four constructs a Camunda
8 model declares a variable with - the targets of `zeebe:ioMapping`, the result variable of
an inline script, the result variable of a called decision, the output collection of a
multi-instance element - because a `@TaskParam` might read one of them and the model looked
like the only place this adapter could see such a name. The core had the names all along: it reads
them off the annotations while it builds the parameter binders. Keeping both would have left
two sources for one answer, and the model was the weaker of them in both directions, so
`declaredVariablesOf` is gone.

Three answers are deliberate. The list is a sorted `TreeSet`, because job streaming
compares it. Where any level of the configuration says `all`, the whole worker asks for
everything, without the guiding failure two conflicting job timeouts would produce -
fetching more than derived is never wrong. And where the core cannot name the aggregate-id
variable of a process, the worker asks for everything too, rather than for a list which
may be missing exactly what its handler reads.

The handlers carry the `Selection` because they need it in a message, not to decide
anything: `Camunda8JobHandler` and `Camunda8UserTaskListenerHandler` name it when the
aggregate-id variable is absent, and their invocation contexts throw when
`getTaskParameter` is asked for a name outside it. That throw is practically
unreachable - a statically named `@TaskParam` is in the list by construction - and it stays
for the name a handler computes at runtime, which the scanner cannot see. Its message says
so, because the first thing a reader checks is the annotation.

## What an operator gets to see

The core measures every delivery on every BPMS. This adapter adds what only makes sense
here, and the whole of it hangs on two seams.

`applyWorkerOptions` is the one place all four kinds of worker pass through, so the
client's own metrics hook is installed there and nowhere else. The hook itself is the
client's; what is ours are the meter names, which is why `JobWorkerMetrics.micrometer()`
is deliberately NOT used: it would publish `camunda.job.invocations` and friends next to
`vanillabp.*`, and a reader should not have to learn two naming schemes for one dashboard.

`Camunda8Metrics` is plain Java with a no-op `NONE`, `MicrometerCamunda8Metrics`
implements it plus `MeterBinder`, and Micrometer stays optional exactly as in the platform
integration. The execution slots come from the virtual-thread executor, which
is the only place holding the bound; in the platform-thread mode the client owns its pool
and reports nothing about it, so those gauges are absent instead of guessed.

**Reading a metric must not cost anything.** The platform's rule applies here too: a gauge
is read on every collection, Prometheus collects every fifteen seconds by default, a
dashboard collects alongside it, and every instance answers each of them - so a gauge which
asks a database or a cluster turns watching the system into load on it. None of this
adapter's gauges do. `execution.slots.configured` reads a record field,
`execution.slots.in.use` and `jobs.waiting` read the permits and the wait queue of the
semaphore in `Camunda8VirtualThreadExecutor`, and the two job counters are incremented by
the client rather than polled. They are therefore exact, and holding them would only make
them stale.

A gauge added here later which DOES have to ask - the cluster, a query API, anything remote
- goes through `CachedGaugeValue` of the adapter SPI
(`io.vanillabp.integration.adapter.spi.observability`), which holds one measurement for the
platform's `vanillabp.metrics.gauge-cache`. That class lives in the SPI precisely so an
adapter can keep the same promise; see `migration-adapter/README.md` for why it is built the
way it is.

`checkHealth()` asks for the topology. Two decisions are worth remembering:

- The timeout is a property of its own (`health-timeout`, two seconds), not the client's
  `request-timeout`. Ten seconds is right for a command carrying work and wrong for a
  question a readiness probe asks with a one-second patience.
- It is set TWICE, on the request and around the waiting. The client's own timeout stops
  the request, ours stops the waiting; without the first one a cluster which never answers
  would leave the request running long after the endpoint gave up on it.

An adapter whose connection is not configured yet answers UNKNOWN. That is the health side
of the same rule the startup validation follows: an application which booted with a guiding
warning has not failed.

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
- `Camunda8ProcessService<A> implements MigratableProcessService<A>` - phase one validates
  only (resolve aggregate ID, verify client configured; no cluster call). Phase two creates the
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
