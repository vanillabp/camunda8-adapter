# Upgrade notes

Documents changes that were necessary when upgrading major dependency versions, or which an
application on this adapter has to act on, so the reasoning can be looked up later. The same
file exists for
[VanillaBP itself](https://github.com/vanillabp/adapter-platform-integration/blob/main/UPGRADE.md).

## The `retryBackoff` task header of a model is read again (2026-08-30)

Version 1 let a Camunda 8 model name the backoff of a single element in the task header
`retryBackoff`, which is how it filled the gap Camunda 8 leaves next to Camunda 7's
`camunda:failedJobRetryTimeCycle`. Version 2 read no task headers, so a model carrying one
lost it and nothing said so. The header is read again, and no model has to be touched for
that.

Two things are different from version 1. The header is read from the JOB rather than from
the model while deploying, so it also holds for process versions this application never
deployed and no redeployment is needed. And version 1 only looked at the header where the
element's `zeebe:taskDefinition` carried a `retries` attribute as well; that condition is
gone, because whoever models a backoff means it either way.

An application which already moved the value into the configuration has one thing to check.
Where it landed at the TASK level
(`vanillabp.workflow-modules.<m>.workflows.<w>.tasks.<t>.adapters.<id>.retry-backoff`),
nothing changes: that level still applies, and one line per element says so where the model
disagrees with it. Where it landed at the workflow, the workflow-module or the adapter
level while the header stayed in the model, the header applies again from now on. Both say
something about one single task, so the more specific one wins, and between two of the same
reach the one you can change without deploying a new process version does. So look at the
models you migrated: a header you meant to retire has to leave the model, not just the
configuration.

A header which is no ISO-8601 duration costs a warning naming the workflow module, the
BPMN process, the element and the value, and then the configured value applies. Version 1
answered such a typo with `Duration.ZERO`, which reads like "no backoff wanted" and hands
the job out again at once. The warning falls when a job of that element fails, once per
element and not once per job, and deliberately not while the application boots: a model
deployed long ago cannot be corrected by the boot which would complain about it.

The default is unchanged: without a header and without configuration a failed job is
handed out again after ten seconds, where version 1 sent no backoff at all.

## Deleting a process definition ends the reports about it (2026-08-30)

Nothing to configure, and one fewer thing to explain away in a startup log.

The check for old process versions asks the cluster which versions of a process it holds. Until
now it asked without naming a state, and Camunda 8 keeps a deleted process definition in its query
API rather than removing it: it is marked `DELETED` and still answered. So a definition an operator
had deleted kept its place in the list, its model was read, and the tasks this application no longer
serves were reported for it at every start - including the FATAL report where workflows were still
counted on it.

The searches now name the state `ACTIVE`. For an application which never deleted a definition
nothing changes at all. Where one was deleted, the reports about it stop with the next start, and
that is the point: deleting the definition is what the report asks for, so it has to work.

Suspended definitions are a different matter and do not exist here - Camunda 8 cannot suspend a
process definition or an instance.

## Two Camunda 8 adapter ids on one cluster are told apart by their scope (2026-08-21)

Nothing changes for an application with one Camunda 8 adapter. What changes is the
setup which migrates a workflow module from tenants to prefixed identifiers on the SAME
cluster, and there it changes correctness.

Until now the election asked every adapter "do you hold the workflow of aggregate X" and the
Camunda 8 adapter searched by the aggregate-ID process variable alone. On one cluster both
deployments carry that variable, so both answered yes and the first entry of
`vanillabp.prioritized-adapters` won every operation. Nothing failed visibly: a user task is
addressed by its key and keys are cluster-global, and the job types of the two scopes differ,
so the jobs went where they belonged. A correlated message did not: it was published under the
name and tenant of the WRONG adapter, where nobody subscribes, and the cluster dropped it once
`message-time-to-live` passed.

The probes now compare the tenant and the process definition id against what the adapter id
itself deployed. Three things follow for such a setup:

- **the cluster needs secondary storage.** A task key can only be mapped to its scope through
  the query API. Two ids on one cluster without it do not boot any more, with a message naming
  the ids which share the cluster. A single Camunda 8 adapter keeps working without it,
- **an election of a task costs one query-API read**, and only where a second id shares the
  cluster,
- **check your election cache.** `WorkflowLocator` remembers which adapter holds a workflow. An
  entry written by the old, wrong election survives this upgrade wherever the application
  supplies its own (clustered) `WorkflowAdapterCache`; the built-in in-memory cache is empty
  after the restart which brings the new version. Where such a cache is shared, clear it while
  upgrading.

Signals were never affected: `sendSignal` broadcasts to every deployed adapter, so each scope
gets its own.

## A restart waits a few seconds longer, and the application after it does not (2026-08-21)

No new property, and nothing to configure. What changes is how long a shutdown
takes and how quickly the next start gets its first job.

A worker asks the cluster for work with a long poll which waits at the cluster for up to
`request-timeout`, ten seconds by default. Closing the worker does not cancel that request,
and neither does closing the client. Measured against `camunda/camunda:8.9.16` with the
plain Camunda client: a job created while such a request is still parked is handed to it,
counts as activated and is answered by nobody, so the worker of the application which is
running by then sees it only once `job-timeout` expired. With seven seconds between the two
applications that was the full lock in all twenty runs; with twelve seconds, beyond the
request window, twenty milliseconds. It is the REST transport, which is the default: the
same scenario over gRPC, and over REST with `stream-enabled`, delivers in milliseconds.

The shutdown of a workflow module therefore waits for its workers to be released before the
client is closed, within the `shutdown-grace` it already had. In those runs the wait cost
8,2 to 8,5 seconds and turned a first job of 20 seconds into one of 30 milliseconds. Two
things follow for an application:

- an ordinary restart takes those seconds longer. `shutdown-grace` (default `PT20S`) bounds
  it, and it still sits below the shutdown budgets of Spring Boot and Kubernetes. `PT0S`
  waives the wait together with the handler drain,
- a process which is killed rather than asked to stop cannot pay it, so a workflow started
  within ten seconds of a `SIGKILL` may still wait for its lock. A shorter `job-timeout`
  bounds what that costs where restarts are frequent, and `stream-enabled: true` or
  `prefer-rest-over-grpc: false` avoids the case altogether.

The line the shutdown writes changed with it. It now says how many workers were closed and
whether the cluster released them, and it warns where one of them still holds its request
when the grace passes.

## The cluster reports itself to your metrics and your health endpoint (2026-08-20)

Additive with one new property, and nothing changes for an application which does nothing.

Where your application brings Micrometer, this adapter now bridges the client's own job
counters per worker into your registry (`vanillabp.camunda8.jobs.activated` and
`.handled`) and reports its execution slots as gauges
(`vanillabp.camunda8.execution.slots.configured`, `.in.use`, `vanillabp.camunda8.jobs.waiting`,
the last two in the virtual-thread mode). Without Micrometer nothing of it is loaded.

Where your platform has a health endpoint, the adapter contributes to the component
respectively readiness check `vanillabp`: it asks the cluster for its topology and reports
UP with the gateway version, DOWN with the reason, or UNKNOWN while the connection is not
configured yet. The new key `vanillabp.adapters.<id>.health-timeout` (default `PT2S`) is how
long it waits, and `PT0S` switches the check off.

What the platform measures for every BPMS came with the same story and is described in its
own [upgrade
notes](https://github.com/vanillabp/adapter-platform-integration/blob/main/UPGRADE.md).

## The artifact version names the Camunda 8 minor (2026-08-19)

Visible to every consumer, because the coordinates change.

The adapter is published once per Camunda 8 minor from now on, and the minor is part of the
version: `2.1.0-8.8`, `2.1.0-8.9`, and `2.1.0-8.10-alpha<n>` for the preview line built
against the alpha of the next minor. Which one you take is decided by your cluster, and the
table in the [wiki](https://github.com/camunda-community-hub/vanillabp-camunda8-adapter/wiki)
says which client and which tested cluster each line stands for.

The reason is that Camunda promises a client against clusters of its own version and newer,
and nothing about the other direction. The client a build was compiled against is therefore
the lowest cluster version that build accepts. Without lines, the day the adapter used
anything only an 8.10 cluster offers, every later bugfix would have been deliverable only
together with a cluster upgrade, and a Camunda 8 cluster upgrade costs more organizationally
than technically.

**What you have to do.** Add the suffix of your cluster's line to the adapter version in your
POM, and nothing else. The groupId, the artifactIds and the API stay as they are: the methods
are identical on every line, checked in the adapter's CI, so you never have to read a suffix
to find out what exists. Where your cluster cannot do something, the same method is there and
fails with a message naming your line.

**What Renovate should do.** Extend the preset the adapter ships and Renovate reads the suffix
as a compatibility value it never changes on its own, so no automatic update moves you to
another cluster minor:

```json
{
  "extends": ["github>vanillabp/camunda8-adapter//renovate/camunda8-lines.json"]
}
```

Without it, plain maven versioning would eventually offer you a boundary crossing, because
Maven sorts `2.2.0-8.8` above `2.1.0-8.9`.

**Until the first release** nothing changes: snapshots keep the coordinate `2.0.0-SNAPSHOT`
and are the current GA line.

**A new log line.** At startup each configured `camunda8` adapter id logs its release line and
the client it was built against, which is the lowest cluster version it accepts.

**A line lives** until the next minor goes GA, so two GA lines exist at a time plus the
preview. When 8.10 goes GA, 8.9 becomes the previous GA and 8.8 ends, although Camunda
supports 8.8 until April 2027. That is the project's policy rather than a technical limit.
