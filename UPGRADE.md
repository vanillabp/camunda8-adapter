# Upgrade notes

Documents changes that were necessary when upgrading major dependency versions, or which an
application on this adapter has to act on, so the reasoning can be looked up later. The same
file exists for
[VanillaBP itself](https://github.com/vanillabp/adapter-platform-integration/blob/main/UPGRADE.md).

## A start says which of your names the cluster already held (2026-09-12)

Version 1 compared the identifiers of a deployment against each other and said nothing about the
ones the cluster already held. This version asks the cluster about them, once per workflow module
while it deploys, and writes one WARN per module listing what it found. Nothing fails for it and no
property turns it off.

What is asked about is the BPMN process ids of the module, in one search, and its DMN decision ids,
one search each. Nothing is asked about message names, signal names, error codes, escalation codes
or job types, because the cluster keeps no index of those.

The line you are most likely to read is about a process you RENAMED. A cluster records no owner of
a definition, so the only marker the adapter has is the resource a definition was deployed from: a
definition of yours which came from a file you have since renamed looks like somebody else's. The
message says that it cannot tell the two apart, and the version and the definition key it names are
what you look the definition up by. Where the finding is your own old file, deleting that definition
from the cluster ends the line, which is the same remedy the report about an old process version
asks for.

A finding under the mode `none` is worth reading twice. Nothing is prefixed and no tenant separates
anybody there, so a second application on the same cluster really does share the name, and which of
the two a start reaches is the cluster's decision and not yours. The ways out are a tenant
(`name-clash-avoidance: by-adapter`), a prefix (`use-prefix`) or a name nobody else uses.

Two more lines can appear, and both are about your own application. Two workflow modules which
declare the same message name, signal name, error code, escalation code, job type or decision id are
named with both sides, because under `none` and under one adapter-wide `tenant-id` the cluster sees
one name where you mean two. The same is said where a version the cluster still holds carries such a name,
which is the clash a workflow module deployed years ago leaves behind.

## An ad-hoc subprocess in your model earns two warnings (2026-09-09)

Version 1 said nothing about the element and neither executed nor reported it. This version serves
the flavour whose activities the model names through `zeebe:adHoc activeElementsCollection`, and it
says two things about a model carrying an ad-hoc subprocess which version 1 kept quiet about.

The element is named as a source of a second token, so a workflow aggregate without a version
attribute earns the warning about two writers on one aggregate, whichever flavour the model uses.

The flavour carrying a `zeebe:taskDefinition` of its own earns one WARN per BPMN process saying that
nothing serves it. That was true in version 1 as well; the difference is that it is said now.
Nothing is said about an element which also carries a `zeebe:modelerTemplate`, because a connector
runtime owns that one.

## `allow-connectors` moved under the adapter (2026-09-09)

Version 1 read `vanillabp.allow-connectors` at the root of the tree, with a workflow-module and a
workflow level below it. The key is back and does the same thing, but it sits where it belongs now:

```
vanillabp.adapters.<id>.allow-connectors
vanillabp.workflow-modules.<m>.adapters.<id>.allow-connectors
vanillabp.workflow-modules.<m>.workflows.<w>.adapters.<id>.allow-connectors
```

Connectors are a Camunda 8 concept and no other BPMS has anything to do with the marker, so the key
belongs to this adapter. The three levels are the ones version 1 had, and the default is `false` as
before. Four things an upgrading application has to act on.

**The resolution changed direction.** Version 1 held the flag in primitive booleans, so a more
specific level could only turn it ON: a global `true` plus a module `false` still yielded `true`. Now
the most specific configured value wins in both directions, like every other key of this adapter. If
you relied on the old OR, read your configuration once: a module or workflow which says `false`
under an adapter which says `true` now switches the rule off for itself.

**A user task built from an element template stays wired.** Version 1 passed a user task carrying
`zeebe:modelerTemplate` over together with the service tasks. This version does not. There is no
user-task connector: a `zeebe:userTask` is served by the cluster's task list, and a template on it
presets an assignee or a form. Passing it over would take away its lifecycle listeners, its CREATED
and CANCELED notifications and the ability of `ProcessService#completeUserTask` to complete it, for
a marker which says nothing about who serves the task. If a user task of yours was passed over in
version 1, it is wired here and needs a `@WorkflowTask` method or the model has to stop claiming it.

**Every boot with the switch on writes a warning.** Version 1 logged nothing at all near the
property, so switching it on was silent and a typo in a task definition was indistinguishable from
an intentional connector. This version writes one framed WARN per workflow module which allows
connectors, naming the key, the module, every element it handed over with its element template, and
what that costs. No key silences it, and that is deliberate: what it says stays true for as long as
the connector is in the model.

**Under `use-prefix` a connector's job type stays unprefixed.** Version 1 had no prefixing mode, so
the question could not arise there. Here the job type of such an element is left as the modeller
wrote it, because it names a runtime somebody else deployed cluster-wide. Under `by-adapter` the
module is kept apart by a tenant instead, and a connector runtime then has to be able to see that
tenant, which is a condition on your cluster rather than something the adapter arranges.

## The message check reads the models the cluster holds (2026-09-08)

If you renamed a BPMN process and a `correlateMessage` for one of its old workflows failed with
"No BPMN model of workflow module ... declares a message ...", this is the entry for you. The
check used to read only the models this application version deployed, so a message declared by
the old id's model alone was refused - inside your transaction, while the workflow waited in
the cluster for exactly that name.

The check now reads the models the cluster holds for every BPMN process id the application
declares, the versions of a renamed process' old id included, and it refuses only after
reading the cluster in that very call. Where those models cannot be read the check says
nothing and phase two publishes as asked: a check which cannot see every model that could
carry the answer must stay silent, never refuse. On a degraded adapter against a cluster
without secondary storage (the `deployment-failure: warn` way out below) that silence is the
permanent state - a genuinely mistyped message name is then buffered by the cluster until its
time-to-live passes, which is what such a cluster costs.

## The cluster has to be one the adapter can search (2026-09-07)

The Camunda 8 adapter of 2.0 requires a cluster which answers searches, and it says so while it
deploys instead of running on and answering questions it cannot answer. Two things make a cluster
answer them: it brings secondary storage (`camunda.data.secondary-storage.type`, an Elasticsearch or
OpenSearch it exports to), and the credentials the adapter is configured with may read process
instances, process definitions, jobs, user tasks and element instances. Where either is missing the
cluster refuses every search with HTTP 403 and says which of the two it was in prose only, so the
message names both.

What the adapter uses a search for is the list a reader can check themselves against: locating a
workflow by its aggregate's id, which is what elects the BPMS of an operation and what a pushed
workflow aggregate is written by; resolving a version specification which names a
`zeebe:versionTag`; the viewer's element history and the definitions of earlier application
versions; and the startup report about the versions the cluster still holds. Until now four of those
had a second behaviour for a cluster which refuses: an optimistic yes with a warning, a guiding
failure, an empty version list, a viewer serving what this application version deployed. Those are
gone.

Where you are coming from decides whether this is news.

**From 1.7.0 or later** your application already needed such a cluster and nobody wrote it down. It
read every active process definition of its tenant through the search API while it started, to parse
the BPMN of earlier deployments for the SPI wiring, with no property switching it off and no
fallback around it. A cluster refusing that search kept the application from starting, so what
changes for you is the message: a named requirement with both reasons and both ways out, instead of
whatever the first failing read said.

**From 1.6.3 or earlier** this is a real change. Those versions ran against the 8.6 client and kept
the metadata of earlier deployments in tables of the application's own database, which is what their
README said out loud, and they searched nothing. If your cluster is still one without secondary
storage, 2.0 does not serve it: give the cluster its secondary storage before you upgrade. The
tables are gone either way - 1.7.0 dropped them - so what answers the questions those tables used to
answer is the cluster, and it has to be askable.

An adapter which is not the first-priority adapter of a workflow module and carries
`vanillabp.adapters.<id>.deployment-failure: warn` boots degraded against such a cluster with a
guiding warning rather than ending the start. That is the way out for the old BPMS of a migration
AWAY from a cluster like this, and it takes one more property: an adapter which deployed nothing
cannot answer the BPMS election either, so a workflow module serving two adapters also needs
`vanillabp.workflow-modules.<id>.election.guessing-adapters: ACCEPTED`. The application says both,
one message per step, and comes up once both are set.

## A worker asks for work only while an execution slot is free (2026-08-31)

Two things changed about how a Camunda 8 adapter runs what it delivers, and neither of them needs
a configuration change.

The adapter now hands the client the executor it runs its workers on, in the platform-thread mode
as well as in the virtual-thread one and on every release line. Until now it did that only for
`worker-threads: virtual`; the platform mode let the client build its own pool, which on the 8.8
line schedules the polls of every worker on the same threads the handlers run on. So on that line
`worker-threads` blocked handlers stopped the adapter from asking the cluster for work at all, and
nothing said so. `worker-threads` now counts handlers running at once wherever you look, which is
what the documentation always promised, and the two execution-slot gauges
(`vanillabp.camunda8.execution.slots.in.use` and `vanillabp.camunda8.jobs.waiting`) exist in the
platform mode as well, where they were absent before.

And a worker asks the cluster for work only while an execution slot of its adapter id is free. The
workers of one adapter id share those slots, so a worker used to be able to activate jobs which
then waited in front of them, spending the lock they were handed out with; on the 8.9 and 8.10
lines, where the client always kept polling and handling apart, that was the normal state. Such a
job is now left at the cluster, where another instance of your application can take it. What you
may see is a job arriving up to a moment later than before on an application whose slots are all
busy - which is an application at its capacity, and the slot gauges say so.

## A start waits for its cluster instead of failing at once (2026-08-30)

Until now a start which could not reach its Camunda 8 cluster ended at the first round it tried
to make, usually the deployment. It waits now: before that round the adapter asks the cluster for
its topology and gives it `vanillabp.adapters.<id>.startup-wait`, ten minutes by default, to
answer. The case this is for is a cluster booting together with the application, which lets every
round of a start fail rather than only the deployment.

What an application has to look at is the ten minutes. A deployment pipeline which expects a
start to fail fast against a cluster which is deliberately not there now waits them out; set
`startup-wait: PT0S` for such a setup and the behaviour is the one you had. Nothing waits silently
in the meantime: a line before the first attempt names the address and the deadline, and one every
few seconds carries the time gone and the cluster's last answer.

An answer the cluster will repeat ends the start at once rather than after the deadline. Which
answers those are is `Camunda8Errors`, the classification the whole adapter reads, so a rejected
request and a refused tenant fail as fast as before. A `401` is not among them, on purpose - the
client refreshes an expired token - so wrong credentials are waited out, with the `401` in every
line the wait writes.

Two more things are checked while the application boots, both about `request-timeout`. A value
which is not positive ends the boot, because there is no request without time. A value below one
second is a warning: it is the deadline of every request this adapter sends, the deployment of a
workflow module and every search included, so a healthy cluster answers too late and it reads
like a network problem.

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
  the query API, and the searchable-cluster requirement of 2.0 (at the top of this file) is what
  refuses a cluster which cannot serve one, whether one adapter id addresses it or two,
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
