# Upgrade notes

Documents changes that were necessary when upgrading major dependency versions, or which an
application on this adapter has to act on, so the reasoning can be looked up later. The same
file exists for
[VanillaBP itself](https://github.com/vanillabp/adapter-platform-integration/blob/main/UPGRADE.md).

## A restart waits a few seconds longer, and the application after it does not (2026-08-21)

Story 102. No new property, and nothing to configure. What changes is how long a shutdown
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
  waives the wait together with the handler drain of story 90,
- a process which is killed rather than asked to stop cannot pay it, so a workflow started
  within ten seconds of a `SIGKILL` may still wait for its lock. A shorter `job-timeout`
  bounds what that costs where restarts are frequent, and `stream-enabled: true` or
  `prefer-rest-over-grpc: false` avoids the case altogether.

The line the shutdown writes changed with it. It now says how many workers were closed and
whether the cluster released them, and it warns where one of them still holds its request
when the grace passes.

## The cluster reports itself to your metrics and your health endpoint (2026-08-20)

Story 92. Additive with one new property, and nothing changes for an application which does
nothing.

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

Story 53. Visible to every consumer, because the coordinates change.

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
