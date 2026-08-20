# Upgrade notes

Documents changes that were necessary when upgrading major dependency versions, or which an
application on this adapter has to act on, so the reasoning can be looked up later. The same
file exists for
[VanillaBP itself](https://github.com/vanillabp/adapter-platform-integration/blob/main/UPGRADE.md).

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
