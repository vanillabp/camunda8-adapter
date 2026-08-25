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

## Release lines

A Camunda 8 cluster upgrade is expensive, more so organizationally than technically, and
users sit on different minors at the same time. VanillaBP still has to move onto every new
Camunda version quickly, because some of what users ask for exists only in the newest
cluster and some of it is what the adapter itself needs.

One artifact cannot do both. Camunda promises a client against clusters of its own version
and newer, and says nothing about the other direction, so the client a build was compiled
against IS the lowest cluster version that build accepts. As soon as an artifact uses
anything only an 8.10 cluster has, every later bugfix in it is deliverable only together
with a cluster upgrade.

That the other direction really does fail is measured rather than assumed: the blueprints
ran a build against `camunda/camunda:8.8.34` while the adapter was compiled against the
8.9 client, and every job activation was rejected with `Request property [tenantFilter]
cannot be parsed`, so a workflow started and its task was never delivered.

The adapter is therefore published once per Camunda 8 minor, with the minor in the version:

|   Channel   |        Version        |   Client pin    |         Tested cluster          |            What lands there            |
|-------------|-----------------------|-----------------|---------------------------------|----------------------------------------|
| previous GA | `2.x.y-8.8`           | `8.8.35`        | `camunda/camunda:8.8.35`        | bugfixes only                          |
| current GA  | `2.x.y-8.9`           | `8.9.16`        | `camunda/camunda:8.9.16`        | everything                             |
| preview     | `2.x.y-8.10-alpha<n>` | `8.10.0-alpha4` | `camunda/camunda:8.10.0-alpha4` | everything, plus what only 8.10 can do |

The tested cluster is the pinned client, and nothing else may appear here as a supported
version: a newer cluster is not tested rather than not supported. Camunda documents its
clients as forward compatible, so a line normally serves clusters above its pin as well,
but that is Camunda's promise and not this project's test result.

The preview line is not publishable at the moment. On `8.10.0-alpha4` the user-task listener
job of the event type `creating` never reaches its worker, so three tests of
`Camunda8TaskProcessingIT` time out while the other 34 tests of the line pass. The cause is an
open Camunda bug, `camunda/camunda#58193`: the REST gateway throws a `NullPointerException`
while converting a `TASK_LISTENER` job and drops the whole activate-jobs batch.

Those three tests are excluded on that line, by the tag `user-task-listener-jobs` in the
`line-8.10` profile, and nowhere else. Leaving them in kept the line red as a whole, and a line
which is always red says nothing about the day something else breaks in it. What the exclusion
costs is written into both places: the tag in `Camunda8TaskProcessingIT` and the profile in the
root POM name the bug and say to remove them together once Camunda ships the fix. Until then the
preview line stays unpublishable for the same reason as before, tests or no tests.

Snapshots have no suffix yet. Until the first release they are `2.0.0-SNAPSHOT` of the
current GA line, which is what a build without a profile produces.

### Which line to use, and moving to the next one

Take the line whose pin is at or below your cluster's minor. On 8.8 that is `-8.8`, on 8.9
and above `-8.9`, and `-8.10-alpha<n>` if you run an alpha, usually because the cluster is
not productive yet and you want everything the newest one offers.

Moving to the next line means upgrading the cluster, so it is one decision and not two.
Renovate will not do it behind your back: the version suffix is read as a compatibility
value, and Renovate never proposes an update that changes it. Extend the preset shipped
here to inherit that in your own application:

```json
{
  "extends": ["github>vanillabp/camunda8-adapter//renovate/camunda8-lines.json"]
}
```

The VanillaBP-facing API is identical on every line, checked in CI by
`bin/api-identity.sh`. You never have to read a version suffix to find out which methods
exist. Where a line's cluster cannot do something, the same method is there and fails with
a message naming your line, the way `cancelUserTask` does. The adapter logs the line and
the client it was built against once per adapter id at startup, so the log says which
cluster minimum is in effect.

### How long a line lives

A GA line lives until the next minor goes GA, so there are two GA lines at a time plus the
preview. When 8.10 goes GA in October 2026, 8.9 becomes the previous GA and 8.8 ends, even
though Camunda supports 8.8 until April 2027. That is our policy and not a technical limit:
it keeps the matrix at three builds and three cluster runs.

### How the lines are built

Every line is a build variant of this one source tree, not a maintenance branch. A line is
a Maven profile that selects the client pin, and with it the cluster the integration tests
run against:

```bash
mvn install verify                                          # current GA line, 2.0.0-SNAPSHOT
mvn -Pline-8.8 -Drevision=2.1.0-8.8 clean install verify    # a release of the previous GA line
mvn -Pline-8.10 -Drevision=2.1.0-8.10-alpha1 clean install verify
```

Switching a line always needs `clean`, and the CI does it that way. Classes compiled
against one client are binary compatible with no other one: a method the 8.9 model library
inherits from a type 8.8 does not have at all is called through the owner the compiler saw,
so a stale `target/` fails at runtime with a `NoClassDefFoundError` rather than at compile
time. Building the same line again is fine.

The version is `${revision}`, resolved into the published POMs by `flatten-maven-plugin`,
so the same commit produces every line. A fix therefore exists on every line the moment it
is committed, and the version number proves it is the same fix. Branches cannot promise
that, and the adapter changes constantly for reasons that have nothing to do with Camunda,
which would mean cherry-picking every one of those changes into every line.

Code that cannot be shared goes into a per-line source directory added by
`build-helper-maven-plugin`, `src/main/java-line-<id>` and `src/test/java-line-<id>`. Only
two kinds of code belong there: code that cannot compile against every supported client,
and code that uses something only a newer cluster has. The test directories hold one test
per line, which proves the pin reached the runtime.

The main directories hold exactly one class today, `Camunda8JobExecutors`, and it is the
textbook case for the scheme. The virtual-thread execution model hands the client an
executor of the adapter's own, and which builder method takes it changed with 8.9: the 8.8
client knows one executor for both the polling and the handler invocations, while 8.9 asks
for `jobWorkerSchedulingExecutor` and `jobHandlingExecutor` separately and its
`jobWorkerExecutor` sets only the first of the two. A client configured through the shared
method alone would therefore run its handlers on the client's own pool from 8.9 on, which
is one thread unless something says otherwise - and that is the very defect the model
exists to fix. The class is package-private and has no public members, so the API identity
check sees the same declaration on every line.

What a line did need so far is a dependency pin rather than code. The 8.10 client brings
generated protobuf code linked against 4.35.1, and protobuf refuses a runtime older than
its gencode, while the Spring Boot BOM manages 4.34.2 and an imported BOM beats a
transitive version. The parent POM therefore manages `protobuf-java` itself, before that
import, high enough for every pinned client. It has to be raised whenever a client's
gencode goes above it; the failure otherwise is an `ExceptionInInitializerError` on the
first command that touches the protocol, which the integration tests of the line catch.

### The tripwire

This scheme was chosen because the per-line delta is small. If the delta grows past a
handful of classes, or the shared code stops compiling on a line in a way a small shim
cannot bridge, then it has become a branch scheme in disguise, and the line is to be split
off deliberately as a maintenance branch. Whoever hits that limit will not be
the person who chose the scheme, so it is written down here.

### Version ordering, and why Renovate does not use maven versioning

Maven orders the suffix as an addition rather than as a pre-release, which is what makes it
usable at all: `2.1.0-8.8 > 2.1.0`, and `2.1.0-8.9 < 2.1.0-8.10` numerically rather than
lexically. One comparison goes wrong, and it is the whole risk of a suffix:
`2.1.0-8.9 < 2.2.0-8.8`, so "the newest version" can cross a line boundary. Renovate reads
the suffix as a compatibility value instead of a version part, which fixes exactly that.
`renovate/verify-line-gating.js` runs the check in CI, including the case above.

A pre-release of the preview line is `2.2.0-8.10-alpha1`: the qualifier comes after the
line, so the line always sits in the same place, and Maven sorts
`2.2.0-8.10-alpha1 < 2.2.0-8.10-alpha2 < 2.2.0-8.10`. `preview1` was rejected, because
Maven ranks an unknown qualifier ABOVE the release: `2.2.0-8.10-preview1 > 2.2.0-8.10`.

### What CI runs

The nightly matrix (`.github/workflows/line-matrix.yaml`) builds every live line with its
own version string and runs its integration tests against its own cluster. The matrix is
read out of the `line-*` profiles, so it cannot fall behind the build. A pull request runs
the current GA line alone: the Camunda 8 integration tests are the slowest thing in the
workspace, and every change touching the adapter would otherwise pay for every line. What a
pull request does run for all lines is the API identity check, which needs no cluster.

### Release and CI plumbing

A release of one line consists of:

1. `mvn -Pline-<id> -Drevision=<version>-<id> deploy` from the release commit, once per
   live line, all from the same commit. The preview line publishes as a pre-release with
   `-alpha<n>` appended.
2. The nightly matrix green for every line, because that is the only evidence for the
   tested-cluster column of the table above.
3. The API identity check green, so no line gained or lost a method.
4. The line table of this README and of the wiki updated when a pin moved.
5. The version property of every consumer pointed at the suffixed coordinates. The
   blueprints carry `camunda8-adapter.version`, which is `2.0.0-SNAPSHOT` today and has to
   become a suffixed version with the first release. That switch is the moment the suffix
   becomes visible to users, and `UPGRADE.md` describes it.
6. The consumers of the snapshot follow the current GA line too. The blueprints start a
   Camunda 8 cluster of their own for the CI (`bin/camunda8_cluster.sh`), and that cluster
   has to be at least the client the adapter was built against, so it moves with the
   default line.
7. One real `renovate --dry-run` against the published artifacts. The gating is proven
   today by `renovate/verify-line-gating.js`, which asks Renovate's own versioning module
   what it would offer a consumer of each line; a full dry run needs versions in a
   datasource, and until the first release there are none.

Rotating the lines when a minor goes GA touches four places: the `line-*` profiles and the
pin properties of the parent POM, the boundary rule of `renovate.json`, the table above,
and the wiki.

## Dependencies

All artifacts use the groupId `org.camunda.community.vanillabp`. Their version carries the
release line once the adapter is released, e.g. `2.1.0-8.9`; until then it is
`2.0.0-SNAPSHOT` of the current GA line, see [Release lines](#release-lines).

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

|                    Property                     |  Applies to  |                  Required                  |                                  Description                                  |
|-------------------------------------------------|--------------|--------------------------------------------|-------------------------------------------------------------------------------|
| `vanillabp.adapters.<id>.mode`                  | both         | no (default `self-managed`)                | `self-managed` or `saas`                                                      |
| `vanillabp.adapters.<id>.rest-address`          | self-managed | yes (unless `prefer-rest-over-grpc=false`) | REST API address, e.g. `http://localhost:8080`                                |
| `vanillabp.adapters.<id>.grpc-address`          | self-managed | only if `prefer-rest-over-grpc=false`      | gRPC address, e.g. `http://localhost:26500`                                   |
| `vanillabp.adapters.<id>.prefer-rest-over-grpc` | self-managed | no (default `true`)                        | use the REST API (recommended) or gRPC                                        |
| `vanillabp.adapters.<id>.cluster-id`            | saas         | yes                                        | SaaS cluster ID                                                               |
| `vanillabp.adapters.<id>.region`                | saas         | yes                                        | SaaS region                                                                   |
| `vanillabp.adapters.<id>.client-id`             | saas         | yes                                        | OAuth client ID                                                               |
| `vanillabp.adapters.<id>.client-secret`         | saas         | yes                                        | OAuth client secret                                                           |
| `vanillabp.adapters.<id>.tenant-id`             | both         | no                                         | Camunda 8 multi-tenancy tenant                                                |
| `vanillabp.adapters.<id>.auth.*`                | both         | no (default: no credentials)               | how the adapter authenticates, see [below](#authenticating-against-a-cluster) |

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

### Authenticating against a cluster

The adapter used to authenticate against Camunda SaaS and against nothing else.
`client-id` and `client-secret` hung on the cloud builder, the self-managed branch set
addresses, the transport preference and the tenant, and never a credentials provider. A
self-managed cluster with its authentication switched on, which is what a self-managed
installation normally looks like, was therefore unreachable, and no message said why, because
the adapter had no property to offer. Our own integration tests hid it: every cluster here ran
with `CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI=true`, so an adapter sending no
credentials passed all of them. `Camunda8AuthenticationIT` is the test that would have caught
it, and it runs against a cluster where nothing is unprotected.

`vanillabp.adapters.<id>.auth.*` carries the credentials of one adapter instance, at adapter
level only: a credential is a property of the connection, and a per-workflow level would be a
promise the connection cannot keep.

**Three methods, because the client has three.** `none`, `basic` and `oidc` are what
`CredentialsProvider` can build, and both builders are used as they are rather than
reimplemented, so the OIDC token is cached in a file and refreshed by the code Camunda
maintains. A fourth value `mtls` was considered and dropped: the Camunda Java client offers no
keystore for its own gRPC or REST connection on any of the three release lines, not through
`CamundaClientBuilder`, not through `ClientProperties`, not through an environment variable.
Its keystore and truststore belong to the token request against the identity provider, and the
`auth` block says so. A property which quietly configures the token request while the user
believes it configures the cluster connection is worse than no property, so this is a
documented deviation instead.

**An absent `method` is detected and the detection is logged.** A user name detects `basic`, a
client id detects `oidc`, a SaaS adapter is `oidc` through its connection keys, and nothing at
all is `none`. The startup line which names the address names the method next to it, with
`(detected)` where nobody wrote it down. A `none` nobody noticed is how the gap survived in the
first place, so it does not get to be quiet a second time. Credentials which cannot belong to
the resolved method fail the boot rather than being ignored, and so do two methods configured
at once: a key nobody sends is a key somebody wrote for nothing.

**A self-managed OIDC adapter has to name its authorization server.** The Camunda client falls
back to `https://login.cloud.camunda.io/oauth/token/` when an OIDC client names none, which no
on-premises installation ever means, so `authorization-server-url` and `audience` are required
outside SaaS. `issuer-url` and `well-known-configuration-url`, which the 8.9 client added, are
deliberately not modelled: the VanillaBP-facing API is identical on every release line, and the
8.8 client has neither.

**SaaS shares the code path.** Its connection keys become the OIDC client with two presets,
Camunda's login endpoint and the audience `zeebe.camunda.io`, which is byte for byte what the
cloud builder would have built. Existing SaaS configurations therefore keep working, and they
gain the rest of the `auth` block: the credentials cache and the timeouts of the token request
were previously unreachable.

**The runtime message rides on `shouldRetryRequest`.** Whether a cluster accepts a credential
is only learnable by asking it, so `method: none` cannot be validated at startup. The client
asks the credentials provider `shouldRetryRequest` on every request a cluster refused, on both
transports and for commands as well as for job activation, which makes that one method the
place where an adapter learns it is unwelcome. The provider handed to the client is wrapped in
`Camunda8Authentication.Observing`, which reports once per adapter id: for `none` with the YAML
which names a method, otherwise with the fact that the configured credentials reached the
cluster and were refused. Building on the classification in `Camunda8Errors` was the
alternative and was dropped, because it would have needed a call at every one of the two dozen
places a command is sent and would still have missed the workers, whose activation failures the
adapter never sees.

**`none` plus credentials in the environment sets no provider at all.** The client builds one
from `CAMUNDA_CLIENT_ID`/`CAMUNDA_CLIENT_SECRET` or `CAMUNDA_BASIC_AUTH_USERNAME`/
`CAMUNDA_BASIC_AUTH_PASSWORD`, but only while the application set none, so setting a Noop
provider would have switched off a deployment which relied on those variables. That case
therefore hands the client nothing, at the price of the runtime message, and says so in the
startup line. Every other case sets a provider, and where the environment carries credentials
alongside, a WARN names the variables and what they no longer decide.

**Who the adapter authenticates as is part of its instance identity.** Two adapter ids on one
self-managed address were previously the same instance and failed the boot; with separate
accounts they are two, which is the same reasoning that already made the SaaS client id count.

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

### When a phase-one check runs

The non-advancing checks of phase one - the job-timeout update for a service task, the empty
update for a user task - run right before the caller's transaction commits, not when the
application calls. The later the check, the smaller the window in which its answer can go
stale before phase two acts on it, and a failing check aborts the commit, which is how the
application learns about a task which is already gone.

The adapter no longer carries that mechanism. It hands the check to the platform
(`PreCommitRegistrar` of the adapter SPI), naming the workflow aggregate, and the platform
asks the transaction runner of that aggregate - which may be a unit of work the
application brought. A runner which cannot offer a pre-commit hook runs the check immediately,
the behaviour this adapter had before.

### The delivery identity is a job key, so it belongs to one cluster

`Camunda8JobHandler` reports the job key as the delivery id, which is what the core remembers a processed delivery
by. A key is stable across every redelivery of that job and is never handed out twice - within one
cluster. The delivery key of the core starts with the adapter id
(`TaskDeliveryKey`: `<adapterId>|<workflowModuleId>|<bpmnProcessId>|<event>|<deliveryId>`), so two adapter ids have
separate identities and a migration between two clusters works even though both count their keys from the same
range.

Replacing the cluster BEHIND one adapter id is the case to know about: a rebuilt cluster starts its keys over, so a
record written for the old one can answer a delivery of the new one, and that task is skipped without a word. The
delivery table of that adapter id has to be emptied then. Recognising it automatically was considered and left out
(2026-08-19): the client offers no cluster identity - `Topology` knows brokers, size and version, nothing unique -
and every heuristic (comparing process definition keys, watching for keys which suddenly become smaller) either
misses cases or risks the opposite mistake, processing a task twice after a harmless redeployment. So it is
documented here and in the wiki instead.

### Which phase-two failures are repeated

The phase-two outbox repeats a failed operation until the entry is blocked. That is right for
a cluster which is busy, unreachable or lost a conflict, and pointless for a command the
cluster rejects: the answer will not change, and the retries only fill the log while
operations wait for the entry to block. The adapter therefore classifies a failure as
permanent when the chain of causes holds one of these:

|                 Failure                  |             Why a repetition cannot help              |
|------------------------------------------|-------------------------------------------------------|
| HTTP `400`, gRPC `INVALID_ARGUMENT`      | the cluster rejected the request itself               |
| HTTP `403`, gRPC `PERMISSION_DENIED`     | credentials or tenant are wrong, not late             |
| HTTP `405` / `501`, gRPC `UNIMPLEMENTED` | this cluster version has no such endpoint             |
| `NumberFormatException`                  | the task or instance key of the entry is not a number |

Everything else is repeated, including three cases which look permanent at first glance.
`404` is the signature of eventual consistency, and for job commands it never reaches the
classification at all - a gone job is the accepted at-least-once residual and consumes the
entry. `401` is usually an expired token, which the client refreshes. `409`, `429` and every
`5xx` are what the outbox exists for.

The same classification serves the commands a job handler sends back to the
cluster (`Camunda8Errors.repeatableJobCommandFailure`), adding the one case which is
permanent there and not here: a job which is gone. One classification serving both
directions is the point - a second opinion about what a repetition can change would drift
away from this one.

### Why correlating a message has no cluster preflight

Since 8.8 the client can search message subscriptions, so a preflight would be possible - and
it would be wrong. The cluster BUFFERS a published message for its time-to-live, so
correlating before the subscription exists is legitimate, and a search would reject exactly
that case. The search also reads the eventually consistent secondary storage, whose window the
caller would wait out inside their own transaction.

What phase one does check is the MODEL: if no BPMN model of the workflow module deployed by
this application version declares the message, the correlation fails where the application
called it. That is the mistake a preflight could have caught - a typo, or a message renamed in
the model - and without the check phase two would publish into the void: the cluster accepts
the publication, the time-to-live passes, nothing correlates and nothing fails. Where this
application version deployed no process of the workflow module (a workflow still running on a
definition of a previous version), the declared names are unknown rather than absent and the
check stays silent.

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

### How the adapter runs what it delivers

The Camunda client owns one executor per client and this adapter owns one client per adapter
id, so a single number decides how much of everything an adapter delivers may be in flight
at once. That number used to be the client's own default of one, nothing passed it
through, and on the 8.8 client that one thread runs the handler invocations AND the poll
scheduling of every worker. Measured against a real cluster: an unrelated job of another
worker waited 8013 ms behind a blocking handler and 13 ms with four threads, and a poll
scheduled with a delay of 100 ms started 4837 ms late while the broker's counter of
completed activation requests stood still. The second half is why this was worth a story of
its own: the backlog was invisible to every client-side signal.

`vanillabp.adapters.<id>.worker-threads` takes a positive number or the literal `virtual`,
and it sits at adapter level because the executor is per client. A workflow-module level
would be a lie.

**Four platform threads by default.** More than one, because one is the defect above. Small,
because every concurrent handler holds a database connection inside VanillaBP's transaction
and the usual pools are ten (Hikari) to twenty (Agroal) connections wide, so four leaves room
for the rest of the application. Four was also what turned 8013 ms into 13 ms in the probe.
The number to size against is the connection pool, not the CPU, and the wiki says so where a
user looks for it.

**The virtual mode was measured before it was offered.** On Java 21 a `synchronized` block
around a blocking call pins the carrier thread, which inside a transaction would be a silent
regression, so the question was settled with a probe rather than an opinion:
`-Djdk.tracePinnedThreads=full` plus the `jdk.VirtualThreadPinned` JFR event over 64 virtual
threads x 50 transactions, against Spring's `DataSourceTransactionManager` with HikariCP and
against Narayana with Agroal, each on embedded H2, on H2 over a TCP socket and on a real
PostgreSQL 16. Zero pinning events in all six combinations; a positive control (a
`Thread.sleep` inside `synchronized`) produced four, so the detection was working. The
drivers moved off `synchronized` for exactly this reason (pgjdbc since 42.5.1, H2 2.x), and
JDK 24 removes the question altogether. So `virtual` is a supported mode rather than a
caveat, and the default stays at four platform threads: at the same bound it buys nothing on
the 8.9 client, which splits scheduling from handling by itself, and a platform pool is what
every line does natively.

`Camunda8VirtualThreadExecutor` is that split for the 8.8 line: two platform threads for the
timing, a virtual thread per submitted task for the work, bounded by a semaphore whose
permits `worker-threads-bound` sizes. Two details are deliberate. The permit is taken INSIDE
the virtual thread, not in `execute`, because the thread calling `execute` is the client's and
blocking it would stall the delivery of every other worker - the defect the mode exists to
avoid. And the bound defaults to the number the platform mode would use, so switching the
mode changes how threads are made and not how much runs at once. With `stream-enabled` the
client wraps whatever executor it was given in its own semaphore of `max-jobs-active` permits
whose acquire waits for the job timeout, so the effective limit is then the smaller of the
two.

**The worker settings are set on the CLIENT, not on every worker.** A worker builder inherits
the client's defaults, and setting them per worker would defeat the environment variables the
next paragraph is about. Only `stream-timeout` has no client-wide equivalent and is therefore
set per worker. `max-jobs-active` defaults to eight per execution slot capped at the client's
32, which is the familiar 32 at four slots and scales down to 8 at one, so the last job of a
batch waits for seven handler runtimes instead of thirty-one. A value below the slot count
fails the boot: some slots could never be busy.

**The three hard coded one-minute locks are gone.** The user-task lifecycle listener, the
BPMS-initiated start and the workflow-ended worker run application code in a transaction
exactly like a task does, so their lock resolves through `Camunda8JobTimeoutResolver` at
adapter, workflow-module and workflow level (no task level, there being no task to key them
by) and defaults to the same five minutes as `job-timeout`. There is no reason for two rules.
One user-task listener job type may belong to several BPMN processes of a module; where those
resolve to different locks the deployment fails guiding, the same way conflicting job timeouts
of one task definition do.

**Environment variables keep their power and lose their silence.** The client applies
`CAMUNDA_*` variables (with legacy `ZEEBE_*` fallbacks) over everything the builder set, and
the probe proved that nothing is logged about it, not even at TRACE: the addresses, the
transport preference, the CA certificate, the TLS authority, the default tenant and the
streaming default could all be replaced without a word in VanillaBP's own log, right after
VanillaBP had validated and reported them. Switching the override off was rejected, because
it is today the only way to reach a client option this adapter does not model.
`Camunda8EnvironmentOverrides` compares what the adapter configured against what the built
client reports and logs a WARN naming every value a variable changed, with the variable, the
property key and both values. Credentials are not among the compared values, so no message
can carry a secret. What that means for credentials is settled by
[Authenticating against a cluster](#authenticating-against-a-cluster): the client installs
a provider from the environment only while the application set
none.

### Task processing

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
     failed with decremented retries and a `retryBackoff` (Camunda 8 redelivers
     after it, see below).

**Asynchronous tasks (`@TaskId`) and the renewal of their lock:** a handler
receiving the task ID completes the task later via `ProcessService#completeTask`.
Such a job must not be redelivered while it waits, so after the commit the adapter
extends the job's lock via `UpdateJobTimeout` by `async-task-lock-renewal` (default
`PT1H`). When the window passes, the cluster hands the same job out again, the core
answers that delivery from its delivery record with `COMPLETION_PENDING`, and this
branch extends the lock once more: the renewal is driven by the cluster's own
redelivery and needs no timer of the adapter's. The worker's own job timeout stays
SHORT - it is the crash-recovery horizon for synchronous handlers.

The window has to sit clearly below `vanillabp.outbox.retention` (seven days), since
the delivery record is what answers the redelivery which renews the lock; a value
which is not below it ends the boot naming both properties and both values. The key
was called `async-task-timeout` once and meant a horizon of fourteen days
which outlived that record, so an asynchronous task open longer than it ran the
handler a second time; the old key now ends the boot naming its successor.

The core measures how long such a task has been open (`vanillabp.delivery.max-task-age`,
`P30D`, report only) and reports it once. Where `async-task-max-age-action` is
`incident` this adapter stops renewing the lock of an overdue task and fails its job
with no retries left, so the cluster raises an incident naming the workflow aggregate
and the age.

**The command which reports the outcome is repeated:** a cluster which
cannot keep up rejects commands, as `RESOURCE_EXHAUSTED` on gRPC and as HTTP 503 on
REST, and the client repeats neither of them (its gRPC retry policy is off by default
and would not cover REST anyway; probe P5b measured 19.433 of 20.000 gRPC commands
rejected at the caller against one node). The outbox covers the phase-two commands, so
what was left unprotected was the command the handler itself sends back: a rejected
completion of committed work escaped into the client's fail path and cost the job a
retry. `Camunda8CommandRetry` now wraps the completion, the BPMN error, the fail command
and the lock renewal of all four worker kinds. It repeats only what
`Camunda8Errors.repeatableJobCommandFailure` calls repeatable, which is the outbox
classification named above plus the gone job (repeating a command against a job which no
longer exists would turn the tolerated at-least-once residual into a storm). It stops at
the job's remaining lock, read from `ActivatedJob#getDeadline()` rather than from the
configured timeout, at five attempts, and at once when the module is shutting down (the
job keeps its lock then, and a retry loop must not hold the drain). The waits are the
client's own activation backoff numbers: 50 ms initially, factor 1.6, a tenth of jitter
and a 5s ceiling the five attempts never reach, which keeps the whole sequence below half
a second because a waiting handler occupies an execution slot. When the bound is reached
the original failure is rethrown, so the behaviour after the retry is exactly what it was
before.

`retry-backoff` (default `PT10S`, resolvable per module, workflow and task like
`job-timeout`, resolved per COMMAND rather than per worker, so nothing has to be aligned
between the processes one worker serves) travels with every fail command which leaves the
job retries. A job failed with `retries(0)` carries none, there being no next attempt. The
error message of a fail command carries the exception's TYPE next to its message, because
that text is what an operator reads in Operate and `NullPointerException` used to write
`null` there.

**Shutting down while work is in flight:** the client does not drain. A
worker's `close()` returns without waiting for the jobs it already handed to a handler,
and `CamundaClient.close()` interrupts every running handler milliseconds later. So
`stopWorkflowProcessing` closes the module's workers and then waits `shutdown-grace`
(default `PT20S`) for the handlers which are still inside the application; every handler
registers its delivery in a per-module `Camunda8Drain`, which is what the wait watches.

**And for the workers themselves.** The handler drain deliberately did not wait for
`JobWorker#isClosed()`, because that answer also covers the activation request in flight
and closing a worker does not cancel it, so an idle worker keeps reporting open for up to
`request-timeout`. What that costs was known, what it buys was not. Measured against
`camunda/camunda:8.9.16` with a plain client and no VanillaBP: an activation request which
is parked at the cluster when its client is closed **stays parked**, and a job created
afterwards is activated into it and answered by nobody. `job-timeout` `PT20S`, one
application closed and the next one starting after the gap below, twenty runs for the two
rows which say so and three respectively five for the others:

|            gap between the two applications            |    first job of the new one     |
|--------------------------------------------------------|---------------------------------|
| 3 s                                                    | 20109 / 20120 / 20202 ms        |
| 7 s (20 runs)                                          | 20027 to 21559 ms, median 20829 |
| 12 s (beyond `request-timeout`)                        | 15 / 23 / 25 ms                 |
| 7 s, workers left open instead of closed               | 20159 / 20194 / 20244 ms        |
| 7 s, the shutdown waiting for `isClosed()` (20 runs)   | 10 to 29 ms, median 10          |
| 7 s over gRPC, nothing waited for (5 runs)             | 7 to 22 ms                      |
| 7 s with `stream-enabled`, nothing waited for (5 runs) | 8 to 26 ms                      |

So the hole is exactly as long as an activation request can outlive its client, closing
the workers first does not shut it, and waiting for them does. The last two rows say where
it lives: over gRPC and over the push path the cluster releases what a gone client held,
and only the REST poll, which `prefer-rest-over-grpc` defaults to, keeps it. The wait is part of
`shutdown-grace` and cost 8,2 to 8,5 seconds in those runs, which is the remainder of the
ten-second request window. `PT0S` switches it off together with the handler drain, and the
shutdown then says at INFO what stays open.

The line the drain writes reports what this adapter knows: how many workers IT closed, and
whether the cluster released them. A worker still holding its request when the grace passes
is a WARN naming `job-timeout` as the delay the next application pays. And because the
promise is only worth what the last shutdown path does, `Camunda8ClientFactory.close()`
closes the workers of every workflow module which never reached `stopWorkflowProcessing`
before it closes the client, with a warning that a hook was missing.

What is still running when the grace passes is named per job (job key and task) and then
cut off. Such a delivery is not reported as a job failure: while the module is shutting
down, all four handlers leave the job to its lock rather than sending `newFailCommand`,
so the cluster redelivers it with its retries intact and the delivery record answers the
redelivery. The rule is the adapter's STATE and never the exception type - a handler
interrupted by the closing client throws like any other. The default sits below the
shutdown budgets of Spring Boot (`spring.lifecycle.timeout-per-shutdown-phase`) and
Kubernetes (`terminationGracePeriodSeconds`), both 30 seconds, so VanillaBP is never the
reason a container is killed; a larger value warns at startup that those have to be
raised with it.

Task-scoped configuration (see the four-level pattern of the VanillaBP
configuration model - the most specific configured value wins):

```yaml
vanillabp:
  adapters:
    myengine:
      type: camunda8
      job-timeout: PT5M                  # adapter level (default PT5M)
      async-task-lock-renewal: PT1H      # adapter level only (default PT1H)
      retry-backoff: PT10S               # adapter level (default PT10S)
      async-task-max-age-action: report  # adapter level only (default report)
      shutdown-grace: PT20S              # adapter level only (default PT20S)
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
                  job-timeout: PT10S     # per task (task definition)
                  retry-backoff: PT30S   # per task, for a slow dependency
                  fetch-variables: all   # per task, for a @TaskParam nobody can derive
```

Limitation: Camunda 8 workers subscribe by job type only. If the SAME task
definition appears with DIFFERENT resolved job timeouts within one module, the
startup fails with a guiding message (one worker per job type - give the
definitions distinct names or align the timeouts).

**Completing/canceling async tasks (`ProcessService#completeTask`/`#cancelTask`):**
the adapter locates the job by its key (the `@TaskId` value). The
awareness probe and the phase-one check are the same NON-ADVANCING command -
`UpdateJobTimeout` by `async-task-lock-renewal` (which conveniently renews the open
job's lock): success means the job exists, `NOT_FOUND` maps to
"unknown", a connection failure to "BPMS unavailable" (never falls back to
another adapter). The phase-one check runs as a PRE-COMMIT transaction
synchronization - as late as possible, minimizing the window between check and
the phase-two dispatch (fewer stale outbox entries). Phase two (after the
commit, through the outbox) sends `CompleteJob` respectively `ThrowError` (the
BPMN error code routes boundary events); a `NOT_FOUND` answer is tolerated with
a WARN (at-least-once residual). Camunda 8 cannot deliver `@TaskEvent CANCELED`
- Zeebe does not notify workers about canceled jobs.

**User tasks:** Camunda-managed user tasks (`zeebe:userTask`) with an
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
**`cancelUserTask` is NOT supported by any cluster up to 8.9:** the engine
offers no command to cancel a Camunda-managed user task by BPMN error (ThrowError
is job-based) and V1's marker-variable workaround is broken by V1's own admission
- a guiding error naming the release line explains it; the listeners it needs
arrive with Camunda 8.10, so it can only ever come on a line built against 8.10
or later.

**Message correlation:** `correlateMessage` publishes AFTER the commit
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

### What a worker fetches

A Camunda 8 worker which names no variables receives the complete variable scope of the
process instance with every job, which Camunda warns can be "tens or more variables, of
arbitrary size" and advises against: fetch only what the handler needs. VanillaBP can be
stricter than a plain client user, because the workflow aggregate is the source of truth.
The handler is served from the application's own database, so the job has to carry only
what the ADAPTER reads out of it, and that is a short list the adapter derives from the
deployed models:

- the variable holding the workflow aggregate's id, named after the aggregate's id
  attribute. Every worker kind begins by reading it;
- the multi-instance variables of the iterations enclosing the element the job belongs to,
  which this adapter injected into the model while deploying. They depend on
  the element, which is why the list is not a constant;
- every variable a `@TaskParam` of the served tasks reads, reported by the core
  (`WorkflowTaskInvoker#taskParameterNames`). The adapter used to read those names off the
  MODEL instead - the mapping targets, script and decision result variables and
  multi-instance output collections a Camunda 8 process declares - because that was the
  only place the adapter could see one. It was a guess in both directions: a model declares
  names nobody reads, and a handler may read a name no model declares. The core scanned the
  annotations while wiring anyway, so it answers exactly, and the model scan is gone rather
  than kept as a second source. The workflow-end listener is the exception: a
  `@WorkflowEnded` method cannot declare a `@TaskParam`, so that worker stays at the
  aggregate's id.

What stays out is everything nobody reads: what only the aggregate sync wrote into the
instance, and what the model declares for its own purposes. On an aggregate with a few
large attributes that is the whole of what Camunda's warning is about, and it is a copy of
data the handler is holding anyway.

The list belongs to the WORKER and not to the delivery. One worker serves a job type
across the BPMN processes of a workflow module, so its list is the union over everything
it serves; two processes disagreeing about the name of the aggregate id are no conflict,
`fetchVariables` being a list. The list is sorted, because the gateway treats two job
streams as equivalent only when job type, worker name, timeout and fetch variables match
and that comparison has to survive a restart of the same application version.

Two cases fetch everything instead. A worker serving a start event the cluster fires
itself hands the variables of that start to the core, which copies them into the aggregate
it builds, so there is nothing to leave out. And where no workflow service serves the BPMN
process, the aggregate's id variable cannot be named at all; such a worker asks for
everything rather than for a list which may be missing exactly what its handler needs.

`vanillabp.adapters.<id>.fetch-variables: all` is the escape hatch, resolvable per
workflow module, workflow and task. A statically named `@TaskParam` does not need it, which
leaves the case the scanner cannot see: a name assembled while the delivery runs. Such a
read is not answered with a null. It fails the delivery with a message
naming the variable, the list and the property, and saying that the name is not on the
method - so the cluster raises an incident instead of the handler computing on a value which
was quietly dropped.

Every worker logs at DEBUG what it fetches when it opens. When somebody reports a variable
their handler no longer sees, that line answers the first question.

### Viewing workflows

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
BPMN process; `none` scopes nothing.

Prefixing is what makes tenants avoidable, which matters because Camunda licenses per
tenant, and it is transparent: BPMN, business code and configuration keep the plain
identifiers while the adapter translates at every boundary.

**The default is `by-adapter`, which is version 1's behaviour** (its `use-tenants` was on
and the tenant id defaulted to the workflow module id), so an application upgrading without
touching its configuration keeps addressing the workflows it started before. What the
cluster owes that mode is multi-tenancy plus an existing tenant: a cluster from the stock
image has multi-tenancy switched off, and `Camunda8TenantCheck` ends the boot naming both
ways out, `use-prefix` (modules stay apart, no tenant needed) and `none` (version 1's
`use-tenants: false`). The default stood at `none` between 2026-08-11 and 2026-08-22, which
left an upgraded version-1 application deploying into no tenant while its workflows lived in
theirs. While `none` applies, a WARN per workflow module names the alternatives
until `accept-unscoped-identifiers` acknowledges that the identifiers are unique.

**Two adapter ids on one cluster.** Migrating a module from tenants to prefixes
runs both scopes side by side: two ids of type `camunda8`, one cluster, differing only in
the mode, the new one first in `prioritized-adapters`. What tells them apart is the scope a
workflow was deployed under, never the key of a task: job keys, user-task keys and
process-instance keys are unique per CLUSTER, and the credential of a migration is a member
of both tenants, so the cluster accepts an operation of the wrong adapter without a word.
The awareness probes therefore compare (tenant, scoped process definition id) against what
THIS adapter id deployed (`Camunda8DeployedProcesses`) before answering `ACTIVE`, and
`processInstanceKeyOf`, which `aggregateChanged` writes through, drops what is not its own.
The set comes from the deployment rather than from the call because one process service
serves every workflow module of its adapter id; an empty set (a module whose deployment
failed under the `warn` policy, a test) answers as before.

The workflow probes filter the result they already have, which is free. The two task probes
have to READ the job respectively the user task to learn its scope, so they do that only
where `Camunda8ClientFactoryRegistry` saw a second adapter id on the same cluster. That
read is a query-API call, which is why two ids on a cluster WITHOUT secondary storage fail
the boot: they cannot be told apart at all, and the alternative is silent misrouting.
`Camunda8WorkflowViewer` and `Camunda8ProcessVersions` were scope-correct from the start
and are what the probes now copy.

**The scope is the one of the CALL.** A probe is handed a
`WorkflowScope` naming the workflow module and the BPMN processes the asking process
service serves, so the comparison is not "one of my deployments" any more but "the module
and process you asked about", translated into the tenant and the scoped process definition
ids. That closes the second half of the gap: two workflow modules of one adapter id no
longer answer for each other, which mattered because aggregate ids are unique per
aggregate type and not across an application.

One case stays coarse on purpose. The two task probes only READ the job respectively the
user task where a second adapter id shares the cluster, because that read is a query-API
round trip on every task election. Without a second id the key of another workflow module
of the same application is still claimed, and it costs nothing: completing or cancelling
addresses that same key, so the operation acts on the task the key names, and a key of
another BPMS is not a Camunda 8 key at all. The workflow probes, whose answer routes a
message or a pushed aggregate, compare the scope always.

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

### Multi-instance

A `@WorkflowTask` method may ask what the engine knows about the iteration it runs in:
`@MultiInstanceElement`, `@MultiInstanceIndex` and `@MultiInstanceTotal`, each naming the
BPMN id of the multi-instance element it asks about. On Camunda 8 all three are answered,
and none of them can be read off a job directly.

What a job carries is the variable `loopCounter` and whatever `inputElement` names. Both are
local to the innermost iteration, so a task inside a multi-instance subprocess sees its own
values and none of the subprocess', and there is no `nrOfInstances` at all - this engine
does not report how many instances a multi-instance element has.

The adapter closes that while deploying, which is the stage it modifies models anyway. Every
multi-instance element of a deployed process gets input mappings named after that element:

```
vanillabpMiIndex_<element id>    = loopCounter
vanillabpMiTotal_<element id>    = count(<the element's input collection>)
vanillabpMiElement_<element id>  = <the element's input element>
```

Those names cannot be shadowed, so a job of a nested task carries one set per iteration it
runs in. Which iterations enclose which element is model knowledge and is remembered while
wiring, since a job reports the id of its own element only. The mappings are added once and
a redeployment produces the same model - the BPMN is not rewritten twice, and no new process
version comes out of an unchanged model.

An application which already ran its models on an earlier version of this adapter deploys a
NEW process version once, because the mappings are what changes the model. Workflows already
running stay on the version they were started on, and a task of such an instance reports no
iteration at all rather than a wrong one - the guiding message of the platform then names the
element it was asked about.

Two details of this engine are worth knowing when modelling:

- **There is no loop cardinality.** A multi-instance element always iterates over an
  `inputCollection`, and the collection is a process variable, so it should hold identifiers
  rather than objects: it travels to the cluster with every sync point, and the business code
  can look up the rest. `inputCollection="=partnerIds"` reads an attribute of the workflow
  aggregate like any other expression does.
- **The index counts from 0** in the application, as it does on every other BPMS, although
  Camunda 8 counts iterations from 1. The adapter translates.

Characters an element id may hold but a variable name may not are replaced by `_`. Two
multi-instance elements of one process whose ids differ only in such characters would end up
sharing variables, which fails the deployment with a message naming both.

A parallel multi-instance element creates one token per instance, and each of them loads and
saves the workflow aggregate. Two instances writing the same attribute means the one
committing last puts back what it read, so an iteration should write a row of its own - see
[workflow aggregates](https://github.com/vanillabp/adapter-platform-integration/wiki/Workflow-aggregates).

### Testing

- **Core unit tests** (no Docker): BPMN parsing / executable-process extraction, client
  configuration validation (missing-property messages, self-managed/SaaS), and the
  process-service phase behavior.
- **Spring Boot** `Camunda8DeploymentAndStartIT` (real Camunda 8 via Testcontainers,
  the cluster image of `ClusterUnderTest`, standalone broker without Elasticsearch): boots the
  application (deploying the BPMN to the cluster on startup) and drives the full two-phase
  start through `ProcessService#startWorkflow` inside a JPA transaction with the gruelbox
  outbox. It asserts that the process instance appears only **after** the transaction
  commits, carrying the aggregate's ID as the `id` variable (named after the test
  aggregate's ID property; observed by a raw Camunda 8 job worker on
  the service task), and **never** after a rollback (the outbox entry is gone and no job
  is ever activated). Skipped automatically when Docker is unavailable
  (`@Testcontainers(disabledWithoutDocker = true)`).
- **Spring Boot** `Camunda8WorkerThreadsIT` and `Camunda8VirtualThreadsIT` (real cluster): the
  acceptance test of the execution slots. A handler blocks its slot for four seconds while a
  workflow of ANOTHER worker of the same adapter is started, and its job has to be served
  meanwhile - which one execution thread could not do. The virtual variant asserts the same
  property plus that the handler really ran on a virtual thread and that the client runs its
  workers on the adapter's own bounded executor. The bound itself is a unit test
  (`Camunda8VirtualThreadExecutorTest`), where more concurrent jobs than the bound can be
  thrown at it without a cluster.
- **Spring Boot / Quarkus discovery tests:** the adapter is discovered and the deployment
  service (one per configured adapter id), process service and client-factory registry
  beans are created (no cluster needed).
- **Quarkus deployment-pipeline test** (`Camunda8DeploymentPipelineTest`, no Docker):
  the Quarkus platform integration runs the deployment pipeline at boot. The test provides
  a BPMN below the configured `resources-location` and a REST address pointing to a closed
  port: the pipeline reads/parses the BPMN and attempts the deployment, whose connection
  failure aborts the boot (the adapter is first-priority) - proving the pipeline mechanics
  without a cluster.
- **Quarkus** `Camunda8WorkflowLifecycleTest` (`quarkus/integration-tests`, real cluster):
  the same documented features the Spring Boot suite runs, on a booted application against
  a cluster with secondary storage. The duplication is deliberate - a correct
  platform-neutral core says nothing about a platform's glue ever calling it, which is why
  coverage is measured per platform. `QuarkusProdModeTest` runs the application in a forked
  JVM, so the tests observe it through its own `introspect/...` endpoints and the JaCoCo
  agent is forwarded into that JVM, otherwise the run would prove the features and count as
  nothing. One class carries all of it because a prod-mode test boots its application once
  per class and every boot costs a container pair. What it does NOT repeat is named in its
  class comment: the startup check for old process versions (several boots against one
  cluster), authentication and the shutdown drain (a cluster respectively a lifecycle of
  their own) and `cancelUserTask` (answered by the release line, so it belongs to a
  per-line test source).

## Decision log

Decisions this repository's code points at. A number is handed out once and never reused or
renumbered, so a citation stays resolvable; a decision which gets overturned keeps its entry,
marked as superseded and naming the entry which replaced it.

### 1. A command carries the shared aggregate values and the aggregate-ID variable, nothing else

Camunda 8 has no business key, so the variable named after the aggregate's ID attribute is
the only way back from a process instance to the workflow, and it is written no matter what
the sync model says. Beside it travel the values the aggregate shares, because a gateway right
behind a service task decides on what the handler just computed. Nothing else does: a
correlated message carries no content of its own.

The one command which carries NO variables at all is the completion of a user-task listener
job. It does not advance the process - the user task stays where it is - and writing there
would overwrite what a form or a task list put into the instance.

### 2. Workflow modules are kept apart by scoping the identifiers

The cluster is always addressed with the SCOPED identifiers - process ids, message and signal
names, error codes and task definitions - while the core's registries stay keyed by the plain
ones, and everything coming back from the cluster is translated before the core sees it. Which
shape scoping takes is the workflow module's configuration: a tenant, a prefix, or nothing at
all, so no code may assume either. Two processes must not end up under the same scoped
identifier, which is what the collision check while preparing a model is for.
See [Keeping workflow modules apart](#keeping-workflow-modules-apart).

### 3. A cluster key never says which scope it belongs to

Job keys, user-task keys and process-instance keys are unique per CLUSTER and carry neither
tenant nor prefix. Where two adapter ids address one cluster - the setup which migrates a
workflow module from tenants to prefixes - a credential which is a member of both scopes gets
an operation of the wrong adapter accepted without a word. Ownership is therefore decided by
the scope a workflow was deployed under, never by a key: the awareness probes compare tenant
and scoped process definition id against the scope of the CALL before they answer, and the two
task probes read the job respectively the user task to learn its scope. That read is a
query-API call, which is why two ids on a cluster without secondary storage end the boot
rather than misrouting silently.

### 4. A class opens its fields one by one, not as a whole

The process service, the deployment service and the client classes of this adapter hold dozens
of fields, most of them collaborators nobody outside the class needs. Which of them a caller
may read belongs to the surface of the class, so an accessor is declared per field, and
`@Getter` on the class is refused even where an IDE offers it: it would publish the current
field list and then keep publishing whatever field a later change adds.
`@SuppressWarnings("LombokGetterMayBeUsed")` on such a class is what keeps that offer from
coming back.

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
probe cannot: a workflow started moments ago is not searchable yet, and reporting
`UNKNOWN_TO_BPMS` would make the core raise `WorkflowNotFoundException` with causes that all
do not apply.

The adapter reports a window instead
(`workflowVisibilityDelay()`, configured as
`vanillabp.adapters.<id>.workflow-visibility-timeout`, default 10 seconds, zero switches it
off), and the core keeps asking for that long - but only while probing an adapter its
`WorkflowAdapterCache` names for that workflow, which VanillaBP fills after phase two of a
start and on every inbound delivery. A workflow nobody ever started has no such hint and
still fails immediately.

The residual: an application on several nodes without a SHARED adapter cache. An operation
reaching a node which neither started the workflow nor received a delivery for it knows
nothing about where the workflow lives, so it does not wait. Retrying the business operation
works, and an application bean implementing `WorkflowAdapterCache` removes the case
altogether. The alternative - asking the phase-two outbox whether a start for this aggregate
is open or was just dispatched - was weighed and dropped; the reasoning is in
[`migration-adapter/README.md`](https://github.com/vanillabp/adapter-platform-integration/blob/main/migration-adapter/README.md).

### Cancel user task

No Camunda 8 cluster up to 8.9 offers a command to cancel a Camunda-managed user task by
BPMN error: *throw error* is job-based, and a user task is not a job. Version 1's
marker-variable workaround is broken by Version 1's own admission, so `cancelUserTask`
throws a guiding error naming the [release line](#release-lines) rather than pretending to
work. The task listeners it needs arrive with Camunda 8.10, so support for it can only ever
come on a line built against 8.10 or later.

### Task cancellation is not reported

`@TaskEvent CANCELED` cannot be delivered for service tasks, because Zeebe does not notify
workers about canceled jobs, so a handler subscribing to lifecycle events never learns that
an open asynchronous task's activity was canceled. The event type `canceling` landed in
8.10.0-alpha2, so this belongs to a line built against 8.10 or later; the prepared follow-up
verifies it against a cluster before anything is reported.

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

### Multi-instance has no loop cardinality

Camunda 8 iterates a multi-instance element over an `inputCollection` and offers no
cardinality, so a model saying "run this five times" has to hand over a collection of five
elements. The count of the instances is not reported by the engine either; the adapter
derives it from the collection while deploying, see [Multi-instance](#multi-instance).
Nothing announced.

### Client certificates for the cluster connection

The Camunda Java client cannot send one. On 8.8.35, 8.9.16 and 8.10.0-alpha4 alike,
`CamundaClientBuilder` has `caCertificatePath` and `overrideAuthority` and nothing else about
TLS material, `ClientProperties` lists no keystore, and the environment variables the client
reads carry none either. The keystore and truststore of `CredentialsProvider`'s OAuth builder
apply to the token request against the identity provider, not to gRPC or REST against the
gateway, and the `auth` block documents them that way. A cluster which demands a client
certificate is therefore out of reach until the client grows the option, and the adapter says
so rather than offering a property which would silently do something else.

### Message deduplication lasts for the message TTL

A correlation carrying a correlation id deduplicates engine-side, because the outbox
idempotency key travels as the Zeebe message id, and the engine remembers a message id for
the message TTL only. A redelivery after the TTL could correlate a second time. Without a
correlation id there is no deduplication at all, on purpose: the same message may legitimately
arrive several times over a workflow's lifetime.

## What an operator gets to see

The platform integration measures every task delivery, every outbox dispatch and puts a
logging context around both; the [Observability wiki
page](https://github.com/vanillabp/adapter-platform-integration/wiki/Observability)
describes all of it. What this adapter adds is documented in the
[Configuration wiki page](https://github.com/camunda-community-hub/vanillabp-camunda8-adapter/wiki/Configuration),
section "What an operator gets to see": the client's own job counters bridged into the
same registry, the execution slots as gauges, and a health contribution
asking the cluster for its topology.

The reasoning behind the shape of it - why the client's Micrometer implementation is not
used, why the health check has a timeout of its own and why the slot gauges are absent in
the platform-thread mode - is in [`core/README.md`](./core/README.md).

## Camunda 8 client

The adapter uses the plain Java client `io.camunda:camunda-client-java`, pinned per
[release line](#release-lines), **not**
Camunda's Spring SDK / Spring Zeebe: VanillaBP does platform wiring and configuration
itself, so a client that carries its own platform integration would conflict with it.
(The deprecated `io.camunda:zeebe-client-java` is deliberately avoided.)

Camunda 8 is a remote, eventually consistent engine that cannot join the application's
local database transaction. Starting a workflow therefore uses VanillaBP's two-phase
commit (`needsTwoPhaseCommitForStartingWorkflows() == true`): phase one only validates,
the actual process-instance creation runs in phase two through the core phase-two
outbox.

## Native images

The Quarkus extension builds into a native image, and an application needs no
configuration of its own for it:

```bash
mvn package -Dquarkus.native.enabled=true -Dquarkus.native.container-build=true
```

What that costs is registrations, and they belong here rather than into every
application, because the path they sit on is the DEPLOYMENT: every boot of every
application reads each BPMN file through the Camunda model API, modifies it, serializes
it back, sends it to the cluster and builds the Camunda client on the way.
`Camunda8NativeImageProcessor` names them:

- the message bundles of the JDK's XML parser, which the model API validates against.
  The first thing that parser wanted to say - where it had looked for the schema - came
  out as a `MissingResourceException`;
- `BPMN20.xsd` and the four schemas it imports, resources of the model API's jar;
- the model API's entry point, initialized at run time rather than at build time: it
  builds its parser in a static initializer and keeps the URL the schema was found
  under, which at build time points into the builder container;
- the gRPC providers behind the service loader, and the client's implementation package,
  whose job workers seed a `Random`;
- the types of the cluster's REST API, which Jackson builds by reflection. Taken from the
  Jandex index of the client rather than written down type by type.

Netty and Apache HttpClient 5 come along with the client and needed the same kind of
answer, one level lower:

- the runtime module depends on Quarkus' own **Netty extension** instead of repeating what
  it registers. An application which brings a Netty-using extension anyway - a REST layer,
  for instance - had that by accident, which is why the gap stayed invisible for so long;
- three GraalVM substitutions in `io.vanillabp.camunda8.quarkus.runtime.graal` answer the
  HTTP client's questions for optional libraries nobody put on the classpath: Conscrypt,
  zstd and brotli4j. Their absence is what a native build reports as `Discovered
  unresolved type during parsing`, because it resolves every type a reachable method
  names. The price is one line in the wiki's deviations: a native image accepts gzip and
  deflate responses, and adding one of those libraries to the application does not change
  that.

Netty's version here is the Quarkus platform's, and that took a build to notice. This
repository imports the Spring Boot BOM before the Quarkus one, so every module sees Spring
Boot's newer Netty - right for the Spring Boot modules, wrong for a module reproducing what
a Quarkus application sees, because Quarkus' Netty substitutions do not match it (`Could
not find target method: Target_io_netty_handler_ssl_JdkSslClientContext`, before the
analysis even starts). `quarkus/native-image-tests` pins `netty-bom` to
`netty.version.quarkus` of the parent POM, which follows the Quarkus version and never
leads it.

Held by `quarkus/native-image-tests` and the `native-build` job of the publishing
workflow: the module builds an image AND runs the binary against a real cluster, where it
deploys its workflow module, starts a workflow through the phase-two outbox and has the
job served in a handler of its own. Both halves are needed, and the demo which found all
this is why: its image built in about a minute and then stopped at the first BPMN file it
read. Measured on 2026-08-25 with Mandrel 25.0.4 on the 8.9 line: 1m30s for the image,
101 MB of binary, on twelve cores.

## Building

Prerequisites (built and installed into the local Maven repository first, in this
order): `spi-for-java`, then `adapter-platform-integration`. Then:

```bash
mvn install verify
```

That is the current GA line as `2.0.0-SNAPSHOT`, no property to remember. Another line is
a profile, another version a `-Drevision`, and `bin/api-identity.sh` compares the public
API of the lines; see [Release lines](#release-lines).

## Test coverage

`mvn install verify` builds one aggregated JaCoCo report per platform:

1. **Spring Boot** (core + Spring Boot integration) - into `test-coverage-report/spring-boot/report`
2. **Quarkus** (core + Quarkus extension) - into `test-coverage-report/quarkus/report`

Both are published to GitHub Pages by the *Publish to GitHub Packages* workflow on every push to
the default branch. Click the [platform's badge](#documentation-and-supported-platforms) to open
the respective report.

The build breaks below the line: `test-coverage-report/coverage-gate` is the last module of the
reactor, reads both reports and fails whenever a platform is below its threshold in the root POM
(`coverage.threshold.spring-boot`, `coverage.threshold.quarkus`, in percent of covered instructions -
the number the badges above show). It also compares every module producing a `jacoco.exec` against
the two aggregates, so a module added to the build without being added to its report cannot stay
unnoticed.

Both platforms run the documented features end to end against a real cluster: Spring Boot in the
`spring-boot` module's `*IT` classes, Quarkus in `quarkus/integration-tests`. That duplication is
deliberate. The adapter core is platform-neutral, but a core being correct says nothing about a
platform's glue ever calling it, so a core line one platform never reaches names a feature that
platform never runs.

The two thresholds still differ, by what one suite can produce and the other cannot. The startup
check for old process versions needs several boots against one cluster, each deploying a
different model, and a Quarkus prod-mode test boots its application once per test class - which is
why `Camunda8ProcessVersions` stands at 27 % on Quarkus against 79 % on Spring Boot. The other half
is the cluster itself: the Quarkus suite runs against one WITH secondary storage, so what the adapter
answers optimistically without it is covered on Spring Boot only, and `cancelUserTask` is answered by
the release line, which belongs to a per-line test source rather than to a prod-mode test. The
Quarkus suite's class comment lists all of it. Everything else is at parity, `Camunda8DeploymentService`
above the Spring Boot number.

## Noteworthy & Contributors

[VanillaBP](https://www.github.com/vanillabp/spi-for-java) was developed by [Phactum](https://www.phactum.at) with the
intention of giving back to the community as it has benefited the community in the past.

![Phactum](./readme/phactum.png)

## License

Copyright 2026 Phactum Softwareentwicklung GmbH

Licensed under the Apache License, Version 2.0
