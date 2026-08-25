# Decision log

Decisions this repository's code points at. A number is handed out once and never reused or
renumbered, so a citation stays resolvable; a decision which gets overturned keeps its entry,
marked as superseded and naming the entry which replaced it.

A citation in code reads `see decision 3 in the repository's DECISIONS.md`, and it names an entry
of THIS repository only. A decision which the platform shares has its own entry in
`adapter-platform-integration`, written from that side; a pointer into another repository is the
fragile kind this log exists to avoid.

Links below point into this repository's [`README.md`](./README.md), which carries the detail an
entry deliberately leaves out.

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
See [Keeping workflow modules apart](./README.md#keeping-workflow-modules-apart).

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

### 5. The deployed model is rewritten so the cluster can do what an embedded engine does for free

Camunda 8 runs remote and answers only what its protocol carries, so several things an embedded
engine offers as a side effect have to be put INTO the model before it reaches the cluster.
`prepareBpmn` and `wireBpmn` therefore add the scoped identifiers of decision 2, the user-task
listeners whose jobs become the CREATED and CANCELED notifications, a `correlationKey` expression
on every message subscription which has none so a catch event correlates by the aggregate id
without the application modelling anything, an `end` execution listener on the process element for
the end of a workflow and on a start event for a workflow the cluster starts itself, and the input
mappings which make the multi-instance index, total and element readable at all.

Two rules keep that predictable. Every addition is idempotent, so re-wiring the same model does
not stack listeners, and nothing the application modelled itself is overwritten. The price is
stated rather than hidden: a model which the adapter rewrites is a new process version in the
cluster, so an application with multi-instance models deploys one on the upgrade.

### 6. A restart waits for the cluster to let go of the workers

`JobWorker#close()` does not drain, and `CamundaClient#close()` interrupts a running handler
within milliseconds. Worse, an activation request of the REST transport survives the client which
sent it: a job created while it is pending is assigned to it, counts as activated, and is answered
by nobody until its lock expires, so the application which starts next waits out `job-timeout`
rather than milliseconds. Measured against a real cluster, that turned a seven-second restart into
a twenty-second gap.

So `stopWorkflowProcessing` closes the workers of the module first and then waits, within
`shutdown-grace`, for two things: the handlers which are still inside the application, and the
cluster releasing the workers. The client factory closes whatever never reached that path before
it closes the client, so the order holds on every shutdown path and not only on the one the
platform lifecycles happen to take.

While that shutdown runs, no worker reports a job as failed. The job keeps its lock and its
retries, the cluster redelivers it, and the delivery record of the platform decides whether the
work runs again. What decides is the STATE of the adapter, never the type of the exception,
because an interrupted handler throws like any other.

### 7. The adapter chooses how many handlers run at once

The client's default is a single thread, and on the 8.8 line that same thread also schedules the
polling of every worker, so one blocking handler stopped the adapter from asking for work at all.
`worker-threads` therefore has a default of its own, four platform threads, and accepts `virtual`
for an unbounded number of virtual threads whose concurrency `worker-threads-bound` limits to the
same figure. Switching the mode changes how threads come into being, not how much runs at once.

Four rather than one because of the defect, and small rather than large because every running
handler holds a database connection. Virtual threads are a regular mode rather than a caveat: a
pinning probe over six driver and transaction-manager combinations measured no pinning at all.

Everything except the stream timeout is configured on the CLIENT rather than per worker, so an
environment variable can still win, and `Camunda8EnvironmentOverrides` compares what the adapter
asked for with what the client reports and warns with variable, property key and both values.

### 8. A worker fetches only the variables somebody actually reads

Nothing asked for `fetchVariables`, so every activated job carried the complete variable scope of
its instance, which with a `FULL` sync model means every shared attribute of an aggregate the
handler is about to load from its own database anyway.

The list is derived per WORKER, as the union over everything that worker serves, and sorted,
because job streaming compares the list and the comparison has to survive a restart. In it are the
aggregate-id variable of each served process, the multi-instance variables of the iterations
around the served element, and the union of the `@TaskParam` names the core reports for that
element. The core is the source for the last part rather than a scan of the model, because a model
declares names nobody reads and misses names no model carries.

A `@TaskParam` outside the list fails the delivery with the variable, the list and the property
instead of arriving as `null`. A `null` would be indistinguishable from a value which genuinely
does not exist, which is the silent loss this avoids. `fetch-variables: all` is the way out, per
task if need be.

### 9. What a job command may repeat, and how long it may keep trying

A cluster under load rejects commands, and the client retries none of them. Phase two is carried
by the outbox, but the command inside a handler is not: a rejected completion of already committed
work costs the job a retry and, under sustained load, produces an incident.

So completion, BPMN error, failure and lock renewal of all four worker kinds run inside
`Camunda8CommandRetry`. What may be repeated is what `Camunda8Errors` classifies as repeatable,
plus the explicit exception that a job which is gone is final, or the at-least-once residual would
turn into a storm. What bounds it is the job's REMAINING lock, taken from the activated job rather
than from the configured timeout, five attempts with the client's own backoff figures, and the
shutdown of decision 6, which ends the retry at once so the job stays on its lock instead of being
reported.

### 10. Authentication belongs to the connection, not to a workflow

`vanillabp.adapters.<id>.auth.*` exists only at adapter level, because a credential is a property
of the connection. The method is named or detected from the keys which are set, and the detection
is printed next to the address at startup so nobody has to guess which one applies.

The runtime message hangs on `CredentialsProvider.shouldRetryRequest` rather than on the error
classification. The client calls it for every rejected request, on both transports, for commands
as well as for job activation, which makes it the one place where an adapter learns that it is
unwelcome. Going through the error classification would have meant two dozen call sites and would
still have missed the workers.

`mtls` is deliberately not a key. The client has no keystore for its own connection to the
cluster on any of the supported lines, only for the token request, and a property which quietly
configures something other than what its name says is worse than no property.
See [Authenticating against a cluster](./README.md#authenticating-against-a-cluster).

### 11. The client an artifact was built against is the minimum cluster version

Camunda does not promise that a newer client works against an older cluster, and it has been
measured that it does not. So a single artifact per adapter version would mean that every bugfix
becomes deliverable only together with a cluster upgrade, as soon as anything in the code touches
what only the newest cluster has.

The adapter is therefore released once per Camunda minor, with the minor in the version
(`2.1.0-8.9`), built from ONE source tree through the `line-8.8`, `line-8.9` and `line-8.10`
profiles. A fix exists on every line with the same commit. Line-specific source folders exist for
the rare real difference and stay empty otherwise, and the API identity check keeps the public
surface identical across lines. See [Release lines](./README.md#release-lines).
