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

### 12. A task id is decimal, and version 1's hexadecimal ids are a data migration

VanillaBP 1 could hand out a task key in hexadecimal (`task-id-as-hex-string`, off by default), and an
application which switched it on stored those ids in its own data. They outlive an upgrade, while this
version parses decimally everywhere and has no such setting.

Offering the setting again was rejected. One representation of a task id is simpler than two, the second
one would have to be carried forever because nobody can prove it unused, and an application which holds
hexadecimal ids has a conversion to do either way - the ids are in ITS tables, not in ours. What the
adapter owes such an application is the sentence which names the setting, and that is what the parse site
says now. The failure stays a permanent one, so the outbox entry is still blocked after a single attempt
rather than retried ten times against a key which will not become a number.

### 13. A start asks the cluster for numbers, and asks as many of them on the last day as on the first

The questions this adapter answers while an application boots read from secondary storage, which
grows for as long as the application is in production: how many workflows still run on an old
version, how many jobs of version 1's user-task construction are still open, how many tasks the
cluster is holding open for a process. A start of ten seconds must not become a start of two
minutes because the application did its job for two years, and the platform states the rule for
every adapter as decision 19 of its own DECISIONS.md.

For Camunda 8 that means two things. A question about a quantity fetches one item and reads
`page().totalItems()`; the page which came back is not the answer, and transferring it to count it
would grow with the data while also capping the number at the page size. And a process definition
is searched for once for the whole process: `fetchDeployedVersions` reads every version anyway, so
it keeps the definition keys it saw, and the questions which follow are addressed with them rather
than each searching again.

What does grow is the number of versions the cluster holds, one per deployment which changed a
model, and the questions about older versions grow with it. That is deliberate: those questions are
what the check is for, and `outfaded-versions` is how an operator says which of them have stopped
being interesting. `Camunda8StartupQuestionCostTest` counts what a start asks.

### 14. What this adapter does per operation is a handler, not a pair of methods

VanillaBP's adapter SPI used to ask for two methods per outbound operation, and this adapter
had nineteen of them - the eighteen halves plus the seven-argument correlation overload which
carried the activation id past a default. It answers a map now: one `PhaseOperationHandler` per
`PhaseOperation`, and the request of a phase carries the operation's arguments behind named
accessors, the activation among them. The overload is gone with the pair it belonged to.

What the handlers do is unchanged. The same preflight commands run as pre-commit hooks, the
same job and user-task commands run after it, and the message id which lets the cluster
deduplicate a correlation is derived from the same values as before, activation included. Only
the shape moved.

The map is what states which operations this adapter serves. Everything a Camunda 8 cluster
cannot answer without a round trip - and there is more of that here than on an embedded engine -
stays where it was: in the handler, not in the operation, because the operation is the same one
every adapter serves.

### 15. The adapter sees process definitions in the state ACTIVE and no others

Deleting a process definition does not remove it from Camunda 8. The cluster keeps it, marks it
`DELETED` and keeps answering searches with it, so a search which names no state gets the deleted
versions back along with the live ones. The startup check for old versions then reads their models,
finds the tasks this application no longer serves and reports them, at every start, and the one
remedy the report itself suggests is the one the operator has already applied. A report which
cannot be switched off teaches everyone to ignore reports of its kind, which is worse than not
having it.

Every search this adapter runs for process definitions therefore restricts itself to the state
`ACTIVE`, in one place (`Camunda8ProcessVersions#onlyDefinitionsWhichStillCount`), so the answer to
"which definitions count" cannot drift apart between the callers. There is deliberately no property
turning it off: a deleted definition is not a state anybody wants to hear about.

The filter arrived in `camunda-client-java` 8.8.33, and by decision 11 the client an artifact was
built against is the lowest cluster version it accepts, so every supported line has it. A fallback
path for clusters without the filter would therefore be dead code and is not to be added.

### 16. What the cluster did is read from its codes, not from the words around them

Two answers of the cluster change what this adapter does with an operation: a publication
refused because a message of that id still lives, and a query endpoint refused because this
cluster cannot be searched. Both used to be recognised by looking for a phrase in the
exception's message, and every one of those phrases is the cluster's to reword in any patch
release. A rewording would have turned a harmless duplicate into an outbox entry which is
repeated and then blocked, and it would have turned "this cluster cannot tell" into "this
cluster is down", after which every operation of the adapter fails after a second instead of
proceeding.

Every classification therefore reads a code. A publication the cluster already knows is HTTP
`409` on REST and the status `ALREADY_EXISTS` on gRPC, a job which is gone is `404`
respectively `NOT_FOUND`, and both transports matter because `prefer-rest-over-grpc` decides
per adapter id which one carries a command.

The query API is the case where a code does not suffice: a cluster refuses a search with HTTP
`403` whether it holds no secondary storage or whether the adapter's credentials are not
allowed to read, and it separates the two in prose only. So the question is not asked per
failure at all. The adapter asks it once, while it starts processing a workflow module, with a
search whose only purpose is that answer, and remembers it per adapter id
(`Camunda8QueryApi`). Every later failure of a search is read against the remembered answer:
on a cluster which can be searched it is an outage and the probe reports `BPMS_UNAVAILABLE`,
on a cluster which refuses it is the missing capability and the probe answers optimistically.
Both reasons for a refusal are permanent and cost the adapter the same thing, so the messages
naming this state name both rather than guessing which one it was.

One place keeps reading a wording, and it decides nothing the adapter does: a tenant request
which fails because the cluster has multi-tenancy switched off is answered with the same
`400` as any other rejected argument, so `Camunda8TenantCheck` picks the sharper of two
guiding messages by what the cluster wrote. A rewording costs the sharper sentence there and
nothing else.

### 17. A start waits once for its cluster instead of repeating each round

The commonest reason a start cannot reach its cluster is a cluster booting alongside the
application, and that lets every round the start makes fail, not only the deployment: the tenant
check, the deploy command, the question whether the cluster can be searched, and the version
queries of the startup check. A retry around one of them would have covered a quarter of the
cases, and it would have been a third repetition mechanic next to `Camunda8CommandRetry` and the
outbox.

So nothing is repeated. Before the first round which decides anything the adapter asks the
cluster for its topology, the cheapest question a Camunda 8 cluster answers, and waits while the
answer does not come - once per adapter instance, because what is waited for is the cluster and
not the workflow module. Everything behind the wait keeps ending the start the way it always did:
a cluster which breaks down in the middle of a deployment is not booting, it is failing.

Three things end the wait, whichever comes first. The cluster answers, which costs one request.
`vanillabp.adapters.<id>.startup-wait` is used up, and the start ends naming the address, the time
waited and the cluster's last answer. Or the cluster answers something `Camunda8Errors` classifies
as permanent, and the start ends at once - one classification, the same one everything else in
this adapter reads, and it is what makes a default of ten minutes bearable. The default is long
because the case it exists for takes minutes; it is paid for with a late abort and not with a late
diagnosis, since a line every few seconds carries the cluster's last answer from the first attempt
on.
