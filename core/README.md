# Camunda 8 adapter - core

Contributor documentation for the platform-neutral core of the VanillaBP Camunda 8
adapter. (User-facing documentation is in the repository-root `README.md`.)

The `core` module is **plain Java** - no Spring or Quarkus dependencies. It holds the
adapter SPI implementations and all Camunda 8 client logic. The platform modules
(`spring-boot`, `quarkus`) only construct and register these core objects (read
configuration, create beans, run the bean lifecycle).

## Client construction

- `Camunda8AdapterConfiguration` - resolved, platform-neutral connection configuration of
  one adapter instance (mode, REST/gRPC address, SaaS credentials, tenant). Populated by
  the platform modules from `camunda8-adapter.<adapter-id>.*` (see the root `README.md`).
  Validated lazily; `validate(adapterId)` throws naming the exact missing property.
- `Camunda8ClientFactory` - owns the single `CamundaClient` of one adapter instance, built
  **lazily on first use** and closed on `close()`. Building never contacts the cluster
  (that happens on the first command). `newClientBuilder()` is used for self-managed,
  `newCloudClientBuilder()` for SaaS.
- `Camunda8ClientFactoryRegistry` - map adapter ID &rarr; factory, registered as a managed
  bean so all clients are closed on shutdown.

## Adapter SPI implementations

- `Camunda8DeploymentService implements AdapterDeploymentService<BpmnModelInstance,
  Camunda8ProcessingContext>` - one instance per configured adapter ID (not per type).
  `readBpmn` parses with `io.camunda.zeebe.model.bpmn.Bpmn.readModelFromStream` and returns
  one entry per executable `<process>`; `prepareBpmn` accumulates the deployable resources
  (deduplicated per filename) into the context; `deployResources` sends one
  `DeployResourceCommand` per workflow module (configured tenant or default). `wireBpmn`
  and `startWorkflowProcessing` only log (task wiring / job workers are later stories).
- `Camunda8ProcessService<A> implements MigratableProcessService<A>` -
  `needsTwoPhaseCommitForStartingWorkflows()` returns `true`. Phase one validates only
  (resolve aggregate ID, verify client configured; no cluster call). Phase two creates the
  instance via `createProcessInstance(bpmnProcessId, aggregateId)` (latest version, single
  `aggregateId` variable).
- `Camunda8ProcessingContext` - the adapter-specific processing context threaded through
  the deployment pipeline: workflow-module ID, deployable resources (per filename) and the
  discovered BPMN process IDs.

Methods of features not implemented yet (awareness/election, etc.) throw
`UnsupportedOperationException("<method> is implemented in a later story")` - never a
silent no-op, so wiring bugs surface loudly.

## BPMN model type

The BPMN model type is `io.camunda.zeebe.model.bpmn.BpmnModelInstance`, shipped in the
artifact `io.camunda:zeebe-bpmn-model`. Against the resolved Camunda 8 client
`io.camunda:camunda-client-java:8.8.31`, `zeebe-bpmn-model:8.8.31` is a transitive
dependency and the class/artifact are **unchanged** from Camunda 7-era Zeebe (no rename
in 8.8). The skeleton only needs `getModelType()` to return this class; BPMN parsing is
a later story.

## Client artifact

The core depends on `io.camunda:camunda-client-java` (which brings `zeebe-bpmn-model`
transitively). The plain Java client is used deliberately instead of Camunda's Spring
SDK - see the root `README.md`.
