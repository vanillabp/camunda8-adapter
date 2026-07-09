# Camunda 8 adapter - core

Contributor documentation for the platform-neutral core of the VanillaBP Camunda 8
adapter. (User-facing documentation is in the repository-root `README.md`.)

The `core` module is **plain Java** - no Spring or Quarkus dependencies. It holds the
adapter SPI implementations and (from later stories on) all Camunda 8 client logic. The
platform modules (`spring-boot`, `quarkus`) only construct and register these core
objects.

## Adapter SPI implementations

- `Camunda8DeploymentService implements AdapterDeploymentService<BpmnModelInstance,
  Camunda8ProcessingContext>` - one instance per configured adapter ID (not per type;
  the same BPMS type may be configured several times for BPMS migration). The adapter
  type is the constant `"camunda8"` (`Camunda8DeploymentService.ADAPTER_TYPE`).
- `Camunda8ProcessService<A> implements MigratableProcessService<A>` -
  `needsTwoPhaseCommitForStartingWorkflows()` returns `true` because Camunda 8 is a
  remote engine (starts are routed through the core phase-two outbox).
- `Camunda8ProcessingContext` - the adapter-specific processing context threaded through
  the deployment pipeline; currently carries only the workflow-module ID.

Skeleton stage: methods not implemented yet throw
`UnsupportedOperationException("<method> is implemented in a later story")` - never a
silent no-op, so wiring bugs surface loudly in later stories. **No `CamundaClient` is
constructed anywhere** in the skeleton, so applications boot without a reachable
cluster.

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
