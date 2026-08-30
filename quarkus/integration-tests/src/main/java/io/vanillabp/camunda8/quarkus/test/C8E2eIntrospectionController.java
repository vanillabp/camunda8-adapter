package io.vanillabp.camunda8.quarkus.test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import io.camunda.client.CamundaClient;
import io.vanillabp.camunda8.client.Camunda8ClientFactoryRegistry;
import io.vanillabp.camunda8.quarkus.runtime.VanillaBpCamunda8Properties;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.transaction.UserTransaction;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

/**
 * What the Quarkus end-to-end tests can see of the running application: a handful of
 * <code>introspect/...</code> endpoints reporting the aggregates, the cluster's state
 * and the adapter's answers, and triggering the {@code ProcessService} operations the
 * tests want to observe.
 * <p>
 * A prod-mode test runs the application in a forked JVM, so nothing of it can be
 * injected into the test - everything travels through these endpoints. The ones
 * driving VanillaBP open their transaction themselves
 * ({@link UserTransaction}), because what happens inside the
 * caller's transaction and what only after its commit is half of what the tests are
 * about.
 */
@Path("/introspect")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class C8E2eIntrospectionController {

  private static final String ADAPTER_ID = "c8";

  static final String MODULE_ID = "c8-e2e";

  /**
   * The adapter runs with name-clash-avoidance 'use-prefix' here, so the CLUSTER knows
   * every process under its prefixed id.
   */
  static String scoped(
      final String bpmnProcessId) {

    return MODULE_ID
        + "__"
        + bpmnProcessId;

  }

  @Inject
  Camunda8ClientFactoryRegistry clientFactoryRegistry;

  @Inject
  C8E2eWorkflowService workflowService;

  @Inject
  C8PushWorkflowService pushWorkflowService;

  @Inject
  VanillaBpCamunda8Properties overlay;

  @Inject
  EntityManager entityManager;

  @Inject
  UserTransaction userTransaction;

  private CamundaClient client() {

    return clientFactoryRegistry
        .getFactory(ADAPTER_ID)
        .getClient();

  }

  // --- the application's own state ---

  @GET
  @Path("/workflow-module")
  @Produces(MediaType.TEXT_PLAIN)
  public String workflowModule() {

    return workflowService.getWorkflowModuleId();

  }

  @GET
  @Path("/aggregates/{id}")
  @Transactional
  public Map<String, Object> aggregate(
      @PathParam("id") final Long id) {

    final var aggregate = entityManager.find(C8E2eAggregate.class, id);
    final var state = new LinkedHashMap<String, Object>();
    state.put("exists", aggregate != null);
    if (aggregate != null) {
      state.put("results", aggregate.getResults());
      state.put("taskId", aggregate.getTaskId());
      state.put("approved", aggregate.isApproved());
      state.put("flat", aggregate.getFlat());
      state.put("nested", aggregate.getNested());
    }
    return state;

  }

  @GET
  @Path("/push-aggregates/{id}")
  @Transactional
  public Map<String, Object> pushAggregate(
      @PathParam("id") final Long id) {

    final var aggregate = entityManager.find(C8PushAggregate.class, id);
    final var state = new LinkedHashMap<String, Object>();
    state.put("exists", aggregate != null);
    if (aggregate != null) {
      state.put("note", aggregate.getNote());
      state.put("taskIds", aggregate.getTaskIds());
    }
    return state;

  }

  /**
   * The aggregates of the workflows the CLUSTER started on its own - the
   * application never creates one of them.
   *
   * @return One "id|processedBy|endedAs" per aggregate
   */
  @GET
  @Path("/timer-aggregates")
  @Transactional
  public List<String> timerAggregates() {

    return entityManager
        .createQuery("select a from C8TimerAggregate a", C8TimerAggregate.class)
        .getResultList()
        .stream()
        .map(aggregate -> "%s|%s|%s".formatted(aggregate.getId(), aggregate.getProcessedBy(), aggregate.getEndedAs()))
        .toList();

  }

  /**
   * @param taskDefinition The task definition
   * @param aggregateId The aggregate
   * @return How often the cluster delivered that task
   */
  @GET
  @Path("/invocations/{taskDefinition}/{aggregateId}")
  @Produces(MediaType.TEXT_PLAIN)
  public int invocations(
      @PathParam("taskDefinition") final String taskDefinition,
      @PathParam("aggregateId") final String aggregateId) {

    return C8E2eWorkflowService.invocations(taskDefinition, aggregateId);

  }

  /**
   * @param taskDefinition The task definition
   * @param aggregateId The aggregate
   * @return The milliseconds between the first two deliveries, or -1
   */
  @GET
  @Path("/delivery-gap/{taskDefinition}/{aggregateId}")
  @Produces(MediaType.TEXT_PLAIN)
  public long deliveryGap(
      @PathParam("taskDefinition") final String taskDefinition,
      @PathParam("aggregateId") final String aggregateId) {

    return C8E2eWorkflowService.deliveryGap(taskDefinition, aggregateId);

  }

  /**
   * What the cluster handed a task as variables - the cluster's view of the aggregate.
   *
   * @return The observed variables
   */
  @GET
  @Path("/observed-variables")
  public Map<String, String> observedVariables() {

    return Map.copyOf(C8E2eWorkflowService.OBSERVED_VARIABLES);

  }

  /**
   * The job timeout and the retry backoff the adapter's overlay resolves for a scope -
   * the four levels of the configuration model, read from the real configuration.
   *
   * @param workflowModuleId The workflow module
   * @param bpmnProcessId The BPMN process
   * @param taskDefinition The task
   * @return The resolved durations
   */
  @GET
  @Path("/config/{workflowModuleId}/{bpmnProcessId}/{taskDefinition}")
  public Map<String, String> resolvedConfiguration(
      @PathParam("workflowModuleId") final String workflowModuleId,
      @PathParam("bpmnProcessId") final String bpmnProcessId,
      @PathParam("taskDefinition") final String taskDefinition) {

    return Map
        .of(
            "jobTimeout",
            overlay.jobTimeoutFor(workflowModuleId, bpmnProcessId, taskDefinition, ADAPTER_ID).toString(),
            "retryBackoff",
            overlay.retryBackoffFor(workflowModuleId, bpmnProcessId, taskDefinition, ADAPTER_ID).toString());

  }

  // --- starting workflows ---

  /**
   * Saves an aggregate WITHOUT starting a workflow - what an operation addressing a
   * workflow nobody started has to answer about.
   *
   * @return The aggregate's id
   */
  @POST
  @Path("/aggregates")
  @Transactional
  public Map<String, Object> seedAggregate() {

    final var aggregate = new C8E2eAggregate();
    entityManager.persist(aggregate);
    entityManager.flush();
    return Map.of("id", String.valueOf(aggregate.getId()));

  }

  /**
   * Starts the primary process through the VanillaBP user API.
   *
   * @return The aggregate's id
   */
  @POST
  @Path("/workflows")
  public Map<String, Object> startWorkflow() throws Exception {

    return startWorkflow(true);

  }

  /**
   * Starts the primary process and rolls the transaction back: neither the aggregate
   * nor the workflow may survive, because the instance is created by the phase-two
   * outbox and that outbox record is written in the caller's transaction.
   *
   * @return The aggregate's id
   */
  @POST
  @Path("/workflows/rollback")
  public Map<String, Object> startWorkflowAndRollback() throws Exception {

    return startWorkflow(false);

  }

  private Map<String, Object> startWorkflow(
      final boolean commit) throws Exception {

    userTransaction.begin();
    final String id;
    try {
      id = String.valueOf(workflowService.startWorkflow(new C8E2eAggregate()).getId());
    } catch (final Exception e) {
      userTransaction.rollback();
      return failure(e);
    }
    if (commit) {
      userTransaction.commit();
    } else {
      userTransaction.rollback();
    }
    return Map.of("id", id);

  }

  /**
   * Saves an aggregate and starts one of the workflow service's SECONDARY processes
   * against the cluster: the injectable process service starts the primary process
   * only. The aggregate-id variable is what VanillaBP finds the aggregate again by.
   *
   * @param bpmnProcessId The BPMN process to start
   * @param payloadSize The size of an additional variable no BPMN model mentions, or
   *          <code>null</code> for none
   * @return The aggregate's id and the instance key the cluster assigned
   */
  @POST
  @Path("/processes/{bpmnProcessId}")
  public Map<String, Object> startSecondaryProcess(
      @PathParam("bpmnProcessId") final String bpmnProcessId,
      @QueryParam("payloadSize") final Integer payloadSize) throws Exception {

    userTransaction.begin();
    final var aggregate = new C8E2eAggregate();
    entityManager.persist(aggregate);
    entityManager.flush();
    final var aggregateId = aggregate.getId();
    userTransaction.commit();

    final var variables = new LinkedHashMap<String, Object>();
    variables.put("id", String.valueOf(aggregateId));
    if (payloadSize != null) {
      // built here rather than sent in: a payload of that size does not fit into a URL
      variables.put("bigPayload", "x".repeat(payloadSize));
    }
    final var instanceKey = client()
        .newCreateInstanceCommand()
        .bpmnProcessId(scoped(bpmnProcessId))
        .latestVersion()
        .variables(variables)
        .send()
        .join()
        .getProcessInstanceKey();
    return Map.of("id", String.valueOf(aggregateId), "processInstanceKey", String.valueOf(instanceKey));

  }

  // --- what the application asks of VanillaBP ---

  @POST
  @Path("/tasks/{taskId}/complete/{aggregateId}")
  public Map<String, Object> completeTask(
      @PathParam("taskId") final String taskId,
      @PathParam("aggregateId") final Long aggregateId) throws Exception {

    return inTransaction(aggregateId, aggregate -> workflowService.completeTask(aggregate, taskId), true);

  }

  @POST
  @Path("/tasks/{taskId}/complete-and-rollback/{aggregateId}")
  public Map<String, Object> completeTaskAndRollback(
      @PathParam("taskId") final String taskId,
      @PathParam("aggregateId") final Long aggregateId) throws Exception {

    return inTransaction(aggregateId, aggregate -> workflowService.completeTask(aggregate, taskId), false);

  }

  @POST
  @Path("/tasks/{taskId}/cancel/{aggregateId}/{errorCode}")
  public Map<String, Object> cancelTask(
      @PathParam("taskId") final String taskId,
      @PathParam("aggregateId") final Long aggregateId,
      @PathParam("errorCode") final String errorCode) throws Exception {

    return inTransaction(aggregateId, aggregate -> workflowService.cancelTask(aggregate, taskId, errorCode), true);

  }

  @POST
  @Path("/user-tasks/{taskId}/complete/{aggregateId}")
  public Map<String, Object> completeUserTask(
      @PathParam("taskId") final String taskId,
      @PathParam("aggregateId") final Long aggregateId) throws Exception {

    return inTransaction(aggregateId, aggregate -> workflowService.completeUserTask(aggregate, taskId), true);

  }

  @POST
  @Path("/user-tasks/{taskId}/cancel/{aggregateId}/{errorCode}")
  public Map<String, Object> cancelUserTask(
      @PathParam("taskId") final String taskId,
      @PathParam("aggregateId") final Long aggregateId,
      @PathParam("errorCode") final String errorCode) throws Exception {

    return inTransaction(aggregateId, aggregate -> workflowService.cancelUserTask(aggregate, taskId, errorCode), true);

  }

  @POST
  @Path("/messages/{messageName}/correlate/{aggregateId}")
  public Map<String, Object> correlateMessage(
      @PathParam("messageName") final String messageName,
      @PathParam("aggregateId") final Long aggregateId) throws Exception {

    return inTransaction(aggregateId, aggregate -> workflowService.correlateMessage(aggregate, messageName), true);

  }

  @POST
  @Path("/messages/{messageName}/correlate/{aggregateId}/{correlationId}")
  public Map<String, Object> correlateMessage(
      @PathParam("messageName") final String messageName,
      @PathParam("aggregateId") final Long aggregateId,
      @PathParam("correlationId") final String correlationId) throws Exception {

    return inTransaction(
        aggregateId,
        aggregate -> workflowService.correlateMessage(aggregate, messageName, correlationId),
        true);

  }

  @POST
  @Path("/messages/{messageName}/correlate-and-rollback/{aggregateId}")
  public Map<String, Object> correlateMessageAndRollback(
      @PathParam("messageName") final String messageName,
      @PathParam("aggregateId") final Long aggregateId) throws Exception {

    return inTransaction(aggregateId, aggregate -> workflowService.correlateMessage(aggregate, messageName), false);

  }

  @POST
  @Path("/messages/{messageName}/start")
  @Transactional
  public Map<String, Object> startWorkflowByMessage(
      @PathParam("messageName") final String messageName) {

    final var aggregate = new C8E2eAggregate();
    entityManager.persist(aggregate);
    entityManager.flush();
    workflowService.startWorkflowByMessage(aggregate, messageName);
    return Map.of("id", String.valueOf(aggregate.getId()));

  }

  @POST
  @Path("/signals/{signalName}")
  @Transactional
  public void sendSignal(
      @PathParam("signalName") final String signalName) {

    workflowService.sendSignal(signalName);

  }

  // --- pushing a changed aggregate ---

  @POST
  @Path("/push/workflows")
  @Transactional
  public Map<String, Object> startPushWorkflow() {

    final var aggregate = new C8PushAggregate();
    aggregate.setNote("before");
    return Map.of("id", String.valueOf(pushWorkflowService.startWorkflow(aggregate).getId()));

  }

  @POST
  @Path("/push/processes/{bpmnProcessId}")
  public Map<String, Object> startPushSecondaryProcess(
      @PathParam("bpmnProcessId") final String bpmnProcessId) throws Exception {

    userTransaction.begin();
    final var aggregate = new C8PushAggregate();
    aggregate.setNote("before");
    entityManager.persist(aggregate);
    entityManager.flush();
    final var aggregateId = aggregate.getId();
    userTransaction.commit();

    final var instanceKey = client()
        .newCreateInstanceCommand()
        .bpmnProcessId(scoped(bpmnProcessId))
        .latestVersion()
        .variables(Map.of("id", String.valueOf(aggregateId), "note", "before"))
        .send()
        .join()
        .getProcessInstanceKey();
    return Map.of("id", String.valueOf(aggregateId), "processInstanceKey", String.valueOf(instanceKey));

  }

  @POST
  @Path("/push/{aggregateId}/global/{note}")
  @Transactional
  public void pushGlobally(
      @PathParam("aggregateId") final Long aggregateId,
      @PathParam("note") final String note) {

    final var aggregate = entityManager.find(C8PushAggregate.class, aggregateId);
    aggregate.setNote(note);
    pushWorkflowService.pushGlobally(aggregate);

  }

  @POST
  @Path("/push/{aggregateId}/task/{taskId}/{note}")
  @Transactional
  public void pushIntoTaskScope(
      @PathParam("aggregateId") final Long aggregateId,
      @PathParam("taskId") final String taskId,
      @PathParam("note") final String note) {

    final var aggregate = entityManager.find(C8PushAggregate.class, aggregateId);
    aggregate.setNote(note);
    pushWorkflowService.pushInto(aggregate, taskId);

  }

  // --- the viewer ---

  /**
   * The process definitions the viewer reports for a workflow, as
   * "id|bpmnProcessId|version|usedByElements".
   *
   * @param aggregateId The aggregate
   * @return One entry per definition, or the exception raised
   */
  @GET
  @Path("/viewer/definitions/{aggregateId}")
  @Transactional
  public Map<String, Object> viewerDefinitions(
      @PathParam("aggregateId") final Long aggregateId) {

    try {
      final var definitions = workflowService
          .processDefinitions(entityManager.find(C8E2eAggregate.class, aggregateId))
          .stream()
          .map(
              definition -> "%s|%s|%s|%s".formatted(
                  definition.id(),
                  definition.bpmnProcessId(),
                  definition.version(),
                  definition.usedByElements()))
          .toList();
      return Map.of("definitions", definitions);
    } catch (final Exception e) {
      return failure(e);
    }

  }

  /**
   * The definition id is handed in as a query parameter, because it carries the
   * adapter id and a '#' - which a URL path cannot.
   *
   * @param processDefinitionId The definition
   * @return The BPMN XML as deployed
   */
  @GET
  @Path("/viewer/xml")
  @Produces(MediaType.TEXT_PLAIN)
  public String viewerXml(
      @QueryParam("id") final String processDefinitionId) throws Exception {

    try (var xml = workflowService.bpmnXml(processDefinitionId)) {
      return new String(xml.readAllBytes(), StandardCharsets.UTF_8);
    }

  }

  /**
   * Asks the viewer for the XML of a process definition nobody deployed - the guiding
   * exception of the SPI.
   *
   * @param processDefinitionId The unknown definition
   * @return The exception raised
   */
  @GET
  @Path("/viewer/unknown-xml")
  public Map<String, Object> viewerUnknownXml(
      @QueryParam("id") final String processDefinitionId) {

    try (var xml = workflowService.bpmnXml(processDefinitionId)) {
      return Map.of("unexpected", xml.toString());
    } catch (final Exception e) {
      return failure(e);
    }

  }

  @GET
  @Path("/viewer/history/{aggregateId}")
  @Transactional
  public Map<String, Object> viewerHistory(
      @PathParam("aggregateId") final Long aggregateId) {

    try {
      final var history = workflowService.workflowHistory(entityManager.find(C8E2eAggregate.class, aggregateId));
      final var reported = new LinkedHashMap<String, Object>();
      reported.put("processDefinitionId", history.processDefinitionId());
      reported.put("started", history.startTime() != null);
      reported
          .put("elements", history.elementsHistory() == null
              ? null
              : history
                  .elementsHistory()
                  .stream()
                  .map(element -> element.elementId())
                  .toList());
      return reported;
    } catch (final Exception e) {
      return failure(e);
    }

  }

  // --- what the cluster holds ---

  /**
   * What the boot deployed, as "processDefinitionId|version|versionTag".
   *
   * @return One entry per deployed process definition
   */
  @GET
  @Path("/cluster/definitions")
  public List<String> clusterDefinitions() {

    return client()
        .newProcessDefinitionSearchRequest()
        .send()
        .join()
        .items()
        .stream()
        .map(
            definition -> "%s|%d|%s".formatted(
                definition.getProcessDefinitionId(),
                definition.getVersion(),
                definition.getVersionTag()))
        .sorted()
        .toList();

  }

  /**
   * The instance key of the workflow of an aggregate, read from the query API.
   *
   * @param aggregateId The aggregate
   * @return The instance key, or an empty string
   */
  @GET
  @Path("/cluster/instance-key/{aggregateId}")
  @Produces(MediaType.TEXT_PLAIN)
  public String instanceKey(
      @PathParam("aggregateId") final String aggregateId) {

    final var found = client()
        .newProcessInstanceSearchRequest()
        // variable values are stored as JSON: a String value is searched WITH its quotes
        .filter(filter -> filter.variables(Map.of("id", "\"%s\"".formatted(aggregateId))))
        .send()
        .join()
        .items();
    return found.isEmpty()
        ? ""
        : String.valueOf(found.getFirst().getProcessInstanceKey());

  }

  /**
   * The state of a workflow as the query API reports it.
   *
   * @param processInstanceKey The instance
   * @return "ACTIVE", "COMPLETED", "TERMINATED" or an empty string
   */
  @GET
  @Path("/cluster/instance-state/{processInstanceKey}")
  @Produces(MediaType.TEXT_PLAIN)
  public String instanceState(
      @PathParam("processInstanceKey") final Long processInstanceKey) {

    final var found = client()
        .newProcessInstanceSearchRequest()
        .filter(filter -> filter.processInstanceKey(processInstanceKey))
        .send()
        .join()
        .items();
    return found.isEmpty()
        ? ""
        : String.valueOf(found.getFirst().getState());

  }

  /**
   * The variables of one name of a workflow, as "scopeKey|value" - where a pushed
   * aggregate landed is read from exactly this.
   *
   * @param processInstanceKey The instance
   * @param name The variable's name
   * @return One entry per scope holding the variable
   */
  @GET
  @Path("/cluster/variables/{processInstanceKey}/{name}")
  public List<String> clusterVariables(
      @PathParam("processInstanceKey") final Long processInstanceKey,
      @PathParam("name") final String name) {

    return client()
        .newVariableSearchRequest()
        .filter(filter -> filter
            .processInstanceKey(processInstanceKey)
            .name(name))
        .send()
        .join()
        .items()
        .stream()
        .map(variable -> "%d|%s".formatted(variable.getScopeKey(), variable.getValue()))
        .toList();

  }

  /**
   * The element instance a job runs in - the scope a task-scoped push must NOT write
   * into.
   *
   * @param taskId The job key
   * @return The element instance key
   */
  @GET
  @Path("/cluster/element-instance-of-job/{taskId}")
  @Produces(MediaType.TEXT_PLAIN)
  public String elementInstanceOfJob(
      @PathParam("taskId") final Long taskId) {

    final var found = client()
        .newJobSearchRequest()
        .filter(filter -> filter.jobKey(taskId))
        .send()
        .join()
        .items();
    return found.isEmpty()
        ? ""
        : String.valueOf(found.getFirst().getElementInstanceKey());

  }

  @POST
  @Path("/cluster/instances/{processInstanceKey}/cancel")
  public void cancelInstance(
      @PathParam("processInstanceKey") final Long processInstanceKey) {

    client()
        .newCancelInstanceCommand(processInstanceKey)
        .send()
        .join();

  }

  /**
   * Completes a job outside VanillaBP - what a concurrent completion looks like to
   * the adapter.
   *
   * @param taskId The job key
   */
  @POST
  @Path("/cluster/jobs/{taskId}/complete")
  public void completeJobInTheCluster(
      @PathParam("taskId") final Long taskId) {

    client()
        .newCompleteCommand(taskId)
        .send()
        .join();

  }

  /**
   * Deploys the second, tagged version of {@code VersionedProcess} while the
   * application runs - the way another node of a rolling deployment does it.
   */
  @POST
  @Path("/cluster/deploy-version-two")
  public void deployVersionTwo() {

    client()
        .newDeployResourceCommand()
        .addResourceFromClasspath("c8-e2e/versioned/versioned-process-v2.bpmn")
        .send()
        .join();

  }

  /**
   * Whether the query API knows the tagged second version yet - the exporter feeding
   * it runs behind the deployment.
   *
   * @return <code>true</code> once the tag arrived
   */
  @GET
  @Path("/cluster/tagged-version-known")
  @Produces(MediaType.TEXT_PLAIN)
  public boolean taggedVersionKnown() {

    return client()
        .newProcessDefinitionSearchRequest()
        .filter(filter -> filter.processDefinitionId(scoped("VersionedProcess")))
        .send()
        .join()
        .items()
        .stream()
        .anyMatch(definition -> "release-2".equals(definition.getVersionTag()));

  }

  /**
   * The user tasks of a workflow, as the query API reports them.
   *
   * @param processInstanceKey The instance
   * @return One user-task key per entry
   */
  @GET
  @Path("/cluster/user-tasks/{processInstanceKey}")
  public List<String> clusterUserTasks(
      @PathParam("processInstanceKey") final Long processInstanceKey) {

    return client()
        .newUserTaskSearchRequest()
        .filter(filter -> filter.processInstanceKey(processInstanceKey))
        .send()
        .join()
        .items()
        .stream()
        .map(task -> String.valueOf(task.getUserTaskKey()))
        .toList();

  }

  // --- plumbing ---

  /**
   * Runs one {@code ProcessService} call in a transaction of its own - everything
   * reaching the cluster happens after that transaction committed, so a
   * rollback has to leave the cluster untouched.
   *
   * @param aggregateId The aggregate the operation works on
   * @param operation What to call
   * @param commit Whether to commit or to roll back
   * @return Nothing, or the exception raised
   */
  private Map<String, Object> inTransaction(
      final Long aggregateId,
      final Consumer<C8E2eAggregate> operation,
      final boolean commit) throws Exception {

    userTransaction.begin();
    try {
      operation.accept(entityManager.find(C8E2eAggregate.class, aggregateId));
    } catch (final Exception e) {
      userTransaction.rollback();
      return failure(e);
    }
    if (commit) {
      try {
        userTransaction.commit();
      } catch (final Exception e) {
        return failure(e);
      }
    } else {
      userTransaction.rollback();
    }
    return Map.of();

  }

  private Map<String, Object> failure(
      final Exception e) {

    var cause = (Throwable) e;
    while ((cause.getCause() != null) && (cause.getCause() != cause)) {
      cause = cause.getCause();
    }
    return Map
        .of(
            "exception",
            e
                .getClass()
                .getSimpleName(),
            "message",
            String.valueOf(e.getMessage()),
            "rootException",
            cause
                .getClass()
                .getSimpleName(),
            "rootMessage",
            String.valueOf(cause.getMessage()));

  }

}
