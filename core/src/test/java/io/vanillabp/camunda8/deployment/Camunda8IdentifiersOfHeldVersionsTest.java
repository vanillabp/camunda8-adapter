package io.vanillabp.camunda8.deployment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.CamundaFuture;
import io.camunda.client.api.fetch.ProcessDefinitionGetXmlRequest;
import io.camunda.client.api.search.filter.ProcessDefinitionFilter;
import io.camunda.client.api.search.request.ProcessDefinitionSearchRequest;
import io.camunda.client.api.search.response.ProcessDefinition;
import io.camunda.client.api.search.response.SearchResponse;
import io.camunda.client.api.search.response.SearchResponsePage;
import io.vanillabp.camunda8.TestCollaborators;
import io.vanillabp.camunda8.TestScoping;
import io.vanillabp.camunda8.client.Camunda8AdapterConfiguration;
import io.vanillabp.camunda8.client.Camunda8ClientFactory;
import io.vanillabp.camunda8.wiring.Camunda8JobTimeoutResolver;
import io.vanillabp.integration.adapter.spi.NameClashAvoidance;
import io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport.ModelIdentifier;
import io.vanillabp.integration.adapter.spi.NameClashAvoidanceSupport.ScopedIdentifierKind;
import io.vanillabp.integration.adapter.spi.version.ProcessVersionCatalog;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Which identifiers a version the cluster still HOLDS declares - the names a workflow
 * module deployed years ago, which no index of the cluster knows and which only that
 * version's model still carries.
 * <p>
 * The model comes back carrying the names the deployment of the day wrote into it, so the
 * prefix is stripped off again before the core sees them: the core composes the scoped forms
 * itself. A job type of such a version is the one of these names which is live rather than
 * dormant, because the workflows still running on that version produce jobs under it.
 * <p>
 * The cluster is played by a definition search and an XML request, and the number of XML
 * requests is part of what is measured: the model of the version in turn is read once,
 * however many questions are put about it.
 */
@ExtendWith(SuppressOutputExtension.class)
public class Camunda8IdentifiersOfHeldVersionsTest {

  private static final String MODULE = "loan-approval";

  private static final String PROCESS = "LoanApproval";

  private final CamundaClient client = mock(CamundaClient.class);

  /**
   * Which definition keys the XML was asked for, which is one request per fetch.
   */
  private final List<Long> xmlRequests = new ArrayList<>();

  /**
   * A model as the cluster runs it: every name carries the prefix the deployment wrote into
   * it, the job type carries the process id as well.
   */
  private static String heldModel(
      final String prefixedNames) {

    return """
        <?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="D" targetNamespace="http://bpmn.io/schema/bpmn">
        %s
          <bpmn:process id="loan-approval__LoanApproval" isExecutable="true">
            <bpmn:serviceTask id="Activity_Approve">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="loan-approval__LoanApproval__approveTheOldWay" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:process>
        </bpmn:definitions>
        """
        .formatted(prefixedNames);

  }

  private static final String NAMES_OF_THE_OLD_MODEL = """
        <bpmn:message id="Msg_1" name="loan-approval__PaymentConfirmed" />
        <bpmn:signal id="Sig_1" name="loan-approval__ApprovalWithdrawn" />
      """;

  @Test
  @DisplayName("The names of a held version are read off its model, stripped of the prefix")
  public void theNamesOfAHeldVersionAreRead() {

    final var catalog = aClusterHolding(Map.of(1, heldModel(NAMES_OF_THE_OLD_MODEL)));

    final var declared = List.copyOf(catalog.identifiersOfVersion(MODULE, PROCESS, "1"));

    assertTrue(
        declared.contains(new ModelIdentifier(ScopedIdentifierKind.MESSAGE_NAME, "PaymentConfirmed", null)),
        () -> "the message name of that version, as the application knew it: "
            + declared);
    assertTrue(
        declared.contains(new ModelIdentifier(ScopedIdentifierKind.SIGNAL_NAME, "ApprovalWithdrawn", null)),
        () -> "and its signal name: "
            + declared);
    assertTrue(
        declared
            .contains(new ModelIdentifier(ScopedIdentifierKind.TASK_DEFINITION, "approveTheOldWay", PROCESS)),
        () -> "the job type of that version, with the process it belongs to - a worker subscribes to "
            + "it while workflows of that version are still running: "
            + declared);

  }

  @Test
  @DisplayName("A held version declaring none of those names answers an empty collection")
  public void aVersionWithoutSuchNamesAnswersEmpty() {

    final var catalog = aClusterHolding(Map.of(1, heldModel("")));

    assertEquals(
        List.of(new ModelIdentifier(ScopedIdentifierKind.TASK_DEFINITION, "approveTheOldWay", PROCESS)),
        List.copyOf(catalog.identifiersOfVersion(MODULE, PROCESS, "1")),
        "the model was read and declares nothing but its job type, which is not the same as an "
            + "adapter which cannot say");

  }

  @Test
  @DisplayName("A version the cluster no longer holds is one this adapter cannot say anything about")
  public void aVersionTheClusterDoesNotHoldIsNotAnswered() {

    final var catalog = aClusterHolding(Map.of(1, heldModel(NAMES_OF_THE_OLD_MODEL)));

    assertNull(
        catalog.identifiersOfVersion(MODULE, PROCESS, "2"),
        "no model was read, so nothing is claimed about the names of that version");

  }

  @Test
  @DisplayName("Both model questions about one version share the one fetch of its model")
  public void bothQuestionsAboutAVersionShareOneFetch() {

    final var catalog = aClusterHolding(Map.of(1, heldModel(NAMES_OF_THE_OLD_MODEL)));

    catalog.identifiersOfVersion(MODULE, PROCESS, "1");
    catalog.tasksOfVersion(MODULE, PROCESS, "1");

    assertEquals(
        1,
        xmlRequests.size(),
        () -> "fetching the XML twice for one version is what the model in turn is held for: "
            + xmlRequests);

  }

  /**
   * A cluster holding the given versions of the process, each with the model it runs, and an
   * adapter which prefixes its identifiers - the mode a held model's names have to be
   * stripped under.
   */
  private ProcessVersionCatalog aClusterHolding(
      final Map<Integer, String> modelsPerVersion) {

    final var search = mock(ProcessDefinitionSearchRequest.class, RETURNS_SELF);
    // which version the filter named, 0 for a search asking for every version this cluster
    // holds - a search for a version it does not hold answers nothing, which is what makes
    // the question about a version nobody holds any more answerable at all
    final var askedFor = new int[]{
        0
    };
    Mockito
        .lenient()
        .when(search.filter(Mockito.<Consumer<ProcessDefinitionFilter>>any()))
        .thenAnswer(invocation -> {
          askedFor[0] = 0;
          final Consumer<ProcessDefinitionFilter> filter = invocation.getArgument(0);
          final var recording = mock(ProcessDefinitionFilter.class, RETURNS_SELF);
          Mockito.lenient().when(recording.version(Mockito.anyInt())).thenAnswer(call -> {
            askedFor[0] = call.getArgument(0);
            return recording;
          });
          filter.accept(recording);
          return search;
        });
    Mockito
        .lenient()
        .when(search.send())
        .thenAnswer(invocation -> future(response(modelsPerVersion
            .keySet()
            .stream()
            .sorted()
            .filter(version -> (askedFor[0] == 0) || (askedFor[0] == version.intValue()))
            .map(Camunda8IdentifiersOfHeldVersionsTest::definition)
            .toList())));
    Mockito.lenient().when(client.newProcessDefinitionSearchRequest()).thenReturn(search);
    Mockito
        .lenient()
        .when(client.newProcessDefinitionGetXmlRequest(Mockito.anyLong()))
        .thenAnswer(invocation -> {
          final var definitionKey = (Long) invocation.getArgument(0);
          xmlRequests.add(definitionKey);
          final var xml = mock(ProcessDefinitionGetXmlRequest.class, RETURNS_SELF);
          Mockito
              .lenient()
              .when(xml.send())
              .thenAnswer(request -> future(modelsPerVersion.get(Integer.valueOf(definitionKey.intValue() - 1000))));
          return xml;
        });

    final var configuration = new Camunda8AdapterConfiguration();
    // an address nothing contacts: every request of this test meets the mock above
    configuration.setRestAddress("http://localhost:1");
    final var clientFactory = new Camunda8ClientFactory("c8", configuration) {

      @Override
      public CamundaClient getClient() {
        return client;
      }

    };
    final var scoping = TestScoping.of(NameClashAvoidance.USE_PREFIX);
    final var deploymentService = new Camunda8DeploymentService(
        "c8", clientFactory, TestCollaborators.of(new Camunda8DeploymentServiceTest.NoOpInvoker(), scoping), (
            workflowModuleId,
            bpmnProcessId,
            taskDefinition) -> Camunda8JobTimeoutResolver.DEFAULT_JOB_TIMEOUT, Duration
                .ofDays(14), adapterId -> configuration, scoping);
    return deploymentService.processVersionCatalogOf(MODULE, PROCESS);

  }

  private static ProcessDefinition definition(
      final int version) {

    final var definition = mock(ProcessDefinition.class);
    Mockito.lenient().when(definition.getProcessDefinitionKey()).thenReturn(Long.valueOf(1000 + version));
    Mockito.lenient().when(definition.getVersion()).thenReturn(version);
    return definition;

  }

  private static <T> SearchResponse<T> response(
      final List<T> items) {

    @SuppressWarnings("unchecked")
    final SearchResponse<T> response = mock(SearchResponse.class);
    final var page = mock(SearchResponsePage.class);
    Mockito.lenient().when(page.totalItems()).thenReturn(Long.valueOf(items.size()));
    Mockito.lenient().when(response.items()).thenReturn(items);
    Mockito.lenient().when(response.page()).thenReturn(page);
    return response;

  }

  private static <T> CamundaFuture<T> future(
      final T value) {

    @SuppressWarnings("unchecked")
    final CamundaFuture<T> future = mock(CamundaFuture.class);
    Mockito.lenient().when(future.join()).thenReturn(value);
    return future;

  }

}
