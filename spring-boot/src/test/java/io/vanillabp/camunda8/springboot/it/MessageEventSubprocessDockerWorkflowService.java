package io.vanillabp.camunda8.springboot.it;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * The workflow service of the integration test of event subprocesses which start on a
 * message. The workflow waits at a timer catch event it never gets past. Two event
 * subprocesses start on a message: one runs next to the workflow, the other one takes it
 * over.
 * <p>
 * Neither message carries a <code>zeebe:subscription</code> in the model. The model is the
 * same as the one a Camunda 7 application would deploy. The adapter adds the correlation key,
 * and the application correlates by the aggregate like for every other message.
 */
@Service
@WorkflowService(
    workflowAggregateClass = MessageEventSubprocessDockerAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "MessageEventSubprocessProcess"))
public class MessageEventSubprocessDockerWorkflowService {

  @Autowired
  private ProcessService<MessageEventSubprocessDockerAggregate> processService;

  public MessageEventSubprocessDockerAggregate startWorkflow() {

    final var aggregate = new MessageEventSubprocessDockerAggregate();
    aggregate.setId(UUID.randomUUID().toString());
    return processService.startWorkflow(aggregate);

  }

  public MessageEventSubprocessDockerAggregate correlate(
      final MessageEventSubprocessDockerAggregate aggregate,
      final String messageName) {

    return processService.correlateMessage(aggregate, messageName);

  }

  @WorkflowTask(taskDefinition = "recordNote")
  public void recordNote(
      final MessageEventSubprocessDockerAggregate aggregate) {

    aggregate.setNotes(aggregate.getNotes()
        + "note;");

  }

  @WorkflowTask(taskDefinition = "recordMessageTakeOver")
  public void recordMessageTakeOver(
      final MessageEventSubprocessDockerAggregate aggregate) {

    aggregate.setTakenOverBy("recordMessageTakeOver");

  }

}
