package io.vanillabp.camunda8.service;

import io.camunda.zeebe.client.ZeebeClient;
import io.vanillabp.camunda8.Camunda8AdapterConfiguration;
import io.vanillabp.springboot.adapter.AdapterAwareProcessService;
import io.vanillabp.springboot.adapter.ProcessServiceImplementation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.repository.CrudRepository;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class Camunda8ProcessService<DE>
        implements ProcessServiceImplementation<DE> {

    private static final Logger logger = LoggerFactory.getLogger(Camunda8ProcessService.class);
    
    private final CrudRepository<DE, String> workflowAggregateRepository;

    private final Class<DE> workflowAggregateClass;

    private final Function<DE, String> getWorkflowAggregateId;

    private AdapterAwareProcessService<DE> parent;
    
    private ZeebeClient client;
        
    public Camunda8ProcessService(
            final CrudRepository<DE, String> workflowAggregateRepository,
            final Function<DE, String> getWorkflowAggregateId,
            final Class<DE> workflowAggregateClass) {
        
        super();
        this.workflowAggregateRepository = workflowAggregateRepository;
        this.workflowAggregateClass = workflowAggregateClass;
        this.getWorkflowAggregateId = getWorkflowAggregateId;
                
    }
    
    @Override
    public void setParent(
            final AdapterAwareProcessService<DE> parent) {
        
        this.parent = parent;
        
    }
    
    public void wire(
            final ZeebeClient client,
            final String workflowModuleId,
            final String bpmnProcessId,
            final boolean isPrimary) {

        if (parent == null) {
            throw new RuntimeException("Not yet wired! If this occurs dependency of either "
                    + "VanillaBP Spring Boot support or Camunda8 adapter was changed introducing this "
                    + "lack of wiring. Please report a Github issue!");
            
        }

        this.client = client;
        parent.wire(
                Camunda8AdapterConfiguration.ADAPTER_ID,
                workflowModuleId,
                bpmnProcessId,
                isPrimary);
        
    }

    @Override
    public Class<DE> getWorkflowAggregateClass() {

        return workflowAggregateClass;

    }
    
    @Override
    public CrudRepository<DE, String> getWorkflowAggregateRepository() {

        return workflowAggregateRepository;

    }

    @Override
    public DE startWorkflow(
            final DE workflowAggregate) throws Exception {
        
        client
                .newCreateInstanceCommand()
                .bpmnProcessId(parent.getPrimaryBpmnProcessId())
                .latestVersion()
                .variables(workflowAggregate)
                .send()
                .get(10, TimeUnit.SECONDS);

        try {
            return workflowAggregateRepository
                    .save(workflowAggregate);
        } catch (RuntimeException exception) {
            // HibernateH2PostgresIdempotency.ignoreDuplicatesExceptionAndRethrow(exception,
            // processEntity);
            throw exception;
        }
        
    }

    @Override
    public DE correlateMessage(
            final DE workflowAggregate,
            final String messageName) {
        
        final var attachedEntity = workflowAggregateRepository
                .save(workflowAggregate);
        final var correlationId = getWorkflowAggregateId
                .apply(workflowAggregate);

        correlateMessage(
                workflowAggregate,
                messageName,
                correlationId);
        
        return attachedEntity;
        
    }
    
    @Override
    public DE correlateMessage(
            final DE workflowAggregate,
            final Object message) {
        
        return correlateMessage(
                workflowAggregate,
                message.getClass().getSimpleName());
        
    }

    @Override
    public DE correlateMessage(
            final DE workflowAggregate,
            final String messageName,
            final String correlationId) {
            
        final var attachedEntity = workflowAggregateRepository
                .save(workflowAggregate);
        
        final var messageKey = client
                .newPublishMessageCommand()
                .messageName(messageName)
                .correlationKey(correlationId)
                .variables(workflowAggregate)
                .send()
                .join()
                .getMessageKey();
        
        logger.trace("Correlated message '{}' using correlation-id '{}' for process '{}' as '{}'",
                messageName,
                correlationId,
                parent.getPrimaryBpmnProcessId(),
                messageKey);
        
        return attachedEntity;
        
    }
    
    @Override
    public DE correlateMessage(
            final DE workflowAggregate,
            final Object message,
            final String correlationId) {
        
        return correlateMessage(
                workflowAggregate,
                message.getClass().getSimpleName(),
                correlationId);
        
    }

    @Override
    public DE completeTask(
            final DE workflowAggregate,
            final String taskId) {
        
        final var attachedEntity = workflowAggregateRepository
                .save(workflowAggregate);
        
        client
                .newCompleteCommand(Long.parseLong(taskId, 16))
                .variables(workflowAggregate)
                .send()
                .join();

        logger.trace("Complete usertask '{}' for process '{}'",
                taskId,
                parent.getPrimaryBpmnProcessId());
        
        return attachedEntity;
        
    }
    
    @Override
    public DE completeUserTask(
            final DE workflowAggregate,
            final String taskId) {

        return completeTask(workflowAggregate, taskId);
        
    }
    
    @Override
    public DE cancelTask(
            final DE workflowAggregate,
            final String taskId,
            final String errorCode) {

        final var attachedEntity = workflowAggregateRepository
                .save(workflowAggregate);
        
        client
                .newThrowErrorCommand(Long.parseLong(taskId))
                .errorCode(errorCode)
                .send()
                .join();

        logger.trace("Complete usertask '{}' for process '{}'",
                taskId,
                parent.getPrimaryBpmnProcessId());
        
        return attachedEntity;
        
    }
    
    @Override
    public DE cancelUserTask(
            final DE workflowAggregate,
            final String taskId,
            final String errorCode) {

        return cancelTask(workflowAggregate, taskId, errorCode);

    }

}
