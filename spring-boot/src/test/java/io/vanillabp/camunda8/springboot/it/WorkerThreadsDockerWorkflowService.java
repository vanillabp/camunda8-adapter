package io.vanillabp.camunda8.springboot.it;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.vanillabp.spi.process.ProcessService;
import io.vanillabp.spi.service.BpmnProcess;
import io.vanillabp.spi.service.WorkflowService;
import io.vanillabp.spi.service.WorkflowTask;

/**
 * The two workflows of the worker-threads integration test: one handler
 * blocks its execution slot for a while, the other one has to be served meanwhile by
 * another slot of the SAME adapter.
 */
@Service
@WorkflowService(
    workflowAggregateClass = WorkerThreadsDockerAggregate.class,
    bpmnProcess = @BpmnProcess(bpmnProcessId = "BlockingProcess"))
public class WorkerThreadsDockerWorkflowService {

  /**
   * How long the blocking handler holds its execution slot.
   */
  public static final long BLOCK_MILLIS = 4000;

  /**
   * Counted down when the blocking handler entered.
   */
  public static volatile CountDownLatch BLOCKING_ENTERED = new CountDownLatch(1);

  /**
   * Counted down when the handler of the other worker ran.
   */
  public static volatile CountDownLatch QUICK_SERVED = new CountDownLatch(1);

  /**
   * Whether the blocking handler was still inside when the other one ran - a quick
   * handler served only AFTER the block would prove nothing.
   */
  public static final AtomicBoolean QUICK_SERVED_WHILE_BLOCKED = new AtomicBoolean();

  /**
   * Whether the quick handler ran on a virtual thread (asserted by the virtual-mode
   * variant of the test).
   */
  public static final AtomicBoolean QUICK_SERVED_ON_VIRTUAL_THREAD = new AtomicBoolean();

  /**
   * When the quick handler ran, to measure how long it waited.
   */
  public static final AtomicLong QUICK_SERVED_AT = new AtomicLong();

  private static final AtomicBoolean BLOCKING = new AtomicBoolean();

  private static final AtomicBoolean ALREADY_BLOCKED = new AtomicBoolean();

  /**
   * Puts the shared observations back to their initial state - called by the test
   * which uses them, so nothing a class left behind reaches the next one.
   */
  public static void reset() {

    BLOCKING_ENTERED = new CountDownLatch(1);
    QUICK_SERVED = new CountDownLatch(1);
    QUICK_SERVED_WHILE_BLOCKED.set(false);
    QUICK_SERVED_ON_VIRTUAL_THREAD.set(false);
    QUICK_SERVED_AT.set(0);
    BLOCKING.set(false);
    ALREADY_BLOCKED.set(false);

  }

  @Autowired
  private ProcessService<WorkerThreadsDockerAggregate> processService;

  public WorkerThreadsDockerAggregate startBlocking() {

    return processService.startWorkflow(new WorkerThreadsDockerAggregate());

  }

  @WorkflowTask(taskDefinition = "blockingTask")
  public void blockingTask(
      final WorkerThreadsDockerAggregate aggregate) throws InterruptedException {

    aggregate.setServedBy("blocking");
    // a redelivery must not block a second time - the block is the test's setup, not
    // the handler's business
    if (ALREADY_BLOCKED.compareAndSet(false, true)) {
      BLOCKING.set(true);
      BLOCKING_ENTERED.countDown();
      try {
        TimeUnit.MILLISECONDS.sleep(BLOCK_MILLIS);
      } finally {
        BLOCKING.set(false);
      }
    }

  }

  /**
   * Whether the blocking handler is inside its block right now - read by the other
   * workflow service, which is what proves the two ran at the same time.
   *
   * @return Whether a slot is blocked at this moment
   */
  public static boolean isBlocking() {

    return BLOCKING.get();

  }

}
