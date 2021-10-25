/*
 * Copyright (c) 2011-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 * Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG.
 */
package com.tc.object;

import org.junit.Assert;
import org.mockito.stubbing.Answer;
import org.terracotta.entity.EntityMessage;
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.exception.EntityException;
import org.terracotta.utilities.test.Diagnostics;

import com.tc.async.api.EventHandler;
import com.tc.async.api.EventHandlerException;
import com.tc.async.api.Sink;
import com.tc.async.api.Stage;
import com.tc.async.api.StageManager;
import com.tc.bytes.TCByteBuffer;
import com.tc.entity.MessageCodecSupplier;
import com.tc.entity.NetworkVoltronEntityMessage;
import com.tc.entity.VoltronEntityMessage;
import com.tc.net.ClientID;
import com.tc.net.NodeID;
import com.tc.net.core.ProductID;
import com.tc.net.protocol.NetworkLayer;
import com.tc.net.protocol.NetworkStackID;
import com.tc.net.protocol.TCNetworkMessage;
import com.tc.net.protocol.tcm.ChannelEventListener;
import com.tc.net.protocol.tcm.ChannelID;
import com.tc.net.protocol.tcm.ClientMessageChannel;
import com.tc.net.protocol.tcm.MessageChannel;
import com.tc.net.protocol.tcm.TCMessage;
import com.tc.net.protocol.tcm.TCMessageType;
import com.tc.net.protocol.transport.ClientConnectionErrorListener;
import com.tc.net.protocol.transport.ConnectionID;
import com.tc.net.protocol.transport.MessageTransport;
import com.tc.net.protocol.transport.MessageTransportInitiator;
import com.tc.object.session.SessionID;
import com.tc.object.tx.TransactionID;
import com.tc.util.TCAssertionError;

import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import junit.framework.TestCase;

import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite that attempts to expose race exposures.
 */
public class ClientEntityManagerImplStressTest  extends TestCase {

  private static final boolean VERBOSE = false;

  private final long startTime = System.currentTimeMillis();

  @Override
  public void setUp() throws Exception {
    // Preload nested classes to avoid "client" startup delays
    Class.forName("com.tc.bytes.TCByteBufferFactory");
    Class.forName("com.tc.object.InFlightMessage");
    Class.forName("com.tc.tracing.Trace");
  }

  public void _testBulkMessageWithShutdown() {
    int clientCount = 600;
    int actorCount = 125;
    Duration testDuration = Duration.ofSeconds(30L);
    System.out.format("ClientCount=%d, ActorCount=%d, TestDuration=%s%n", clientCount, actorCount, testDuration);

    ExecutorService clientService = Executors.newFixedThreadPool(clientCount);
    try {
      Client.SingleUseGate dumpGate = new Client.SingleUseGate();
      List<Future<?>> clients = new ArrayList<>();
      for (int i = 0; i < clientCount; i++) {
        clients.add(clientService.submit(() -> new Client(startTime, testDuration, actorCount, dumpGate).run()));
      }

      AssertionError fault = null;
      boolean interrupted = false;
      boolean timedOut = false;
      Duration waitLimit = testDuration.multipliedBy(20L);
      for (Iterator<Future<?>> iterator = clients.iterator(); iterator.hasNext(); ) {
        Future<?> future = iterator.next();
        try {
          future.get(waitLimit.toNanos(), TimeUnit.NANOSECONDS);
          iterator.remove();
        } catch (ExecutionException e) {
          if (fault == null) {
            fault = new AssertionError(e.getCause());
          } else {
            fault.addSuppressed(e.getCause());
          }
          iterator.remove();
        } catch (InterruptedException e) {
          interrupted = true;
          break;
        } catch (TimeoutException e) {
          timedOut = true;
        }
      }

      if (fault != null || interrupted || timedOut) {
        clients.forEach(f -> f.cancel(true));
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        if (timedOut) {
          dumpGate.apply(() -> System.err.format("Heap dump written to %s%n", Diagnostics.dumpHeap(false)));
        }
        if (fault == null) {
          throw new AssertionError("Test " + (interrupted ? "interrupted" : "timedOut after " + waitLimit));
        } else {
          fault.addSuppressed(new AssertionError("Test " + (interrupted ? "interrupted" : "timedOut after " + waitLimit)));
          throw fault;
        }
      } else {
        System.out.format("Test complete without stall");
      }

    } finally {
      clientService.shutdownNow();
    }
  }

  public void testRepeatedMessageWithShutdown() {
    int clientCount = fromIntProperty("clientCount", 200);
    int actorCount = fromIntProperty("actorCount", 900);
    Duration testDuration = fromDurationProperty("testDuration", Duration.ofSeconds(10L));
    System.out.format("ClientCount=%d, ActorCount=%d, TestDuration=%s%n", clientCount, actorCount, testDuration);

    Duration waitLimit = testDuration.multipliedBy(20L);

    ExecutorService clientService = Executors.newFixedThreadPool(1);    // Single client at a time
    Client.SingleUseGate dumpGate = new Client.SingleUseGate();
    int emitPeriod = clientCount * 10 / 100;
    try {
      for (int i = 0; i < clientCount; i++) {
        Future<?> future = clientService.submit(() -> new Client(startTime, testDuration, actorCount, dumpGate).run());
        try {
          future.get(waitLimit.toNanos(), TimeUnit.NANOSECONDS);
        } catch (ExecutionException e) {
          throw new AssertionError(e.getCause());
        } catch (InterruptedException e) {
          throw new AssertionError("Test interrupted");
        } catch (TimeoutException e) {
          dumpGate.apply(() -> System.err.format("Heap dump written to %s%n", Diagnostics.dumpHeap(false)));
          throw new AssertionError("Test timedOut after " + waitLimit);
        }

        if ((i + 1) % emitPeriod == 0 || (i + 1) == clientCount) {
          System.out.format("%tM:%<tS.%<tL Completed %d of %d client cycles without stall%n",
              System.currentTimeMillis() - startTime, (i + 1), clientCount);
        }
      }
    } finally {
      clientService.shutdownNow();
    }
  }

  private int fromIntProperty(String name, int defaultValue) {
    String propertyName = this.getClass().getSimpleName() + '.' + name;
    String propertyValue = System.getProperty(propertyName);
    if (propertyValue == null || propertyValue.isEmpty()) {
      return defaultValue;
    } else {
      try {
        return Integer.parseUnsignedInt(propertyValue);
      } catch (NumberFormatException e) {
        throw new AssertionError("Value specified for " + propertyName
            + " must be a non-negative integer: found '" + propertyValue + "'", e);
      }
    }
  }

  private Duration fromDurationProperty(@SuppressWarnings("SameParameterValue") String name, Duration defaultValue) {
    String propertyName = this.getClass().getSimpleName() + '.' + name;
    String propertyValue = System.getProperty(propertyName);
    if (propertyValue == null || propertyValue.isEmpty()) {
      return defaultValue;
    } else {
      try {
        Duration duration = Duration.parse(propertyValue);
        if (duration.isNegative()) {
          throw new DateTimeParseException(name + " duration may not be negative", propertyValue, 0);
        }
        return duration;
      } catch (DateTimeParseException e) {
        throw new AssertionError("Value specified for " + propertyName
            + " must be non-negative Duration value expressed as a string: found '" + propertyValue + "';"
            + "\n    See https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-", e);
      }
    }
  }

  /**
   * Simulates a single, multi-threaded messaging client.
   */
  private static final class Client implements Runnable {
    private static final AtomicInteger CLIENT_COUNTER = new AtomicInteger(0);

    private final int clientNumber = CLIENT_COUNTER.incrementAndGet();
    private final ClientInstanceID clientInstance = new ClientInstanceID(clientNumber);
    private final boolean retireOnCompletion = false;

    /**
     * Amount of time, following ClientEntityManager shutdown, for the client threads
     * to continue to attempt to send messages.
     */
    private final Duration postShutdownDelay = Duration.ofMillis(250L);

    private final ThreadGroup clientThreadGroup;

    private final Duration testDuration;
    private final int actorCount;
    private final int serverThreadCount;
    private final Random rnd = new Random();

    private final ClientEntityManager manager;
    private final long startTime;
    private final SingleUseGate dumpGate;

    /**
     * Creates a client instance.
     * @param startTime the {@code System.currentTimeMillis()} value at the start of the overall test;
     *                  this value is used to emit <i>delta</i> timestamps in the console log
     * @param testDuration the period over which message generation should be performed; the
     *                     full client lifetime is greater to permit completion of enqueued
     *                     messages
     * @param actorCount the number of threads the client should use for message generation
     * @param dumpGate the {@link SingleUseGate} through which heap dump instances are produced
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Client(long startTime, Duration testDuration, int actorCount, SingleUseGate dumpGate) {
      this.startTime = startTime;
      this.testDuration = testDuration;
      this.actorCount = actorCount;
      this.dumpGate = dumpGate;
      this.serverThreadCount = 1;   // Single thread is used to "receive" and process server traffic

      this.clientThreadGroup = new ThreadGroup("client-" + clientNumber);
      this.clientThreadGroup.setDaemon(true);

      ClientID clientID = new ClientID(clientInstance.getID());
      TestClientMessageChannel channel = new TestClientMessageChannel(clientID);
      StageManager stageMgr = mock(StageManager.class);
      when(stageMgr.createStage(any(String.class), any(Class.class), any(
          EventHandler.class), anyInt(), anyInt())).then((Answer)invocation -> {
        Stage stage = mock(Stage.class);
        when(stage.getSink()).thenReturn(new FakeSink((EventHandler)invocation.getArguments()[2]));
        return stage;
      });
      when(stageMgr.getStage(any(String.class), any(Class.class))).then((Answer)invocation -> {
        Stage stage = mock(Stage.class);
        when(stage.getSink()).thenReturn(new FakeSink(null));
        return stage;
      });
      this.manager = new ClientEntityManagerImpl(channel, stageMgr);
      channel.setManager(this.manager);
    }

    /**
     * Manages running the number of threads designated by {@link  #actorCount} each producing and awaiting
     * retirement of as many messages permitted within the {@link #testDuration}.  Each message is completed
     * and retired using the number of threads designated in {@link #serverThreadCount} (normally 1) by way of
     * a {@link DelayQueue} which introduced message response and retirement delays.
     */
    @Override
    @SuppressWarnings("ConstantConditions")
    public void run() {

      ExecutorService messageService =
          Executors.newCachedThreadPool(new PurposedThreadFactory(clientThreadGroup, "actor"));
      ((ThreadPoolExecutor)messageService).setCorePoolSize(1 + actorCount * 50 / 100);
      message("Instantiating %d client actor threads%n", ((ThreadPoolExecutor)messageService).getCorePoolSize());
      ((ThreadPoolExecutor)messageService).prestartAllCoreThreads();

      ExecutorService serviceService =
          Executors.newFixedThreadPool(1, new PurposedThreadFactory(clientThreadGroup, "server"));
      message("Instantiating %d server task threads%n", ((ThreadPoolExecutor)serviceService).getCorePoolSize());
      ((ThreadPoolExecutor)serviceService).prestartAllCoreThreads();
      DelayQueue<DelayableRunnable> serverQueue = new DelayQueue<>();

      LongAdder submittedMessageCount = new LongAdder();
      LongAdder completedMessageCount = new LongAdder();
      LongAdder fetchedMessageCount = new LongAdder();
      AtomicLong retiredMessageCount = new AtomicLong(0);

      Runnable progressMessage = () -> message("Submitted messages=%d, Completed messages=%d, Fetched messages=%d, Retired messages=%d%n",
          submittedMessageCount.longValue(), completedMessageCount.longValue(), fetchedMessageCount.longValue(), retiredMessageCount.longValue());

      EntityID entityID = new EntityID("ClassName", String.format("testEntity%04d", 0));
      FetchID fetchID = new FetchID(0);
      EntityDescriptor descriptor = EntityDescriptor.createDescriptorForInvoke(fetchID, clientInstance);

      AtomicBoolean stopped = new AtomicBoolean(false);

      try {
        /*
         * Create and start message producers that complete, but do not retire, messages
         */
        List<Producer> producers = new ArrayList<>();
        AtomicInteger messageId = new AtomicInteger();
        for (int i = 0; i < actorCount; i++) {
          int actorId = i;
          Producer producer = new Producer(actorId);

          Future<Producer> future = messageService.submit(() -> {
            Thread currentThread = Thread.currentThread();
            producer.setThreadId(currentThread);

            while (!stopped.get() && !Thread.interrupted()) {

              /*
               * "Send" a message for which 'get' waits for retirement.
               */
              State state = new State(actorId, messageId.incrementAndGet(), producer);
              state.threadId = currentThread.toString();
              if (VERBOSE) message("Sending InFlightMessage %d:%d%n", state.actor, state.messageId);
              InFlightMessage inFlightMessage = manager.invokeAction(entityID, descriptor,
                  EnumSet.noneOf(VoltronEntityMessage.Acks.class), null,
                  true, true, new byte[0]);
              state.message = inFlightMessage;
              state.msgSent = true;
              submittedMessageCount.increment();
              producer.sentMessages.incrementAndGet();
              if (VERBOSE)
                message("Sent InFlightMessage %d:%d %s%n", state.actor, state.messageId, inFlightMessage.getTransactionID());

              /*
               * Schedule completion of the message.
               */
              state.msgDelay = (long)rnd.nextInt(250);
              if (VERBOSE)
                message("Scheduling InFlightMessage %d:%d %s to complete after %dms%n",
                    state.actor, state.messageId, inFlightMessage.getTransactionID(), state.msgDelay);
              serverQueue.add(new DelayableRunnable(Duration.ofMillis(state.msgDelay),
                  () -> {
                    ((TestRequestBatchMessage)inFlightMessage.getMessage()).explicitComplete(new byte[0], null, retireOnCompletion);
                    completedMessageCount.increment();
                    producer.completedMessages.incrementAndGet();
                    if (VERBOSE)
                      message("Completed InFlightMessage %d:%d %s after %dms%n",
                          state.actor, state.messageId, inFlightMessage.getTransactionID(), state.msgDelay);

                    /*
                     * If necessary, schedule retirement of the message.
                     */
                    if (!retireOnCompletion) {
                      serverQueue.add(new DelayableRunnable(Duration.ofNanos(ThreadLocalRandom.current().nextLong(50)),
                          () -> {
                            if (inFlightMessage != null)
                              if (!inFlightMessage.isDone()) {
                                if (VERBOSE)
                                  message("Retiring InFlightMessage %d:%d %s%n",
                                      state.actor, state.messageId, inFlightMessage.getTransactionID());
                                try {
                                  manager.retired(inFlightMessage.getTransactionID());
                                  state.retired = true;
                                  state.producer.retiredMessages.incrementAndGet();
                                  if (retiredMessageCount.incrementAndGet() % 10000 == 0) {
                                    progressMessage.run();
                                  }
                                } catch (TCAssertionError e) {
                                  message("Failed to retire InFlightMessage %d:%d %s:%n",
                                      state.actor, state.messageId, inFlightMessage.getTransactionID());
                                  e.printStackTrace(System.out);
                                }
                              } else {
                                if (VERBOSE)
                                  message("InFlightMessage %d:%d %s already done%n",
                                      state.actor, state.messageId, inFlightMessage.getTransactionID());
                              }
                          }));
                    }
                  }));

              /*
               * Now await message completion (and retirement.)
               */
              inFlightMessage.get();
              state.msgComplete = true;
              fetchedMessageCount.increment();
              producer.fetchedMessages.incrementAndGet();
              if (VERBOSE)
                message("InFlightMessage %d:%d %s complete for %s%n",
                    state.actor, state.messageId, inFlightMessage.getTransactionID(), entityID);
            }

            return producer;
          });

          producer.setFuture(future);
          producers.add(producer);
        }

        /*
         * Now crank up the server thread(s) to completed and retire messages.
         */
        List<ServerTask<?>> serverTasks = new ArrayList<>();
        for (int i = 0; i < serverThreadCount; i++) {
          ServerTask<Void> serverTask = new ServerTask<>();
          Future<Void> future = serviceService.submit(() -> {
            serverTask.setThreadId(Thread.currentThread());
            try {
              while (!(serverQueue.isEmpty() && stopped.get())) {
                DelayableRunnable task = serverQueue.take();
                if (task.isMarker()) {
                  if (VERBOSE) message("Marker received; terminating server task%n");
                  serverQueue.put(task);    // Re-queue for any other finishers
                  break;
                }
                task.run();
              }
            } catch (InterruptedException e) {
              message("Finish thread interrupted%n");
            }
          }, null);
          serverTask.setFuture(future);
          serverTasks.add(serverTask);
        }

        /*
         * Now that all the client threads are started, wait for the period
         * designated by testDuration to give the client threads a chance to
         * move messages around.
         */
        message("Snoozing until test duration (%s) expires ...%n", testDuration);
        try {
          TimeUnit.NANOSECONDS.sleep(testDuration.toNanos());
        } catch (InterruptedException e) {
          message("Test truncated by interruption%n");
          Thread.currentThread().interrupt();
        }

        /*
         * The testDuration is now complete; shut down the ClientEntityManager instance.
         */
        message("Attempting manager.shutdown%n");
        manager.shutdown();
        message("Manager.shutdown successful%n");

        /*
         * In a normal client, some time may pass between ClientEntityManager shutdown
         * and recognition by each thread that a shutdown has occurred.  Allow some
         * time for generation of additional messages.
         */
        if (!Thread.currentThread().isInterrupted()) {
          message("Pausing for %s to allow more messages after shutdown%n", postShutdownDelay);
          try {
            TimeUnit.NANOSECONDS.sleep(postShutdownDelay.toNanos());
          } catch (InterruptedException e) {
            message("Test truncated by interruption%n");
            Thread.currentThread().interrupt();
          }
        }

        /*
         * Signal the client threads to cease attempting to generate messages and
         * to exit once any "active" message is completed/retired.
         */
        message("Signaling producer threads to stop%n");
        stopped.set(true);

        /*
         * Allow server task thread(s) to finish any pending completion and retirement tasks.
         * A "gate" task is added following any enqueued tasks to cause the server thread(s)
         * to terminate.
         */
        message("Stopping server service and awaiting termination: %d server tasks queued ...%n", serverQueue.size());
        serviceService.shutdown();

        DelayableRunnable gate = new DelayableRunnable(serverQueue.stream()
            .max(comparingLong(d -> d.getDelay(TimeUnit.NANOSECONDS)))
            .map(Delayable::expiration)
            .orElse(System.nanoTime()));
        message("Adding gate entry to server task queue (%d): %s%n", serverQueue.size(), gate);
        serverQueue.add(gate);

        try {
          if (serviceService.awaitTermination(1L, TimeUnit.MINUTES)) {
            message("Server service ended: %d server tasks remain queued%n", serverQueue.size());
          } else {
            message("Server service not ended: %d server tasks remain active, %d server tasks remain queued%n",
                ((ThreadPoolExecutor)serviceService).getActiveCount(), serverQueue.size());
          }
        } catch (InterruptedException e) {
          message("Wait for server service stop interrupted%n");
          Thread.currentThread().interrupt();
        }

        /*
         * Await completion of each server task thread noting for diagnostic purposes
         * any thread which is not complete within the allotted time.
         */
        Calendar threadsInstant = Calendar.getInstance();   // Time at which the thread
        List<Thread> allThreads = Arrays.asList(Diagnostics.getAllThreads());
        List<Thread> threadsToDump = new ArrayList<>();
        boolean testFault = false;

        long serverWaitStart = System.nanoTime();
        long serverDeadlineNanos = Math.max(0, gate.getDelay(TimeUnit.NANOSECONDS)) + Duration.ofSeconds(30L).toNanos();

        message("Awaiting completion of server tasks; %d queued ...%n", serverQueue.size());
        for (Iterator<ServerTask<?>> iterator = serverTasks.iterator(); iterator.hasNext(); ) {
          ServerTask<?> task = iterator.next();
          try {
            message("Waiting %s for server task completion%n", Duration.ofNanos(serverDeadlineNanos));    // DEBUG
            task.future().get(serverDeadlineNanos, TimeUnit.NANOSECONDS);
            if (VERBOSE)
              message("Server task [%s] completed: %d tasks remain queued%n", task.threadId(), serverQueue.size());
            iterator.remove();
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            message("Server task [%s] failed:%n", task.threadId());
            cause.printStackTrace(System.out);
            iterator.remove();
          } catch (InterruptedException e) {
            message("Wait for server task [%s] interrupted%n", task.threadId());
            Thread.currentThread().interrupt();
            break;
          } catch (TimeoutException e) {
            message("Failed to fetch server task [%s]%n", task.threadId());
            testFault = true;

            allThreads.stream()
                .filter(t -> t.toString().equals(task.threadId()))
                .forEach(threadsToDump::add);
          }

          serverDeadlineNanos = Math.max(0, serverDeadlineNanos - (System.nanoTime() - serverWaitStart));
        }

        /*
         * Await completion of all client actor threads.  For any thread that does
         * not complete in the allotted time, treat the thread as blocked for
         * diagnostic purposes.
         */
        message("Awaiting completion of producer tasks ...%n");
        int closeObservedCount = 0;
        try {
          for (Iterator<Producer> iterator = producers.iterator(); iterator.hasNext(); ) {
            Producer producer = iterator.next();
            Future<Producer> task = producer.future();
            try {
              Producer stats = task.get(5L, TimeUnit.SECONDS);
              if (VERBOSE)
                message("Producer %d:* complete: %s%n", producer.actorId, stats);
              iterator.remove();
            } catch (ExecutionException e) {
              Throwable cause = e.getCause();
              if ((cause instanceof UndeclaredThrowableException) && (cause.getCause() instanceof ConnectionClosedException)) {
                closeObservedCount++;
              } else if (cause instanceof ConnectionClosedException) {
                closeObservedCount++;
              } else {
                message("Producer %d:* task get failed; %s%n", producer.actorId, e);
                e.printStackTrace(System.out);
              }
              iterator.remove();
            } catch (InterruptedException e) {
              message("Producer %d:* task get interrupted%n", producer.actorId);
              Thread.currentThread().interrupt();
              break;
            } catch (TimeoutException e) {
              message("Timed out attempting to complete Producer %d:* \"%s\"%n", producer.actorId, producer.threadId());
              testFault = true;

              allThreads.stream()
                  .filter(t -> t.toString().equals(producer.threadId()))
                  .forEach(threadsToDump::add);
            }
          }
        } finally {
          message("closedObservedCount=%d%n", closeObservedCount);
        }

        progressMessage.run();

        if (testFault) {
          dumpGate.apply(() -> message("Heap dump at %s%n", Diagnostics.dumpHeap(true)));
          synchronized (System.out) {
            message("%d stalled threads encountered%n" +
                "    %s%n", threadsToDump.size(), threadsToDump.stream().map(Thread::toString). collect(toList()));
            System.out.format("%nSelect thread dump %tF %<tT.%<tL %<tz%n", threadsInstant);
            Diagnostics.dumpThreads(threadsToDump, true, System.out);
          }
          fail("Stalled threads detected " + threadsToDump);
        }

      } finally {
        messageService.shutdownNow();
      }
    }

    private void message(String format, Object... args) {
      synchronized (System.out) {
        System.out.format("%tM:%<tS.%<tL Client[%4d] [%s] ", System.currentTimeMillis() - startTime, clientNumber, Thread.currentThread());
        System.out.format(format, args);
      }
    }

    public static class SingleUseGate {
      private final AtomicBoolean gate = new AtomicBoolean(false);

      @SuppressWarnings("UnusedReturnValue")
      public boolean apply(Runnable task) {
        if (gate.compareAndSet(false, true)) {
          task.run();
          return true;
        } else {
          return false;
        }
      }
    }

    private static class PurposedThreadFactory implements ThreadFactory {
      private final AtomicInteger threadCount = new AtomicInteger(0);

      private final String purpose;
      private final ThreadGroup clientThreadGroup;

      private PurposedThreadFactory(ThreadGroup clientThreadGroup, String purpose) {
        this.clientThreadGroup = clientThreadGroup;
        this.purpose = purpose;
      }

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(clientThreadGroup, r,
            clientThreadGroup.getName() + "-" + purpose + "-" + threadCount.incrementAndGet());
      }
    }
  }

  private static class TestRequestBatchMessage implements NetworkVoltronEntityMessage {
    private final ClientEntityManager clientEntityManager;
    private final byte[] resultObject;
    private final EntityException resultException;
    private final boolean autoComplete;
    private TransactionID transactionID;
    private EntityDescriptor descriptor;
    private EntityID entityID;
    private TCByteBuffer extendedData;
    private boolean requiresReplication;
    private Type type;

    boolean sent = false;

    public TestRequestBatchMessage(ClientEntityManager clientEntityManager, byte[] resultObject, EntityException resultException, boolean autoComplete) {
      this.clientEntityManager = clientEntityManager;
      this.resultObject = resultObject;
      this.resultException = resultException;
      this.autoComplete = autoComplete;
    }

    public void explicitComplete(byte[] explicitResult, EntityException resultException, boolean retire) {
      Assert.assertFalse(this.autoComplete);
      if (null != explicitResult) {
        this.clientEntityManager.complete(this.transactionID, explicitResult);
      } else {
        if (null != resultException) {
          this.clientEntityManager.failed(this.transactionID, resultException);
        } else {
          this.clientEntityManager.complete(this.transactionID);
        }
      }
      if (retire) {
        this.clientEntityManager.retired(this.transactionID);
      }
    }

    @Override
    public TransactionID getTransactionID() {
      return this.transactionID;
    }

    @Override
    public EntityID getEntityID() {
      return this.entityID;
    }

    @Override
    public Set<Acks> getRequestedAcks() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean doesRequestReceived() {
      return true;
    }

    @Override
    public boolean doesRequestRetired() {
      return false;
    }

    @Override
    public TCMessageType getMessageType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void hydrate() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void dehydrate() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean send() {
      assertFalse(sent);
      sent = true;
      if (this.autoComplete) {
        if (null != this.resultObject) {
          this.clientEntityManager.complete(this.transactionID, this.resultObject);
        } else {
          if (null != this.resultException) {
            this.clientEntityManager.failed(this.transactionID, this.resultException);
          } else {
            this.clientEntityManager.complete(this.transactionID);
          }
        }
        this.clientEntityManager.retired(this.transactionID);
      }
      return sent;
    }

    @Override
    public MessageChannel getChannel() {
      throw new UnsupportedOperationException();
    }
    @Override
    public NodeID getSourceNodeID() {
      throw new UnsupportedOperationException();
    }
    @Override
    public NodeID getDestinationNodeID() {
      throw new UnsupportedOperationException();
    }
    @Override
    public SessionID getLocalSessionID() {
      throw new UnsupportedOperationException();
    }
    @Override
    public int getTotalLength() {
      throw new UnsupportedOperationException();
    }
    @Override
    public ClientID getSource() {
      throw new UnsupportedOperationException();
    }
    @Override
    public EntityDescriptor getEntityDescriptor() {
      return this.descriptor;
    }
    @Override
    public boolean doesRequireReplication() {
      return this.requiresReplication;
    }
    @Override
    public Type getVoltronType() {
      return type;
    }
    @Override
    public TCByteBuffer getExtendedData() {
      return this.extendedData.asReadOnlyBuffer();
    }
    @Override
    public TransactionID getOldestTransactionOnClient() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setContents(ClientID clientID, TransactionID transactionID, EntityID eid, EntityDescriptor entityDescriptor,
                            Type type, boolean requiresReplication, TCByteBuffer extendedData, TransactionID oldestTransactionPending, Set<Acks> acks) {
      this.transactionID = transactionID;
      Assert.assertNotNull(eid);
      this.entityID = eid;
      this.descriptor = entityDescriptor;
      this.extendedData = extendedData;
      this.requiresReplication = requiresReplication;
      this.type = type;
    }

    @Override
    public void setMessageCodecSupplier(MessageCodecSupplier supplier) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EntityMessage getEntityMessage() {
      throw new UnsupportedOperationException();
    }
  }

  private static class FakeSink implements Sink<Object> {

    private final EventHandler<Object> handle;

    public FakeSink(EventHandler<Object> handle) {
      this.handle = handle;
    }

    @Override
    public void addToSink(Object context) {
      try {
        handle.handleEvent(context);
      } catch (EventHandlerException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final class Producer extends ThreadBoundTask<Producer> {
    private final int actorId;
    final AtomicInteger sentMessages = new AtomicInteger(0);
    final AtomicInteger completedMessages = new AtomicInteger(0);
    final AtomicInteger fetchedMessages = new AtomicInteger(0);
    final AtomicInteger retiredMessages = new AtomicInteger(0);

    Producer(int actorId) {
      this.actorId = actorId;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("{");
      sb.append("actor=").append(actorId);
      sb.append(", threadId='").append(threadId()).append('\'');
      sb.append(", sentMessages=").append(sentMessages);
      sb.append(", completedMessages=").append(completedMessages);
      sb.append(", fetchedMessages=").append(fetchedMessages);
      sb.append(", retiredMessages=").append(retiredMessages);
      sb.append('}');
      return sb.toString();
    }
  }

  private static final class ServerTask<V> extends ThreadBoundTask<V> {
  }

  private abstract static class ThreadBoundTask<V> {

    private volatile Future<V> producerFuture;
    private volatile String threadId;

    void setFuture(Future<V> future) {
      this.producerFuture = future;
    }

    Future<V> future() {
      return producerFuture;
    }

    public String threadId() {
      return threadId;
    }

    void setThreadId(Thread thread) {
      this.threadId = thread.toString();
    }
  }

  private static final class State {
    private static final int THREAD_ID_FIELD_SIZE = 32;

    final int actor;
    final int messageId;
    final Producer producer;

    volatile String threadId;
    volatile Long msgDelay;
    volatile InFlightMessage message;

    volatile Boolean created;
    volatile Boolean msgSent;
    volatile Boolean msgComplete;
    volatile Boolean destroyed;
    volatile Boolean retired;
    volatile Throwable fault;

    public State(int actor, int messageId, Producer producer) {
      this.actor = actor;
      this.messageId = messageId;
      this.producer = producer;
    }

    @SuppressWarnings("unused")
    public void reset() {
      if (fault != null) {
        throw new IllegalStateException("Cannot reset when faulted", fault);
      }
      created = null;
      msgSent = null;
      msgComplete = null;
      destroyed = null;
      retired = null;
    }

    @SuppressWarnings("unused")
    public boolean isClean() {
      return fault == null
          && (created == null || created)
          && (msgSent == null || msgSent)
          && (msgComplete == null || msgComplete)
          && (destroyed == null || destroyed)
          && (retired == null || retired);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      State state = (State)o;
      return actor == state.actor && messageId == state.messageId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(actor, messageId);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("threadId='");
      int threadIdLength = threadId.length();
      if (threadIdLength > THREAD_ID_FIELD_SIZE) {
        sb.append("...").append(threadId.substring(threadIdLength + 3 - THREAD_ID_FIELD_SIZE)).append('\'');
      } else {
        sb.append(threadId).append('\'');
        if (threadIdLength < THREAD_ID_FIELD_SIZE) {
          sb.append(new String(new char[THREAD_ID_FIELD_SIZE - threadIdLength]).replace('\0', ' '));
        }
      }

      if (msgDelay != null) {
        sb.append(", msgDelay=").append(String.format("%4d", msgDelay));
      }
      if (created != null) {
        sb.append(", created=").append(created);
        if (created) sb.append(' ');
      }
      if (msgSent != null) {
        sb.append(", msgSent=").append(msgSent);
        if (msgSent) sb.append(' ');
      }
      if (msgComplete != null) {
        sb.append(", msgComplete=").append(msgComplete);
        if (msgComplete) sb.append(' ');
      }
      if (destroyed != null) {
        sb.append(", destroyed=").append(destroyed);
        if (destroyed) sb.append(' ');
      }
      if (retired != null) {
        sb.append(", retired=").append(retired);
        if (retired) sb.append(' ');
      }
      if (fault != null) {
        sb.append(", fault=").append(fault);
      }
      return sb.toString();
    }
  }

  private abstract static class Delayable implements Delayed {

    private final long expiration;

    public Delayable(long expiration) {
      this.expiration = expiration;
    }

    public Delayable(Duration delay) {
      this.expiration = System.nanoTime() + delay.toNanos();
    }

    public long expiration() {
      return expiration;
    }

    protected long currentDelay() {
      return expiration - System.nanoTime();
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(currentDelay(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      return Long.compare(currentDelay(), o.getDelay(TimeUnit.NANOSECONDS));
    }
  }

  private static final class DelayableRunnable extends Delayable implements Runnable {
    private final Runnable task;

    public DelayableRunnable(long expiration) {
      super(expiration + 1000);
      this.task = null;
    }

    public DelayableRunnable(Duration delay, Runnable task) {
      super(delay);
      this.task = task;
    }

    public boolean isMarker() {
      return task == null;
    }

    @Override
    public void run() {
      if (task != null) {
        task.run();
      } else {
        throw new IllegalStateException();
      }
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("DelayableRunnable{");
      sb.append("expiration=").append(expiration());
      sb.append(", marker=").append(isMarker());
      sb.append('}');
      return sb.toString();
    }
  }

  private static final class TestClientMessageChannel implements ClientMessageChannel {

    private final ClientID clientID;
    private final AtomicReference<ClientEntityManager> managerRef = new AtomicReference<>();
    private final byte[] resultObject;
    private final EntityException resultException = null;

    public TestClientMessageChannel(ClientID clientID) {
      this.clientID = clientID;

      byte[] resultObject = new byte[8];
      ByteBuffer.wrap(resultObject).putLong(1L);
      this.resultObject = resultObject;
    }

    public void setManager(ClientEntityManager manager) {
      if (!managerRef.compareAndSet(null, manager)) {
        System.err.format("[%s] ClientEntityManager already set%n", Thread.currentThread());
      }
    }

    private ClientEntityManager manager() {
      ClientEntityManager localManager = managerRef.get();
      if (localManager == null) {
        throw new AssertionError("[" + Thread.currentThread() + "] ClientEntityManager not set");
      }
      return localManager;
    }

    @Override
    public ClientID getClientID() {
      return clientID;
    }

    @Override
    public ProductID getProductID() {
      return ProductID.STRIPE;
    }

    @Override
    public TCMessage createMessage(TCMessageType type) {
      if (type == TCMessageType.VOLTRON_ENTITY_MESSAGE) {
        return new TestRequestBatchMessage(manager(), resultObject, resultException, false);
      } else {
        return null;
      }
    }

    @Override
    public short getStackLayerFlag() {
      throw new UnsupportedOperationException("TestClientMessageChannel.getStackLayerFlag not implemented");
      // return 0;
    }

    @Override
    public String getStackLayerName() {
      throw new UnsupportedOperationException("TestClientMessageChannel.getStackLayerName not implemented");
      // return null;
    }

    @Override
    public void setSendLayer(NetworkLayer layer) {
      throw new UnsupportedOperationException("TestClientMessageChannel.setSendLayer not implemented");

    }

    @Override
    public void setReceiveLayer(NetworkLayer layer) {
      throw new UnsupportedOperationException("TestClientMessageChannel.setReceiveLayer not implemented");

    }

    @Override
    public NetworkLayer getReceiveLayer() {
      throw new UnsupportedOperationException("TestClientMessageChannel.getReceiveLayer not implemented");
      // return null;
    }

    @Override
    public void receive(TCByteBuffer[] msgData) {
      throw new UnsupportedOperationException("TestClientMessageChannel.receive not implemented");

    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException("TestClientMessageChannel.reset not implemented");

    }

    @Override
    public int getConnectCount() {
      throw new UnsupportedOperationException("TestClientMessageChannel.getConnectCount not implemented");
      // return 0;
    }

    @Override
    public int getConnectAttemptCount() {
      throw new UnsupportedOperationException("TestClientMessageChannel.getConnectAttemptCount not implemented");
      // return 0;
    }

    @Override
    public void setMessageTransportInitiator(MessageTransportInitiator initiator) {
      throw new UnsupportedOperationException("TestClientMessageChannel.setMessageTransportInitiator not implemented");

    }

    @Override
    public void addClientConnectionErrorListener(ClientConnectionErrorListener errorListener) {
      throw new UnsupportedOperationException(
          "TestClientMessageChannel.addClientConnectionErrorListener not implemented");

    }

    @Override
    public void removeClientConnectionErrorListener(ClientConnectionErrorListener errorListener) {
      throw new UnsupportedOperationException(
          "TestClientMessageChannel.removeClientConnectionErrorListener not implemented");

    }

    @Override
    public void onError(InetSocketAddress serverAddress, Exception e) {
      throw new UnsupportedOperationException("TestClientMessageChannel.onError not implemented");

    }

    @Override
    public void notifyTransportConnected(MessageTransport transport) {
      throw new UnsupportedOperationException("TestClientMessageChannel.notifyTransportConnected not implemented");

    }

    @Override
    public void notifyTransportDisconnected(MessageTransport transport, boolean forcedDisconnect) {
      throw new UnsupportedOperationException("TestClientMessageChannel.notifyTransportDisconnected not implemented");

    }

    @Override
    public void notifyTransportConnectAttempt(MessageTransport transport) {
      throw new UnsupportedOperationException("TestClientMessageChannel.notifyTransportConnectAttempt not implemented");

    }

    @Override
    public void notifyTransportClosed(MessageTransport transport) {
      throw new UnsupportedOperationException("TestClientMessageChannel.notifyTransportClosed not implemented");

    }

    @Override
    public void notifyTransportReconnectionRejected(MessageTransport transport) {
      throw new UnsupportedOperationException(
          "TestClientMessageChannel.notifyTransportReconnectionRejected not implemented");

    }

    @Override
    public InetSocketAddress getLocalAddress() {
      throw new UnsupportedOperationException("TestClientMessageChannel.getLocalAddress not implemented");
      // return null;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
      throw new UnsupportedOperationException("TestClientMessageChannel.getRemoteAddress not implemented");
      // return null;
    }

    @Override
    public void addListener(ChannelEventListener listener) {
      throw new UnsupportedOperationException("TestClientMessageChannel.addListener not implemented");

    }

    @Override
    public boolean isOpen() {
      throw new UnsupportedOperationException("TestClientMessageChannel.isOpen not implemented");
      // return false;
    }

    @Override
    public boolean isClosed() {
      throw new UnsupportedOperationException("TestClientMessageChannel.isClosed not implemented");
      // return false;
    }

    @Override
    public Object getAttachment(String key) {
      throw new UnsupportedOperationException("TestClientMessageChannel.getAttachment not implemented");
      // return null;
    }

    @Override
    public void addAttachment(String key, Object value, boolean replace) {
      throw new UnsupportedOperationException("TestClientMessageChannel.addAttachment not implemented");

    }

    @Override
    public Object removeAttachment(String key) {
      throw new UnsupportedOperationException("TestClientMessageChannel.removeAttachment not implemented");
      // return null;
    }

    @Override
    public boolean isConnected() {
      throw new UnsupportedOperationException("TestClientMessageChannel.isConnected not implemented");
      // return false;
    }

    @Override
    public void send(TCNetworkMessage message) {
      throw new UnsupportedOperationException("TestClientMessageChannel.send not implemented");

    }

    @Override
    public NetworkStackID open(InetSocketAddress serverAddress) {
      throw new UnsupportedOperationException("TestClientMessageChannel.open not implemented");
      // return null;
    }

    @Override
    public NetworkStackID open(Iterable<InetSocketAddress> serverAddresses) {
      throw new UnsupportedOperationException("TestClientMessageChannel.open not implemented");
      // return null;
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException("TestClientMessageChannel.close not implemented");

    }

    @Override
    public NodeID getLocalNodeID() {
      throw new UnsupportedOperationException("TestClientMessageChannel.getLocalNodeID not implemented");
      // return null;
    }

    @Override
    public void setLocalNodeID(NodeID source) {
      throw new UnsupportedOperationException("TestClientMessageChannel.setLocalNodeID not implemented");

    }

    @Override
    public NodeID getRemoteNodeID() {
      throw new UnsupportedOperationException("TestClientMessageChannel.getRemoteNodeID not implemented");
      // return null;
    }

    @Override
    public ConnectionID getConnectionID() {
      throw new UnsupportedOperationException("TestClientMessageChannel.getConnectionID not implemented");
      // return null;
    }

    @Override
    public ChannelID getChannelID() {
      throw new UnsupportedOperationException("TestClientMessageChannel.getChannelID not implemented");
      // return null;
    }
  }
}
