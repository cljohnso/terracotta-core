/*
 * Copyright (c) 2011-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 * Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG.
 */
package com.tc.object;

import org.junit.Assert;
import org.mockito.Mockito;
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
import com.tc.net.protocol.tcm.ClientMessageChannel;
import com.tc.net.protocol.tcm.MessageChannel;
import com.tc.net.protocol.tcm.TCMessage;
import com.tc.net.protocol.tcm.TCMessageType;
import com.tc.object.session.SessionID;
import com.tc.object.tx.TransactionID;
import com.tc.util.ProductID;
import com.tc.util.TCAssertionError;

import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.time.Duration;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import junit.framework.TestCase;

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

  public void testBulkMessageWithShutdown() {
    int clientCount = 5;
    int actorCount = 100;
    Duration testDuration = Duration.ofSeconds(45L);

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
      Duration waitLimit = testDuration.multipliedBy(150L).dividedBy(100L);
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

  private static void stateDump(Set<State> attempts, @SuppressWarnings("SameParameterValue") boolean fullState) {
    if (fullState) {
      for (State attempt : attempts) {
        System.out.format("attempt[%4d] = {%s}%n", attempt.actor, attempt);
      }
    } else {
      boolean allClean = true;
      for (State attempt : attempts) {
        if (!attempt.isClean()) {
          System.out.format("attempt[%4d] = {%s}%n", attempt.actor, attempt);
          allClean = false;
        }
      }
      if (allClean) {
        System.out.format("All attempts clean");
      }
    }
  }

  private static final class Client implements Runnable {
    private static final AtomicInteger CLIENT_COUNTER = new AtomicInteger(0);

    private final int clientNumber = CLIENT_COUNTER.incrementAndGet();
    private final ClientInstanceID clientInstance = new ClientInstanceID(clientNumber);

    private final ThreadGroup clientThreadGroup;

    private final Duration testDuration;
    private final int actorCount;
    private final int finishThreadCount;
    private final Random rnd = new Random();

    private final ClientMessageChannel channel;
    private final ClientEntityManager manager;
    private final long startTime;
    private final SingleUseGate dumpGate;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Client(long startTime, Duration testDuration, int actorCount, SingleUseGate dumpGate) {
      this.startTime = startTime;
      this.testDuration = testDuration;
      this.actorCount = actorCount;
      this.dumpGate = dumpGate;
      this.finishThreadCount = Math.max(1, this.actorCount * 5 / 100);

      this.clientThreadGroup = new ThreadGroup("client-" + clientNumber);
      this.clientThreadGroup.setDaemon(true);

      this.channel = mock(ClientMessageChannel.class);
      when(this.channel.getProductID()).thenReturn(ProductID.STRIPE);
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
      this.manager = new ClientEntityManagerImpl(this.channel, stageMgr);
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void run() {

      byte[] resultObject = new byte[8];
      ByteBuffer.wrap(resultObject).putLong(1L);
      EntityException resultException = null;
      when(channel.createMessage(Mockito.eq(TCMessageType.VOLTRON_ENTITY_MESSAGE))).then(
          (Answer<TCMessage>)invocation -> new TestRequestBatchMessage(manager, resultObject, resultException, false));

      ExecutorService messageService =
          Executors.newCachedThreadPool(new PurposedThreadFactory(clientThreadGroup, "actor"));
      ((ThreadPoolExecutor)messageService).setCorePoolSize(1 + actorCount * 50 / 100);
      ((ThreadPoolExecutor)messageService).prestartAllCoreThreads();

      ScheduledExecutorService timerService =
          Executors.newScheduledThreadPool(1 + actorCount * 20 / 100, new PurposedThreadFactory(clientThreadGroup, "timer"));
      ((ThreadPoolExecutor)timerService).prestartAllCoreThreads();

      ExecutorService finishService =
          Executors.newFixedThreadPool(finishThreadCount, new PurposedThreadFactory(clientThreadGroup, "finisher"));

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
        DelayQueue<DelayedState> finishQueue = new DelayQueue<>();
        AtomicInteger messageId = new AtomicInteger();
        for (int i = 0; i < actorCount; i++) {
          int actorId = i;
          Producer producer = new Producer(actorId);

          Future<Producer> future = messageService.submit(() -> {
            Thread currentThread = Thread.currentThread();
            producer.setThreadId(currentThread);

            while (!stopped.get() && !Thread.interrupted()) {
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

              state.msgDelay = (long)rnd.nextInt(250);
              if (VERBOSE)
                message("Scheduling InFlightMessage %d:%d %s to complete after %dms%n",
                    state.actor, state.messageId, inFlightMessage.getTransactionID(), state.msgDelay);
              timerService.schedule(() -> {
                ((TestRequestBatchMessage)inFlightMessage.getMessage()).explicitComplete(new byte[0], null, false);
                completedMessageCount.increment();
                producer.completedMessages.incrementAndGet();
                if (VERBOSE)
                  message("Completed InFlightMessage %d:%d %s after %dms%n",
                      state.actor, state.messageId, inFlightMessage.getTransactionID(), state.msgDelay);

                finishQueue.add(new DelayedState(state));
              }, state.msgDelay, TimeUnit.MILLISECONDS);

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
         * Now crank up a few threads to retire completed messages.
         */
        List<Future<?>> finishTasks = new ArrayList<>();
        for (int i = 0; i < finishThreadCount; i++) {
          finishTasks.add(finishService.submit(() -> {
            try {
              while (!stopped.get() && !Thread.interrupted()) {
                DelayedState delayedState = finishQueue.take();
                State state = delayedState.state;
                InFlightMessage inFlightMessage = state.message;
                if (inFlightMessage != null)
                  if (!inFlightMessage.isDone()) {
                    if (VERBOSE)
                      message("Retiring InFlightMessage %d:%d %s%n",
                          state.actor, state.messageId, inFlightMessage.getTransactionID());
                    try {
                      manager.retired(inFlightMessage.getTransactionID());
                      state.retired = true;
                      state.producer.retiredMessages.incrementAndGet();
                      if (retiredMessageCount.incrementAndGet() % 10000 == 0)  {
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
              }
            } catch (InterruptedException e) {
              message("Finish thread interrupted%n");
            }
          }));
        }

        message("Snoozing until test duration (%s) expires ...%n", testDuration);
        try {
          TimeUnit.NANOSECONDS.sleep(testDuration.toNanos());
        } catch (InterruptedException e) {
          message("Test truncated by interruption%n");
          Thread.currentThread().interrupt();
        }

        message("Attempting manager.shutdown%n");
        manager.shutdown();
        message("Manager.shutdown successful%n");

        stopped.set(true);

        boolean testFault = false;

        message("Awaiting completion of finish tasks; %d queued ...%n", finishQueue.size());
        for (Iterator<Future<?>> iterator = finishTasks.iterator(); iterator.hasNext(); ) {
          Future<?> task = iterator.next();
          try {
            task.get(500L, TimeUnit.MILLISECONDS);
            iterator.remove();
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            message("Finish task failed:%n");
            cause.printStackTrace(System.out);
            iterator.remove();
          } catch (InterruptedException e) {
            message("Wait for finish task interrupted%n");
            Thread.currentThread().interrupt();
            break;
          } catch (TimeoutException e) {
            message("Failed to fetch finish task%n");
            testFault = true;
          }
        }

        Calendar threadsInstant = Calendar.getInstance();
        List<Thread> allThreads = Arrays.asList(Diagnostics.getAllThreads());
        List<Thread> threadsToDump = new ArrayList<>();

        message("Awaiting completion of producer tasks ...%n");
        int closeObservedCount = 0;
        try {
          for (Iterator<Producer> iterator = producers.iterator(); iterator.hasNext(); ) {
            Producer producer = iterator.next();
            Future<Producer> task = producer.future();
            try {
              Producer stats = task.get(500L, TimeUnit.MILLISECONDS);
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
              message("Timed out attempting to complete Producer %d:* \"%s\"%n", producer.actorId, producer.threadId);
              testFault = true;

              allThreads.stream()
                  .filter(t -> t.toString().equals(producer.threadId))
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
            message("%d stalled threads encountered%n", threadsToDump.size());
            System.out.format("%nSelect thread dump %tF %<tT.%<tL %<tz%n", threadsInstant);
            Diagnostics.dumpThreads(threadsToDump, true, System.out);
          }
          fail("Stalled threads detected");
        }

      } finally {
        timerService.shutdownNow();
        messageService.shutdownNow();
      }
    }

    private void message(String format, Object... args) {
      synchronized (System.out) {
        System.out.format("%tM:%<tS.%<tL %4d [%s] ", System.currentTimeMillis() - startTime, clientNumber, Thread.currentThread());
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

  private static final class Producer {
    private final int actorId;
    final AtomicInteger sentMessages = new AtomicInteger(0);
    final AtomicInteger completedMessages = new AtomicInteger(0);
    final AtomicInteger fetchedMessages = new AtomicInteger(0);
    final AtomicInteger retiredMessages = new AtomicInteger(0);

    private volatile Future<Producer> producerFuture;
    private volatile String threadId;

    Producer(int actorId) {
      this.actorId = actorId;
    }

    void setFuture(Future<Producer> future) {
      this.producerFuture = future;
    }

    public Future<Producer> future() {
      return producerFuture;
    }

    void setThreadId(Thread thread) {
      this.threadId = thread.toString();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("{");
      sb.append("actor=").append(actorId);
      sb.append(", threadId='").append(threadId).append('\'');
      sb.append(", sentMessages=").append(sentMessages);
      sb.append(", completedMessages=").append(completedMessages);
      sb.append(", fetchedMessages=").append(fetchedMessages);
      sb.append(", retiredMessages=").append(retiredMessages);
      sb.append('}');
      return sb.toString();
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

  private static class DelayedState implements Delayed {

    private static final TimeUnit TIME_UNIT = TimeUnit.NANOSECONDS;
    private final long nanoDelay;
    private final State state;

    private DelayedState(State state) {
      this.state = state;
      this.nanoDelay = 0;
//      this.nanoDelay = ThreadLocalRandom.current().nextLong(50);
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(nanoDelay, TIME_UNIT);
    }

    @Override
    public int compareTo(Delayed o) {
      return Long.compare(this.nanoDelay, o.getDelay(TIME_UNIT));
    }
  }
}
