/*
 *
 *  The contents of this file are subject to the Terracotta Public License Version
 *  2.0 (the "License"); You may not use this file except in compliance with the
 *  License. You may obtain a copy of the License at
 *
 *  http://terracotta.org/legal/terracotta-public-license.
 *
 *  Software distributed under the License is distributed on an "AS IS" basis,
 *  WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
 *  the specific language governing rights and limitations under the License.
 *
 *  The Covered Software is Terracotta Core.
 *
 *  The Initial Developer of the Covered Software is
 *  Terracotta, Inc., a Software AG company
 *
 */
package org.terracotta.testing.rules;


import com.tc.util.LoggingOutputStream;
import com.tc.util.runtime.ThreadDumpUtil;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;

import java.io.PrintStream;


/**
 * This test is similar to BasicExternalClusterClassRuleIT, in that it uses the class rule to perform a basic test.
 * 
 * In this case, the basic test is watching how Galvan handles interaction with a basic active-passive cluster.
 */
// XXX: Currently ignored since this test depends on restartability of the server, which now requires a persistence service
//  to be plugged in (and there isn't one available, in open source).
public class ClientLeakIT {
  @ClassRule
  public static final Cluster CLUSTER = BasicExternalClusterBuilder.newCluster(1).build(); //logConfigExtensionResourceName("custom-logback-ext.xml").build();

  private final Logger logger = LoggerFactory.getLogger(ClientLeakIT.class);
  private final PrintStream loggingStream = new PrintStream(new LoggingOutputStream(logger, Level.INFO));

  @Rule
  public final TestName testName = new TestName();

  @Before
  public void preDumpThreads() {
    logger.info("Dumping threads BEFORE {}", testName.getMethodName());
    loggingStream.println(ThreadDumpUtil.getThreadDump());
    loggingStream.flush();
  }

  @After
  public void postDumpThreads() {
    logger.info("Dumping threads AFTER {}", testName.getMethodName());
    loggingStream.println(ThreadDumpUtil.getThreadDump());
    loggingStream.flush();
  }

  /**
   * This will ensure that a fail-over correctly happens.
   */
  @Test
  public void testClientLeakIsCleaned() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    Connection leak = CLUSTER.newConnection();
    CLUSTER.getClusterControl().terminateActive();
    Thread reconnect = lookForConnectionEstablisher();
    while (reconnect == null) {
      Thread.sleep(1000);
      reconnect = lookForConnectionEstablisher();
    }
    leak = null;
    while (reconnect.isAlive()) {
      System.out.println("reconnect thread " + reconnect.getName() + " is alive " + reconnect.isAlive());
      System.gc();
      Thread.sleep(1000);
    }
  }

  @Test
  public void testConnectionEstablisherDiesAfterJob() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    Connection leak = CLUSTER.newConnection();
    CLUSTER.getClusterControl().terminateActive();
    Thread reconnect = lookForConnectionEstablisher();
    while (reconnect == null) {
      Thread.sleep(1000);
      reconnect = lookForConnectionEstablisher();
    }
    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForActive();
    while (reconnect.isAlive()) {
      System.out.println("reconnect thread " + reconnect.getName() + " is alive " + reconnect.isAlive());
      System.gc();
      Thread.sleep(1000);
    }
  }
  
  @Test
  public void testConnectionMakerDiesWithNoRef() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().terminateAllServers();
    Thread target = Thread.currentThread();
    new Thread(()->{
      try {
        Thread maker;
        while ((maker = lookForConnectionMaker()) == null) {
          Thread.sleep(1000);
        }
        logger.info("Connection Maker found; maker={}", maker);
        Thread.sleep(1000);
        logger.info("Connection Maker found; interrupting {}", target);
        target.interrupt();
      } catch (InterruptedException ie) {
        
      }
    }).start();
    Connection leak = null;
    try {
      leak = CLUSTER.newConnection();
    } catch (ConnectionException ce) {
      // expected
    }
    assertNull(leak);
    Thread maker = lookForConnectionMaker();
    logger.info("Checking Connection Maker {}", maker);
    if (maker != null) {
      for (int x=0;x<500 && maker.isAlive();x++) {
        System.gc();
        Exception printer = new Exception("trying to join:" + x);
        printer.setStackTrace(maker.getStackTrace());
        printer.printStackTrace();
        maker.join(1000);
      }
      try {
        assertFalse(maker.isAlive());
      } catch (AssertionError e) {
        try {
          logger.info("Connection Maker not terminated {}", maker);
          loggingStream.println(ThreadDumpUtil.getThreadDump());
        } catch (Exception ex) {
          e.addSuppressed(ex);
        }
        throw e;
      }
    }
  }
  
  private static Thread lookForConnectionMaker() {
    Thread[] list = new Thread[Thread.activeCount()];
    Thread.enumerate(list);
    for (Thread t : list) {
      if (t.getName().startsWith("Connection Maker")) {
        return t;
      }
    }
    return null;
  }  
  
  private static Thread lookForConnectionEstablisher() {
    Thread[] list = new Thread[Thread.activeCount()];
    Thread.enumerate(list);
    for (Thread t : list) {
      if (t != null && t.getName().startsWith("ConnectionEstablisher")) {
        return t;
      }
    }
    return null;
  }
}
