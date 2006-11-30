/*
 * Copyright (c) 2003-2006 Terracotta, Inc. All rights reserved.
 */
package com.tc.net.core;

import EDU.oswego.cs.dl.util.concurrent.CopyOnWriteArraySet;

import com.tc.logging.TCLogger;
import com.tc.logging.TCLogging;
import com.tc.net.TCSocketAddress;
import com.tc.net.core.event.TCListenerEvent;
import com.tc.net.core.event.TCListenerEventListener;
import com.tc.net.protocol.ProtocolAdaptorFactory;
import com.tc.util.Assert;
import com.tc.util.TCTimeoutException;
import com.tc.util.concurrent.SetOnceFlag;
import com.tc.util.concurrent.TCExceptionResultException;
import com.tc.util.concurrent.TCFuture;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Iterator;

/**
 * Base class for Listeners
 * 
 * @author teck
 */
abstract class AbstractTCListener implements TCListener {
  protected final static TCLogger      logger          = TCLogging.getLogger(TCListener.class);

  private final InetAddress            addr;
  private final int                    port;
  private final TCSocketAddress        sockAddr;

  private final TCListenerEvent        staticEvent;

  private final SetOnceFlag            closeEventFired = new SetOnceFlag();
  private final SetOnceFlag            stopPending     = new SetOnceFlag();
  private final SetOnceFlag            stopped         = new SetOnceFlag();

  private final CopyOnWriteArraySet    listeners       = new CopyOnWriteArraySet();

  private final ProtocolAdaptorFactory factory;

  AbstractTCListener(ServerSocket ss, ProtocolAdaptorFactory factory) {
    this.addr = ss.getInetAddress();
    this.port = ss.getLocalPort();
    this.sockAddr = new TCSocketAddress(this.addr, this.port);
    this.factory = factory;
    this.staticEvent = new TCListenerEvent() {
      public TCListener getSource() {
        return AbstractTCListener.this;
      }
    };
  }

  abstract protected void stopImpl(Runnable callback);

  public final void stop() {
    try {
      stop(0);
    } catch (Exception e) {
      logger.error("unexpected exception", e);
    }
  }

  public final TCSocketAddress getBindSocketAddress() {
    return sockAddr;
  }

  public final void stop(long timeout) throws TCTimeoutException {
    if (stopped.isSet()) {
      logger.warn("listener already stopped");
      return;
    }

    if (stopPending.attemptSet()) {
      final TCFuture future = new TCFuture();

      stopImpl(new Runnable() {
        public void run() {
          future.set("stop done");
        }
      });

      try {
        future.get(timeout);
      } catch (InterruptedException e) {
        logger.warn("stop interrupted");
        return;
      } catch (TCExceptionResultException e) {
        logger.error(e);
        Assert.eval("exception result set in future", false);
        return;
      } finally {
        fireCloseEvent();
        stopped.set();
      }
    } else {
      logger.warn("stop already requested");
    }
  }

  public final int getBindPort() {
    return port;
  }

  public final InetAddress getBindAddress() {
    return addr;
  }

  public final void addEventListener(TCListenerEventListener lsnr) {
    if (lsnr == null) {
      logger.warn("trying to add a null event listener");
      return;
    }

    listeners.add(lsnr);
  }

  public final void removeEventListener(TCListenerEventListener lsnr) {
    if (lsnr == null) {
      logger.warn("trying to remove a null event listener");
      return;
    }

    listeners.remove(lsnr);
  }

  public final boolean isStopped() {
    return stopped.isSet();
  }

  public final String toString() {
    return getClass().getName() + " " + addr.getHostAddress() + ":" + port;
  }

  protected final void fireCloseEvent() {
    if (closeEventFired.attemptSet()) {
      for (final Iterator iter = listeners.iterator(); iter.hasNext();) {
        final TCListenerEventListener lsnr = (TCListenerEventListener) iter.next();

        try {
          lsnr.closeEvent(staticEvent);
        } catch (Exception e) {
          logger.error("exception in close event handler", e);
        }
      }
    }
  }

  final ProtocolAdaptorFactory getProtocolAdaptorFactory() {
    return factory;
  }
}