/*
 * Copyright (c) 2003-2006 Terracotta, Inc. All rights reserved.
 */
package com.tc.object.msg;

import com.tc.bytes.TCByteBuffer;
import com.tc.io.TCByteBufferOutput;
import com.tc.io.TCSerializable;
import com.tc.net.protocol.tcm.MessageChannel;
import com.tc.net.protocol.tcm.MessageMonitor;
import com.tc.net.protocol.tcm.TCMessageHeader;
import com.tc.net.protocol.tcm.TCMessageType;
import com.tc.object.ObjectID;
import com.tc.object.lockmanager.api.LockContext;
import com.tc.object.lockmanager.api.WaitContext;
import com.tc.object.session.SessionID;
import com.tc.object.tx.TransactionID;
import com.tc.util.SequenceID;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ClientHandshakeMessageImpl extends DSOMessageBase implements ClientHandshakeMessage {

  private static final byte MANAGED_OBJECT_ID        = 1;
  private static final byte WAIT_CONTEXT             = 2;
  private static final byte LOCK_CONTEXT             = 3;
  private static final byte TRANSACTION_SEQUENCE_IDS = 4;
  private static final byte PENDING_LOCK_CONTEXT     = 5;
  private static final byte RESENT_TRANSACTION_IDS   = 6;
  private static final byte REQUEST_OBJECT_IDS       = 7;

  private final Set         objectIDs                = new HashSet();
  private final Set         lockContexts             = new HashSet();
  private final Set         waitContexts             = new HashSet();
  private final Set         pendingLockContexts      = new HashSet();
  private final Set         sequenceIDs              = new HashSet();
  private final Set         txnIDs                   = new HashSet();
  private boolean           requestObjectIDs;

  public ClientHandshakeMessageImpl(MessageMonitor monitor, TCByteBufferOutput out, MessageChannel channel,
                                    TCMessageType messageType) {
    super(monitor, out, channel, messageType);
  }

  public ClientHandshakeMessageImpl(SessionID sessionID, MessageMonitor monitor, MessageChannel channel,
                                    TCMessageHeader header, TCByteBuffer[] data) {
    super(sessionID, monitor, channel, header, data);
  }

  public void addObjectID(ObjectID objectID) {
    synchronized (objectIDs) {
      objectIDs.add(objectID);
    }
  }

  public void addLockContext(LockContext ctxt) {
    synchronized (lockContexts) {
      lockContexts.add(ctxt);
    }
  }

  public Collection getLockContexts() {
    synchronized (lockContexts) {
      return new HashSet(lockContexts);
    }
  }

  public void addPendingLockContext(LockContext ctxt) {
    synchronized (pendingLockContexts) {
      pendingLockContexts.add(ctxt);
    }
  }

  public Collection getPendingLockContexts() {
    synchronized (pendingLockContexts) {
      return new HashSet(pendingLockContexts);
    }
  }

  public void addWaitContext(WaitContext ctxt) {
    synchronized (waitContexts) {
      waitContexts.add(ctxt);
    }
  }

  public Collection getWaitContexts() {
    synchronized (waitContexts) {
      return new HashSet(waitContexts);
    }
  }

  public Collection getObjectIDs() {
    synchronized (objectIDs) {
      return new HashSet(objectIDs);
    }
  }

  protected void dehydrateValues() {
    for (Iterator i = objectIDs.iterator(); i.hasNext();) {
      putNVPair(MANAGED_OBJECT_ID, ((ObjectID) i.next()).toLong());
    }
    for (Iterator i = lockContexts.iterator(); i.hasNext();) {
      putNVPair(LOCK_CONTEXT, (TCSerializable) i.next());
    }
    for (Iterator i = waitContexts.iterator(); i.hasNext();) {
      putNVPair(WAIT_CONTEXT, (TCSerializable) i.next());
    }
    for (Iterator i = pendingLockContexts.iterator(); i.hasNext();) {
      putNVPair(PENDING_LOCK_CONTEXT, (TCSerializable) i.next());
    }
    for (Iterator i = sequenceIDs.iterator(); i.hasNext();) {
      putNVPair(TRANSACTION_SEQUENCE_IDS, ((SequenceID) i.next()).toLong());
    }
    for (Iterator i = txnIDs.iterator(); i.hasNext();) {
      putNVPair(RESENT_TRANSACTION_IDS, ((TransactionID) i.next()).toLong());
    }
    putNVPair(REQUEST_OBJECT_IDS, this.requestObjectIDs);
  }

  protected boolean hydrateValue(byte name) throws IOException {
    switch (name) {
      case MANAGED_OBJECT_ID:
        objectIDs.add(new ObjectID(getLongValue()));
        return true;
      case LOCK_CONTEXT:
        lockContexts.add(getObject(new LockContext()));
        return true;
      case WAIT_CONTEXT:
        waitContexts.add(getObject(new WaitContext()));
        return true;
      case PENDING_LOCK_CONTEXT:
        pendingLockContexts.add(getObject(new LockContext()));
        return true;
      case TRANSACTION_SEQUENCE_IDS:
        sequenceIDs.add(new SequenceID(getLongValue()));
        return true;
      case RESENT_TRANSACTION_IDS:
        txnIDs.add(new TransactionID(getLongValue()));
        return true;
      case REQUEST_OBJECT_IDS:
        this.requestObjectIDs = getBooleanValue();
        return true;
      default:
        return false;
    }
  }

  public Collection getTransactionSequenceIDs() {
    return sequenceIDs;
  }

  public Collection getResentTransactionIDs() {
    return txnIDs;
  }

  public void setTransactionSequenceIDs(Collection seqIDs) {
    this.sequenceIDs.addAll(seqIDs);
  }

  public void setResentTransactionIDs(Collection resentTransactionIDs) {
    this.txnIDs.addAll(resentTransactionIDs);
  }

  public void setIsObjectIDsRequested(boolean request) {
    this.requestObjectIDs = request;
  }

  public boolean isObjectIDsRequested() {
    return this.requestObjectIDs;
  }
}
