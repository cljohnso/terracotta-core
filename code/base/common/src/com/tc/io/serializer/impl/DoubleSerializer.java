/*
 * Copyright (c) 2003-2006 Terracotta, Inc. All rights reserved.
 */
package com.tc.io.serializer.impl;

import com.tc.io.serializer.api.Serializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Double
 */
public final class DoubleSerializer implements Serializer {

  public void serializeTo(Object o, ObjectOutput out) throws IOException {
    out.writeDouble(((Double)o).doubleValue());
  }

  public Object deserializeFrom(ObjectInput in) throws IOException {
    return new Double(in.readDouble());
  }

  public byte getSerializerID() {
    return DOUBLE;
  }
  
}