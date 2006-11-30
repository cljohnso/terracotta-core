/*
 * Copyright (c) 2003-2006 Terracotta, Inc. All rights reserved.
 */
package com.tc.io.serializer.impl;

import com.tc.io.serializer.api.Serializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Character
 */
public final class CharacterSerializer implements Serializer {

  public void serializeTo(Object o, ObjectOutput out) throws IOException {
    out.writeChar(((Character)o).charValue());
  }

  public Object deserializeFrom(ObjectInput in) throws IOException {
    return new Character(in.readChar());
  }

  public byte getSerializerID() {
    return CHARACTER;
  }
  
}