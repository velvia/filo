// automatically generated, do not modify

package org.velvia.filo.vector;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class ConstStringVector extends Table {
  public static ConstStringVector getRootAsConstStringVector(ByteBuffer _bb) { return getRootAsConstStringVector(_bb, new ConstStringVector()); }
  public static ConstStringVector getRootAsConstStringVector(ByteBuffer _bb, ConstStringVector obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public ConstStringVector __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public int len() { int o = __offset(4); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public NaMask naMask() { return naMask(new NaMask()); }
  public NaMask naMask(NaMask obj) { int o = __offset(6); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public String str() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer strAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }

  public static int createConstStringVector(FlatBufferBuilder builder,
      int len,
      int naMask,
      int str) {
    builder.startObject(3);
    ConstStringVector.addStr(builder, str);
    ConstStringVector.addNaMask(builder, naMask);
    ConstStringVector.addLen(builder, len);
    return ConstStringVector.endConstStringVector(builder);
  }

  public static void startConstStringVector(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addLen(FlatBufferBuilder builder, int len) { builder.addInt(0, len, 0); }
  public static void addNaMask(FlatBufferBuilder builder, int naMaskOffset) { builder.addOffset(1, naMaskOffset, 0); }
  public static void addStr(FlatBufferBuilder builder, int strOffset) { builder.addOffset(2, strOffset, 0); }
  public static int endConstStringVector(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishConstStringVectorBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

