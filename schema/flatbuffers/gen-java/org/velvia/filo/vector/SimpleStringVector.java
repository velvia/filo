// automatically generated, do not modify

package org.velvia.filo.vector;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class SimpleStringVector extends Table {
  public static SimpleStringVector getRootAsSimpleStringVector(ByteBuffer _bb) { return getRootAsSimpleStringVector(_bb, new SimpleStringVector()); }
  public static SimpleStringVector getRootAsSimpleStringVector(ByteBuffer _bb, SimpleStringVector obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public SimpleStringVector __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public NaMask naMask() { return naMask(new NaMask()); }
  public NaMask naMask(NaMask obj) { int o = __offset(4); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public String data(int j) { int o = __offset(6); return o != 0 ? __string(__vector(o) + j * 4) : null; }
  public int dataLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }

  public static int createSimpleStringVector(FlatBufferBuilder builder,
      int naMask,
      int data) {
    builder.startObject(2);
    SimpleStringVector.addData(builder, data);
    SimpleStringVector.addNaMask(builder, naMask);
    return SimpleStringVector.endSimpleStringVector(builder);
  }

  public static void startSimpleStringVector(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addNaMask(FlatBufferBuilder builder, int naMaskOffset) { builder.addOffset(0, naMaskOffset, 0); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addOffset(1, dataOffset, 0); }
  public static int createDataVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startDataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endSimpleStringVector(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

