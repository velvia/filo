// automatically generated, do not modify

package org.velvia.filo.vector;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class DiffPrimitiveVector extends Table {
  public static DiffPrimitiveVector getRootAsDiffPrimitiveVector(ByteBuffer _bb) { return getRootAsDiffPrimitiveVector(_bb, new DiffPrimitiveVector()); }
  public static DiffPrimitiveVector getRootAsDiffPrimitiveVector(ByteBuffer _bb, DiffPrimitiveVector obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public DiffPrimitiveVector __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public int len() { int o = __offset(4); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public NaMask naMask() { return naMask(new NaMask()); }
  public NaMask naMask(NaMask obj) { int o = __offset(6); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public DataInfo baseInfo() { return baseInfo(new DataInfo()); }
  public DataInfo baseInfo(DataInfo obj) { int o = __offset(8); return o != 0 ? obj.__init(o + bb_pos, bb) : null; }
  public int base(int j) { int o = __offset(10); return o != 0 ? bb.get(__vector(o) + j * 1) & 0xFF : 0; }
  public int baseLength() { int o = __offset(10); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer baseAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }
  public DataInfo info() { return info(new DataInfo()); }
  public DataInfo info(DataInfo obj) { int o = __offset(12); return o != 0 ? obj.__init(o + bb_pos, bb) : null; }
  public int data(int j) { int o = __offset(14); return o != 0 ? bb.get(__vector(o) + j * 1) & 0xFF : 0; }
  public int dataLength() { int o = __offset(14); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer dataAsByteBuffer() { return __vector_as_bytebuffer(14, 1); }

  public static void startDiffPrimitiveVector(FlatBufferBuilder builder) { builder.startObject(6); }
  public static void addLen(FlatBufferBuilder builder, int len) { builder.addInt(0, len, 0); }
  public static void addNaMask(FlatBufferBuilder builder, int naMaskOffset) { builder.addOffset(1, naMaskOffset, 0); }
  public static void addBaseInfo(FlatBufferBuilder builder, int baseInfoOffset) { builder.addStruct(2, baseInfoOffset, 0); }
  public static void addBase(FlatBufferBuilder builder, int baseOffset) { builder.addOffset(3, baseOffset, 0); }
  public static int createBaseVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startBaseVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static void addInfo(FlatBufferBuilder builder, int infoOffset) { builder.addStruct(4, infoOffset, 0); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addOffset(5, dataOffset, 0); }
  public static int createDataVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startDataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static int endDiffPrimitiveVector(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishDiffPrimitiveVectorBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

