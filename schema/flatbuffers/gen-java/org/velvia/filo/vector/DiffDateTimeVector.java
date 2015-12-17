// automatically generated, do not modify

package org.velvia.filo.vector;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class DiffDateTimeVector extends Table {
  public static DiffDateTimeVector getRootAsDiffDateTimeVector(ByteBuffer _bb) { return getRootAsDiffDateTimeVector(_bb, new DiffDateTimeVector()); }
  public static DiffDateTimeVector getRootAsDiffDateTimeVector(ByteBuffer _bb, DiffDateTimeVector obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public DiffDateTimeVector __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public NaMask naMask() { return naMask(new NaMask()); }
  public NaMask naMask(NaMask obj) { int o = __offset(4); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public DDTVars vars() { return vars(new DDTVars()); }
  public DDTVars vars(DDTVars obj) { int o = __offset(6); return o != 0 ? obj.__init(o + bb_pos, bb) : null; }
  public DataInfo millisInfo() { return millisInfo(new DataInfo()); }
  public DataInfo millisInfo(DataInfo obj) { int o = __offset(8); return o != 0 ? obj.__init(o + bb_pos, bb) : null; }
  public int millis(int j) { int o = __offset(10); return o != 0 ? bb.get(__vector(o) + j * 1) & 0xFF : 0; }
  public int millisLength() { int o = __offset(10); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer millisAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }
  public DataInfo tzInfo() { return tzInfo(new DataInfo()); }
  public DataInfo tzInfo(DataInfo obj) { int o = __offset(12); return o != 0 ? obj.__init(o + bb_pos, bb) : null; }
  public int tz(int j) { int o = __offset(14); return o != 0 ? bb.get(__vector(o) + j * 1) & 0xFF : 0; }
  public int tzLength() { int o = __offset(14); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer tzAsByteBuffer() { return __vector_as_bytebuffer(14, 1); }

  public static void startDiffDateTimeVector(FlatBufferBuilder builder) { builder.startObject(6); }
  public static void addNaMask(FlatBufferBuilder builder, int naMaskOffset) { builder.addOffset(0, naMaskOffset, 0); }
  public static void addVars(FlatBufferBuilder builder, int varsOffset) { builder.addStruct(1, varsOffset, 0); }
  public static void addMillisInfo(FlatBufferBuilder builder, int millisInfoOffset) { builder.addStruct(2, millisInfoOffset, 0); }
  public static void addMillis(FlatBufferBuilder builder, int millisOffset) { builder.addOffset(3, millisOffset, 0); }
  public static int createMillisVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startMillisVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static void addTzInfo(FlatBufferBuilder builder, int tzInfoOffset) { builder.addStruct(4, tzInfoOffset, 0); }
  public static void addTz(FlatBufferBuilder builder, int tzOffset) { builder.addOffset(5, tzOffset, 0); }
  public static int createTzVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startTzVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static int endDiffDateTimeVector(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishDiffDateTimeVectorBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

