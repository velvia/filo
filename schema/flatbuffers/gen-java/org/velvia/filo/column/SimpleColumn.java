// automatically generated, do not modify

package org.velvia.filo.column;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

public class SimpleColumn extends Table {
  public static SimpleColumn getRootAsSimpleColumn(ByteBuffer _bb) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (new SimpleColumn()).__init(_bb.getInt(_bb.position()) + _bb.position(), _bb); }
  public SimpleColumn __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public NaMask naMask() { return naMask(new NaMask()); }
  public NaMask naMask(NaMask obj) { int o = __offset(4); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public byte vectorType() { int o = __offset(6); return o != 0 ? bb.get(o + bb_pos) : 0; }
  public Table vector(Table obj) { int o = __offset(8); return o != 0 ? __union(obj, o) : null; }

  public static int createSimpleColumn(FlatBufferBuilder builder,
      int naMask,
      byte vector_type,
      int vector) {
    builder.startObject(3);
    SimpleColumn.addVector(builder, vector);
    SimpleColumn.addNaMask(builder, naMask);
    SimpleColumn.addVectorType(builder, vector_type);
    return SimpleColumn.endSimpleColumn(builder);
  }

  public static void startSimpleColumn(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addNaMask(FlatBufferBuilder builder, int naMaskOffset) { builder.addOffset(0, naMaskOffset, 0); }
  public static void addVectorType(FlatBufferBuilder builder, byte vectorType) { builder.addByte(1, vectorType, 0); }
  public static void addVector(FlatBufferBuilder builder, int vectorOffset) { builder.addOffset(2, vectorOffset, 0); }
  public static int endSimpleColumn(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

