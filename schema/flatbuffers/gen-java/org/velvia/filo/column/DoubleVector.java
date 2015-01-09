// automatically generated, do not modify

package org.velvia.filo.column;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

public class DoubleVector extends Table {
  public static DoubleVector getRootAsDoubleVector(ByteBuffer _bb) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (new DoubleVector()).__init(_bb.getInt(_bb.position()) + _bb.position(), _bb); }
  public DoubleVector __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public double data(int j) { int o = __offset(4); return o != 0 ? bb.getDouble(__vector(o) + j * 8) : 0; }
  public int dataLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer dataAsByteBuffer() { return __vector_as_bytebuffer(4, 8); }

  public static int createDoubleVector(FlatBufferBuilder builder,
      int data) {
    builder.startObject(1);
    DoubleVector.addData(builder, data);
    return DoubleVector.endDoubleVector(builder);
  }

  public static void startDoubleVector(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addOffset(0, dataOffset, 0); }
  public static int createDataVector(FlatBufferBuilder builder, double[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addDouble(data[i]); return builder.endVector(); }
  public static void startDataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static int endDoubleVector(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

