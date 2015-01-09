// automatically generated, do not modify

package org.velvia.filo.column;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

public class IntVector extends Table {
  public static IntVector getRootAsIntVector(ByteBuffer _bb) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (new IntVector()).__init(_bb.getInt(_bb.position()) + _bb.position(), _bb); }
  public IntVector __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public int data(int j) { int o = __offset(4); return o != 0 ? bb.getInt(__vector(o) + j * 4) : 0; }
  public int dataLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer dataAsByteBuffer() { return __vector_as_bytebuffer(4, 4); }

  public static int createIntVector(FlatBufferBuilder builder,
      int data) {
    builder.startObject(1);
    IntVector.addData(builder, data);
    return IntVector.endIntVector(builder);
  }

  public static void startIntVector(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addOffset(0, dataOffset, 0); }
  public static int createDataVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addInt(data[i]); return builder.endVector(); }
  public static void startDataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endIntVector(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

