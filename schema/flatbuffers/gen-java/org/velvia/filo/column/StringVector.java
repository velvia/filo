// automatically generated, do not modify

package org.velvia.filo.column;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

public class StringVector extends Table {
  public static StringVector getRootAsStringVector(ByteBuffer _bb) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (new StringVector()).__init(_bb.getInt(_bb.position()) + _bb.position(), _bb); }
  public StringVector __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String data(int j) { int o = __offset(4); return o != 0 ? __string(__vector(o) + j * 4) : null; }
  public int dataLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer dataAsByteBuffer() { return __vector_as_bytebuffer(4, 4); }

  public static int createStringVector(FlatBufferBuilder builder,
      int data) {
    builder.startObject(1);
    StringVector.addData(builder, data);
    return StringVector.endStringVector(builder);
  }

  public static void startStringVector(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addOffset(0, dataOffset, 0); }
  public static int createDataVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startDataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endStringVector(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

