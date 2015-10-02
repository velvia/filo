// automatically generated, do not modify

package org.velvia.filo.column;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class LongVector extends Table {
  public static LongVector getRootAsLongVector(ByteBuffer _bb) { return getRootAsLongVector(_bb, new LongVector()); }
  public static LongVector getRootAsLongVector(ByteBuffer _bb, LongVector obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public LongVector __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long data(int j) { int o = __offset(4); return o != 0 ? bb.getLong(__vector(o) + j * 8) : 0; }
  public int dataLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer dataAsByteBuffer() { return __vector_as_bytebuffer(4, 8); }

  public static int createLongVector(FlatBufferBuilder builder,
      int data) {
    builder.startObject(1);
    LongVector.addData(builder, data);
    return LongVector.endLongVector(builder);
  }

  public static void startLongVector(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addOffset(0, dataOffset, 0); }
  public static int createDataVector(FlatBufferBuilder builder, long[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addLong(data[i]); return builder.endVector(); }
  public static void startDataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static int endLongVector(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

