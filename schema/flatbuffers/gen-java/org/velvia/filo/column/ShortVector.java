// automatically generated, do not modify

package org.velvia.filo.column;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class ShortVector extends Table {
  public static ShortVector getRootAsShortVector(ByteBuffer _bb) { return getRootAsShortVector(_bb, new ShortVector()); }
  public static ShortVector getRootAsShortVector(ByteBuffer _bb, ShortVector obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public ShortVector __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public short data(int j) { int o = __offset(4); return o != 0 ? bb.getShort(__vector(o) + j * 2) : 0; }
  public int dataLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer dataAsByteBuffer() { return __vector_as_bytebuffer(4, 2); }

  public static int createShortVector(FlatBufferBuilder builder,
      int data) {
    builder.startObject(1);
    ShortVector.addData(builder, data);
    return ShortVector.endShortVector(builder);
  }

  public static void startShortVector(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addOffset(0, dataOffset, 0); }
  public static int createDataVector(FlatBufferBuilder builder, short[] data) { builder.startVector(2, data.length, 2); for (int i = data.length - 1; i >= 0; i--) builder.addShort(data[i]); return builder.endVector(); }
  public static void startDataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(2, numElems, 2); }
  public static int endShortVector(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

