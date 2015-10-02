// automatically generated, do not modify

package org.velvia.filo.column;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class ByteVector extends Table {
  public static ByteVector getRootAsByteVector(ByteBuffer _bb) { return getRootAsByteVector(_bb, new ByteVector()); }
  public static ByteVector getRootAsByteVector(ByteBuffer _bb, ByteVector obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public ByteVector __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public byte dataType() { int o = __offset(4); return o != 0 ? bb.get(o + bb_pos) : 0; }
  public byte data(int j) { int o = __offset(6); return o != 0 ? bb.get(__vector(o) + j * 1) : 0; }
  public int dataLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer dataAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }

  public static int createByteVector(FlatBufferBuilder builder,
      byte dataType,
      int data) {
    builder.startObject(2);
    ByteVector.addData(builder, data);
    ByteVector.addDataType(builder, dataType);
    return ByteVector.endByteVector(builder);
  }

  public static void startByteVector(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addDataType(FlatBufferBuilder builder, byte dataType) { builder.addByte(0, dataType, 0); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addOffset(1, dataOffset, 0); }
  public static int createDataVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startDataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static int endByteVector(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

