// automatically generated, do not modify

package org.velvia.filo.column;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

public class NaMask extends Table {
  public static NaMask getRootAsNaMask(ByteBuffer _bb) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (new NaMask()).__init(_bb.getInt(_bb.position()) + _bb.position(), _bb); }
  public NaMask __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public byte maskType() { int o = __offset(4); return o != 0 ? bb.get(o + bb_pos) : 2; }
  /// for type = SimpleBitMask
  public long bitMask(int j) { int o = __offset(6); return o != 0 ? bb.getLong(__vector(o) + j * 8) : 0; }
  public int bitMaskLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer bitMaskAsByteBuffer() { return __vector_as_bytebuffer(6, 8); }

  public static int createNaMask(FlatBufferBuilder builder,
      byte maskType,
      int bitMask) {
    builder.startObject(2);
    NaMask.addBitMask(builder, bitMask);
    NaMask.addMaskType(builder, maskType);
    return NaMask.endNaMask(builder);
  }

  public static void startNaMask(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addMaskType(FlatBufferBuilder builder, byte maskType) { builder.addByte(0, maskType, 2); }
  public static void addBitMask(FlatBufferBuilder builder, int bitMaskOffset) { builder.addOffset(1, bitMaskOffset, 0); }
  public static int createBitMaskVector(FlatBufferBuilder builder, long[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addLong(data[i]); return builder.endVector(); }
  public static void startBitMaskVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static int endNaMask(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

