// automatically generated, do not modify

package org.velvia.filo.vector;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class DictStringVector extends Table {
  public static DictStringVector getRootAsDictStringVector(ByteBuffer _bb) { return getRootAsDictStringVector(_bb, new DictStringVector()); }
  public static DictStringVector getRootAsDictStringVector(ByteBuffer _bb, DictStringVector obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public DictStringVector __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public int len() { int o = __offset(4); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String dictionary(int j) { int o = __offset(6); return o != 0 ? __string(__vector(o) + j * 4) : null; }
  public int dictionaryLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public int nbits() { int o = __offset(8); return o != 0 ? bb.get(o + bb_pos) & 0xFF : 0; }
  public int codes(int j) { int o = __offset(10); return o != 0 ? bb.get(__vector(o) + j * 1) & 0xFF : 0; }
  public int codesLength() { int o = __offset(10); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer codesAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }

  public static int createDictStringVector(FlatBufferBuilder builder,
      int len,
      int dictionary,
      int nbits,
      int codes) {
    builder.startObject(4);
    DictStringVector.addCodes(builder, codes);
    DictStringVector.addDictionary(builder, dictionary);
    DictStringVector.addLen(builder, len);
    DictStringVector.addNbits(builder, nbits);
    return DictStringVector.endDictStringVector(builder);
  }

  public static void startDictStringVector(FlatBufferBuilder builder) { builder.startObject(4); }
  public static void addLen(FlatBufferBuilder builder, int len) { builder.addInt(0, len, 0); }
  public static void addDictionary(FlatBufferBuilder builder, int dictionaryOffset) { builder.addOffset(1, dictionaryOffset, 0); }
  public static int createDictionaryVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startDictionaryVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addNbits(FlatBufferBuilder builder, int nbits) { builder.addByte(2, (byte)(nbits & 0xFF), 0); }
  public static void addCodes(FlatBufferBuilder builder, int codesOffset) { builder.addOffset(3, codesOffset, 0); }
  public static int createCodesVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startCodesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static int endDictStringVector(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishDictStringVectorBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

