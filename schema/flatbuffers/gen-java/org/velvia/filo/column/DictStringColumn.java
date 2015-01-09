// automatically generated, do not modify

package org.velvia.filo.column;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

public class DictStringColumn extends Table {
  public static DictStringColumn getRootAsDictStringColumn(ByteBuffer _bb) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (new DictStringColumn()).__init(_bb.getInt(_bb.position()) + _bb.position(), _bb); }
  public DictStringColumn __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public NaMask naMask() { return naMask(new NaMask()); }
  public NaMask naMask(NaMask obj) { int o = __offset(4); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public String dictionary(int j) { int o = __offset(6); return o != 0 ? __string(__vector(o) + j * 4) : null; }
  public int dictionaryLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer dictionaryAsByteBuffer() { return __vector_as_bytebuffer(6, 4); }
  public byte codesType() { int o = __offset(8); return o != 0 ? bb.get(o + bb_pos) : 0; }
  public Table codes(Table obj) { int o = __offset(10); return o != 0 ? __union(obj, o) : null; }

  public static int createDictStringColumn(FlatBufferBuilder builder,
      int naMask,
      int dictionary,
      byte codes_type,
      int codes) {
    builder.startObject(4);
    DictStringColumn.addCodes(builder, codes);
    DictStringColumn.addDictionary(builder, dictionary);
    DictStringColumn.addNaMask(builder, naMask);
    DictStringColumn.addCodesType(builder, codes_type);
    return DictStringColumn.endDictStringColumn(builder);
  }

  public static void startDictStringColumn(FlatBufferBuilder builder) { builder.startObject(4); }
  public static void addNaMask(FlatBufferBuilder builder, int naMaskOffset) { builder.addOffset(0, naMaskOffset, 0); }
  public static void addDictionary(FlatBufferBuilder builder, int dictionaryOffset) { builder.addOffset(1, dictionaryOffset, 0); }
  public static int createDictionaryVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startDictionaryVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addCodesType(FlatBufferBuilder builder, byte codesType) { builder.addByte(2, codesType, 0); }
  public static void addCodes(FlatBufferBuilder builder, int codesOffset) { builder.addOffset(3, codesOffset, 0); }
  public static int endDictStringColumn(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

