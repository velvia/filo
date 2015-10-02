// automatically generated, do not modify

package org.velvia.filo.column;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Column extends Table {
  public static Column getRootAsColumn(ByteBuffer _bb) { return getRootAsColumn(_bb, new Column()); }
  public static Column getRootAsColumn(ByteBuffer _bb, Column obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public Column __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public byte colType() { int o = __offset(4); return o != 0 ? bb.get(o + bb_pos) : 0; }
  public Table col(Table obj) { int o = __offset(6); return o != 0 ? __union(obj, o) : null; }
  public int len() { int o = __offset(8); return o != 0 ? bb.getInt(o + bb_pos) : 0; }

  public static int createColumn(FlatBufferBuilder builder,
      byte col_type,
      int col,
      int len) {
    builder.startObject(3);
    Column.addLen(builder, len);
    Column.addCol(builder, col);
    Column.addColType(builder, col_type);
    return Column.endColumn(builder);
  }

  public static void startColumn(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addColType(FlatBufferBuilder builder, byte colType) { builder.addByte(0, colType, 0); }
  public static void addCol(FlatBufferBuilder builder, int colOffset) { builder.addOffset(1, colOffset, 0); }
  public static void addLen(FlatBufferBuilder builder, int len) { builder.addInt(2, len, 0); }
  public static int endColumn(FlatBufferBuilder builder) {
    int o = builder.endObject();
    builder.required(o, 6);  // col
    return o;
  }
  public static void finishColumnBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

