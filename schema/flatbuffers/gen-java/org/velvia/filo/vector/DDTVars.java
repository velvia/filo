// automatically generated, do not modify

package org.velvia.filo.vector;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class DDTVars extends Struct {
  public DDTVars __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public int len() { return bb.getInt(bb_pos + 0); }
  public byte baseTz() { return bb.get(bb_pos + 4); }
  public long baseMillis() { return bb.getLong(bb_pos + 8); }

  public static int createDDTVars(FlatBufferBuilder builder, int len, byte baseTz, long baseMillis) {
    builder.prep(8, 16);
    builder.putLong(baseMillis);
    builder.pad(3);
    builder.putByte(baseTz);
    builder.putInt(len);
    return builder.offset();
  }
};

