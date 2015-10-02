package org.velvia.filo

import com.google.flatbuffers.FlatBufferBuilder
import framian.column.Mask
import java.nio.ByteBuffer
import org.velvia.filo.column._


/**
 * A whole bunch of encoders for simple (no compression) binary representation of sequences,
 * using Google FlatBuffers
 */
object SimpleEncoders {
  import Utils._

  var count = 0

  def toSimpleColumn[A](data: Seq[A], naMask: Mask, vectBuilder: DataVectorBuilder[A]): ByteBuffer = {
    count += 1
    val fbb = new FlatBufferBuilder(BufferSize)
    val (naOffset, empty) = populateNaMask(fbb, naMask, data.length)
    val (dataOffset, dataType) = if (empty) (0, 0.toByte) else vectBuilder(fbb, data)
    SimpleColumn.startSimpleColumn(fbb)
    SimpleColumn.addNaMask(fbb, naOffset)
    if (!empty) {
      SimpleColumn.addVector(fbb, dataOffset)
      SimpleColumn.addVectorType(fbb, dataType)
    }
    finishColumn(fbb, AnyColumn.SimpleColumn, data.length)
  }
}
