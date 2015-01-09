package org.velvia.filo

import com.google.flatbuffers.FlatBufferBuilder
import framian.column.Mask
import java.nio.ByteBuffer
import org.velvia.filo.column._

/**
 * Encoders for dictionary encoding/compression
 */
object DictEncodingEncoders {
  import Utils._

  var count = 0

  // Uses the smallest vector possible to fit in integer values
  def smallIntVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Int], maxNum: Int): (Int, Byte) = {
    // Add support for stuff below byte level
    if (maxNum < 256)        byteVectorBuilder(fbb, data.map(_.toByte))
    else if (maxNum < 65536) shortVectorBuilder(fbb, data.map(_.toShort))
    else                     intVectorBuilder(fbb, data)
  }

  def toDictStringColumn(data: Seq[String], naMask: Mask, stringSet: collection.Set[String]): ByteBuffer = {
    count += 1

    // Convert the set of strings to an encoding
    val uniques = stringSet.toSeq
    val strToCode = uniques.zipWithIndex.toMap

    // Encode each string to the code per the map above
    val codes = data.zipWithIndex.map { case (s, i) => if (naMask(i)) 0 else strToCode(s) }

    val fbb = new FlatBufferBuilder(BufferSize)
    val (naOffset, empty) = populateNaMask(fbb, naMask)
    val (dataOffset, dataType) = if (empty) (0, 0.toByte) else smallIntVectorBuilder(fbb, codes, stringSet.size)
    val dictVect = makeStringVect(fbb, uniques)
    DictStringColumn.startDictStringColumn(fbb)
    DictStringColumn.addNaMask(fbb, naOffset)
    DictStringColumn.addDictionary(fbb, dictVect)
    if (!empty) {
      DictStringColumn.addCodes(fbb, dataOffset)
      DictStringColumn.addCodesType(fbb, dataType)
    }
    finishColumn(fbb, AnyColumn.DictStringColumn)
  }
}