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

  // Note: This is a way to avoid storing null and dealing with NPEs for NA values
  val NaString = ""

  // Uses the smallest vector possible to fit in integer values
  def smallIntVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Int], maxNum: Int): (Int, Byte) = {
    // Add support for stuff below byte level
    if (maxNum < 256)        { byteVectorBuilder(fbb, data.map(_.toByte)) }
    else if (maxNum < 65536) { shortVectorBuilder(fbb, data.map(_.toShort)) }
    else                     { intVectorBuilder(fbb, data) }
  }

  def toDictStringColumn(data: Seq[String], naMask: Mask, stringSet: collection.Set[String]): ByteBuffer = {
    count += 1

    // Convert the set of strings to an encoding
    val uniques = stringSet.toSeq
    val strToCode = uniques.zipWithIndex.toMap

    // Encode each string to the code per the map above
    val codes = data.zipWithIndex.map { case (s, i) => if (naMask(i)) 0 else strToCode(s) + 1 }

    val fbb = new FlatBufferBuilder(BufferSize)
    val empty = isDataEmpty(naMask, data.length)
    val (dataOffset, dataType) =
      if (empty) (0, 0.toByte) else smallIntVectorBuilder(fbb, codes, stringSet.size + 1)
    val dictVect = makeStringVect(fbb, Seq(NaString) ++ uniques)
    DictStringColumn.startDictStringColumn(fbb)
    DictStringColumn.addDictionary(fbb, dictVect)
    if (!empty) {
      DictStringColumn.addCodes(fbb, dataOffset)
      DictStringColumn.addCodesType(fbb, dataType)
    }
    finishColumn(fbb, AnyColumn.DictStringColumn, data.length)
  }
}