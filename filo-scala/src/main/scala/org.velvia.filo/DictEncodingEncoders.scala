package org.velvia.filo

import com.google.flatbuffers.FlatBufferBuilder
import java.nio.ByteBuffer
import org.velvia.filo.vector._
import scala.collection.mutable.BitSet

/**
 * Encoders for dictionary encoding/compression
 */
object DictEncodingEncoders extends ThreadLocalBuffers {
  import Utils._

  var count = 0

  // Note: This is a way to avoid storing null and dealing with NPEs for NA values
  val NaString = ""

  def toStringVector(data: Seq[String], naMask: BitSet, stringSet: collection.Set[String]): ByteBuffer = {
    import DictStringVector._

    count += 1
    val builder = PrimitiveUnsignedBuilders.IntDataVectBuilder

    // Convert the set of strings to an encoding
    val uniques = stringSet.toSeq
    val strToCode = uniques.zipWithIndex.toMap

    // Encode each string to the code per the map above
    val codes = data.zipWithIndex.map { case (s, i) => if (naMask(i)) 0 else strToCode(s) + 1 }

    val fbb = new FlatBufferBuilder(getBuffer)
    val (dataOffset, nbits) = builder.build(fbb, codes, 0, stringSet.size + 1)
    val dictVect = stringVect(fbb, Seq(NaString) ++ uniques)
    startDictStringVector(fbb)
    addDictionary(fbb, dictVect)
    addLen(fbb, data.length)
    addCodes(fbb, dataOffset)
    addInfo(fbb, DataInfo.createDataInfo(fbb, nbits, false))
    finishDictStringVectorBuffer(fbb, endDictStringVector(fbb))
    putHeaderAndGet(fbb, WireFormat.VECTORTYPE_DICT, WireFormat.SUBTYPE_STRING)
  }
}