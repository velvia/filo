package org.velvia.filo.codecs

import com.google.flatbuffers.FlatBufferBuilder
import java.nio.ByteBuffer
import java.util.HashMap
import scala.collection.mutable.{ArrayBuffer, BitSet}
import scala.language.postfixOps
import scalaxy.loops._

import org.velvia.filo._
import org.velvia.filo.vector._

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
    // NOTE: sorry but java's HashMap is just much faster (for the next step)
    // This used to be `uniques.zipWithIndex.toMap`
    val strToCode = new HashMap[String, Int]()
    for { i <- 0 until uniques.length optimized } {
      strToCode.put(uniques(i), i)
    }

    // Encode each string to the code per the map above
    // Again we could have used data.zipWithIndex.map(....) but this is much faster.
    val codes = ArrayBuffer.fill(data.length)(0)
    for { i <- 0 until data.length optimized } {
      if (!naMask(i)) codes(i) = strToCode.get(data(i)) + 1
    }

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