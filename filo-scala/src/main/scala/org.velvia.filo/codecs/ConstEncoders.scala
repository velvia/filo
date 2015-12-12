package org.velvia.filo.codecs

import com.google.flatbuffers.FlatBufferBuilder
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.BitSet

import org.velvia.filo._
import org.velvia.filo.vector._

/**
 * Encoders for sequences where the non-NA values are all the same
 */
object ConstEncoders extends ThreadLocalBuffers {
  import Utils._

  var count = 0

  /**
   * Creates a SimplePrimitiveVector-based Filo vector.
   * @param min the minimum value from the data points that are available.
   *            Be careful not to include points from NA parts of the data sequence.
   */
  def toPrimitiveVector[A: PrimitiveDataVectBuilder](data: Seq[A],
                                                     naMask: BitSet,
                                                     min: A,
                                                     max: A): ByteBuffer = {
    import SimplePrimitiveVector._
    require(min == max)

    val vectBuilder = implicitly[PrimitiveDataVectBuilder[A]]
    count += 1
    val fbb = new FlatBufferBuilder(getBuffer)
    val naOffset = populateNaMask(fbb, naMask, data.length)
    val ((dataOffset, nbits), signed) = vectBuilder.build(fbb, Seq(min), min, max)
    startSimplePrimitiveVector(fbb)
    addNaMask(fbb, naOffset)
    addLen(fbb, data.length)
    addData(fbb, dataOffset)
    addInfo(fbb, DataInfo.createDataInfo(fbb, nbits, signed))
    finishSimplePrimitiveVectorBuffer(fbb, endSimplePrimitiveVector(fbb))
    putHeaderAndGet(fbb, WireFormat.VECTORTYPE_CONST, WireFormat.SUBTYPE_PRIMITIVE)
  }

  def toStringVector(str: String, len: Int, naMask: BitSet): ByteBuffer = {
    import ConstStringVector._

    count += 1
    val fbb = new FlatBufferBuilder(getBuffer)
    val naOffset = populateNaMask(fbb, naMask, len)
    val strOffset = fbb.createString(str)
    val offset = createConstStringVector(fbb, len, naOffset, strOffset)
    finishConstStringVectorBuffer(fbb, offset)
    putHeaderAndGet(fbb, WireFormat.VECTORTYPE_CONST, WireFormat.SUBTYPE_STRING)
  }
}