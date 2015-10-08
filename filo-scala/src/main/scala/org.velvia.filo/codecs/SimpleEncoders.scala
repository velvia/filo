package org.velvia.filo.codecs

import com.google.flatbuffers.FlatBufferBuilder
import java.nio.ByteBuffer
import scala.collection.mutable.BitSet

import org.velvia.filo._
import org.velvia.filo.vector._

/**
 * A whole bunch of encoders for simple (no compression) binary representation of sequences,
 * using Google FlatBuffers
 */
object SimpleEncoders extends ThreadLocalBuffers {
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
                                                     max: A,
                                                     signed: Boolean): ByteBuffer = {
    import SimplePrimitiveVector._

    val vectBuilder = implicitly[PrimitiveDataVectBuilder[A]]
    count += 1
    val fbb = new FlatBufferBuilder(getBuffer)
    val naOffset = populateNaMask(fbb, naMask, data.length)
    val (dataOffset, nbits) = vectBuilder.build(fbb, data, min, max)
    startSimplePrimitiveVector(fbb)
    addNaMask(fbb, naOffset)
    addLen(fbb, data.length)
    addData(fbb, dataOffset)
    addInfo(fbb, DataInfo.createDataInfo(fbb, nbits, signed))
    finishSimplePrimitiveVectorBuffer(fbb, endSimplePrimitiveVector(fbb))
    putHeaderAndGet(fbb, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_PRIMITIVE)
  }

  def toEmptyVector(len: Int): ByteBuffer = {
    val bb = ByteBuffer.allocate(4)
    bb.putInt(WireFormat.emptyVector(len))
    bb.position(0)
    bb
  }

  def toStringVector(data: Seq[String], naMask: BitSet): ByteBuffer = {
    val fbb = new FlatBufferBuilder(getBuffer)
    val naOffset = populateNaMask(fbb, naMask, data.length)
    val dataOffset = stringVect(fbb, data)
    val ssvOffset = SimpleStringVector.createSimpleStringVector(fbb, naOffset, dataOffset)
    SimpleStringVector.finishSimpleStringVectorBuffer(fbb, ssvOffset)
    putHeaderAndGet(fbb, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_STRING)
  }
}
