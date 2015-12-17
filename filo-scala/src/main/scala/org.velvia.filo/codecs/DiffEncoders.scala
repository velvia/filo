package org.velvia.filo.codecs

import com.google.flatbuffers.FlatBufferBuilder
import java.nio.{ByteBuffer, ByteOrder}
import org.joda.time.DateTime
import scala.collection.mutable.BitSet

import org.velvia.filo._
import org.velvia.filo.vector._

/**
 * Encoders that store deltas from a base value to reduce the FiloVector size.
 */
object DiffEncoders extends ThreadLocalBuffers {
  import Utils._

  var count = 0

  /**
   * Creates a DiffPrimitiveVector-based Filo vector.
   * @param min the minimum value from the data points that are available.
   *            Be careful not to include points from NA parts of the data sequence.
   */
  def toPrimitiveVector[A: PrimitiveDataVectBuilder](data: Seq[A],
                                                     naMask: BitSet,
                                                     min: A,
                                                     max: A): ByteBuffer = {
    import DiffPrimitiveVector._

    val vectBuilder = implicitly[PrimitiveDataVectBuilder[A]]
    count += 1
    val fbb = new FlatBufferBuilder(getBuffer)
    val naOffset = populateNaMask(fbb, naMask, data.length)

    val ((dataOffset, dnbits), dsigned) = vectBuilder.buildDeltas(fbb, data, min, max)
    startDiffPrimitiveVector(fbb)
    addNaMask(fbb, naOffset)
    addLen(fbb, data.length)
    addData(fbb, dataOffset)
    addInfo(fbb, DataInfo.createDataInfo(fbb, dnbits, dsigned))
    addBase(fbb, vectBuilder.toLong(min))
    finishDiffPrimitiveVectorBuffer(fbb, endDiffPrimitiveVector(fbb))
    putHeaderAndGet(fbb, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_PRIMITIVE)
  }

  def toDateTimeVector(millis: LongVectorBuilder,
                       tz: IntVectorBuilder,
                       naMask: BitSet): ByteBuffer = {
    import DiffDateTimeVector._

    val intVectBuilder = AutoIntegralDVBuilders.IntDataVectBuilder
    val longVectBuilder = AutoIntegralDVBuilders.LongDataVectBuilder
    count += 1
    val fbb = new FlatBufferBuilder(getBuffer)
    val naOffset = populateNaMask(fbb, naMask, millis.length)

    val ((mOffset, mnbits), msigned) = longVectBuilder.buildDeltas(fbb, millis.data,
                                                               millis.min, millis.max)
    // Only build timezone vector if they are different.  Most DateTime's have same TZ
    val ((tOffset, tnbits), tsigned) = if (tz.min != tz.max) {
      intVectBuilder.buildDeltas(fbb, tz.data, tz.min, tz.max)
    } else {
      ((-1, -1), false)
    }

    startDiffDateTimeVector(fbb)
    addNaMask(fbb, naOffset)
    addVars(fbb, DDTVars.createDDTVars(fbb, millis.length, tz.min.toByte, millis.min))
    addMillisInfo(fbb, DataInfo.createDataInfo(fbb, mnbits, msigned))
    addMillis(fbb, mOffset)
    if (tOffset >= 0) {
      addTzInfo(fbb, DataInfo.createDataInfo(fbb, tnbits, tsigned))
      addTz(fbb, tOffset)
    }
    finishDiffDateTimeVectorBuffer(fbb, endDiffDateTimeVector(fbb))
    putHeaderAndGet(fbb, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_DATETIME)
  }
}