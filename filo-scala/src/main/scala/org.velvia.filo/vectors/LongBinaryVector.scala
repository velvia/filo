package org.velvia.filo.vectors

import java.nio.ByteBuffer
import scala.language.postfixOps
import scalaxy.loops._

import org.velvia.filo._

object LongBinaryVector {
  /**
   * Creates a new MaskedLongAppendingVector, allocating a byte array of the right size for the max #
   * of elements plus a bit mask.
   * @param maxElements initial maximum number of elements this vector will hold. Will automatically grow.
   */
  def appendingVector(maxElements: Int): BinaryAppendableVector[Long] = {
    val bytesRequired = 8 + BitmapMask.numBytesRequired(maxElements) + 8 * maxElements
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(bytesRequired)
    GrowableVector(new MaskedLongAppendingVector(base, off, nBytes, maxElements))
  }

  /**
   * Creates a LongAppendingVector - does not grow and does not have bit mask. All values are marked
   * as available.
   */
  def appendingVectorNoNA(maxElements: Int): BinaryAppendableVector[Long] = {
    val bytesRequired = 4 + 8 * maxElements
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(bytesRequired)
    new LongAppendingVector(base, off, nBytes)
  }

  def apply(buffer: ByteBuffer): BinaryVector[Long] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    LongBinaryVector(base, off, len)
  }

  def masked(buffer: ByteBuffer): MaskedLongBinaryVector = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new MaskedLongBinaryVector(base, off, len)
  }

  def const(buffer: ByteBuffer): BinaryVector[Long] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new LongConstVector(base, off, len)
  }

  def fromIntBuf(buf: ByteBuffer): BinaryVector[Long] =
    new LongIntWrapper(IntBinaryVector(buf))
  def fromMaskedIntBuf(buf: ByteBuffer): BinaryVector[Long] =
    new LongIntWrapper(IntBinaryVector.masked(buf))

  /**
   * Produces a smaller BinaryVector if possible given combination of minimal nbits as well as
   * if all values are not NA.
   * Here are the things tried:
   *  1. If min and max are the same, then a LongConstVector is produced.
   *  2. Check the nbits, if it can fit in smaller # of bits do that, possibly creating int vector
   *  3. If all values are filled (no NAs) then the bitmask is dropped
   */
  def optimize(vector: MaskedLongAppendingVector): BinaryAppendableVector[Long] = {
    val intWrapper = new IntLongWrapper(vector)

    if (intWrapper.binConstVector) {
      val (b, o, n) = ConstVector.make(vector.length, 8) { case (base, off) =>
        UnsafeUtils.setLong(base, off, vector(0))
      }
      new LongConstVector(b, o, n)
    // Check if all integrals. use the wrapper to avoid an extra pass
    } else if (intWrapper.fitInInt) {
      // After optimize, you are supposed to just call toFiloBuffer(), so this is fine
      IntBinaryVector.optimize(intWrapper).asInstanceOf[BinaryAppendableVector[Long]]
    } else if (vector.noNAs) {
      vector.subVect
    } else {
      vector
    }
  }
}

case class LongBinaryVector(base: Any, offset: Long, numBytes: Int) extends BinaryVector[Long] {
  override val length: Int = (numBytes - 4) / 8
  final def isAvailable(index: Int): Boolean = true
  final def apply(index: Int): Long = UnsafeUtils.getLong(base, offset + 4 + index * 8)
}

class MaskedLongBinaryVector(val base: Any, val offset: Long, val numBytes: Int) extends
BitmapMaskVector[Long] {
  val bitmapOffset = offset + 4L
  val subVectOffset = UnsafeUtils.getInt(base, offset)
  private val longVect = LongBinaryVector(base, offset + subVectOffset, numBytes - subVectOffset)

  override final def length: Int = longVect.length
  final def apply(index: Int): Long = longVect.apply(index)
}

class LongAppendingVector(base: Any, offset: Long, maxBytes: Int)
extends PrimitiveAppendableVector[Long](base, offset, maxBytes, 64, true) {
  final def addNA(): Unit = addData(0L)
  final def addData(data: Long): Unit = {
    checkOffset()
    UnsafeUtils.setLong(base, writeOffset, data)
    writeOffset += 8
  }

  private final val readVect = new LongBinaryVector(base, offset, maxBytes)
  final def apply(index: Int): Long = readVect.apply(index)

  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Long] =
    new LongBinaryVector(newBase, newOff, numBytes)
}

class MaskedLongAppendingVector(base: Any,
                                val offset: Long,
                                val maxBytes: Int,
                                val maxElements: Int) extends
// First four bytes: offset to LongBinaryVector
BitmapMaskAppendableVector[Long](base, offset + 4L, maxElements) {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE

  val subVect = new LongAppendingVector(base, offset + subVectOffset, maxBytes - subVectOffset)

  final def minMax: (Long, Long) = {
    var min = Long.MaxValue
    var max = Long.MinValue
    for { index <- 0 until length optimized } {
      if (isAvailable(index)) {
        val data = subVect.apply(index)
        if (data < min) min = data
        if (data > max) max = data
      }
    }
    (min, max)
  }

  override def optimize(): BinaryAppendableVector[Long] = LongBinaryVector.optimize(this)

  override def newInstance(growFactor: Int = 2): BinaryAppendableVector[Long] = {
    val (newbase, newoff, nBytes) = BinaryVector.allocWithMagicHeader(maxBytes * growFactor)
    new MaskedLongAppendingVector(newbase, newoff, maxBytes * growFactor, maxElements * growFactor)
  }

  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Long] = {
    // Don't forget to write the new subVectOffset
    UnsafeUtils.setInt(newBase, newOff, (bitmapOffset + bitmapBytes - offset).toInt)
    new MaskedLongBinaryVector(newBase, newOff, 4 + bitmapBytes + subVect.numBytes)
  }
}

/**
 * A wrapper around MaskedLongAppendingVector that returns Ints.  Designed to feed into IntVector
 * optimizer so that an optimized int representation of Long vector can be produced in one pass without
 * appending to another Int based AppendingVector first.
 * If it turns out the optimizer needs the original 32-bit vector, then it calls dataVect / getVect.
 */
class IntLongWrapper(val inner: MaskedLongAppendingVector) extends MaskedIntAppending
with AppendableVectorWrapper[Int, Long] {
  val (min, max) = inner.minMax
  def minMax: (Int, Int) = (min.toInt, max.toInt)
  val nbits: Short = 64

  val fitInInt = min >= Int.MinValue.toLong && max <= Int.MaxValue.toLong

  val binConstVector = (min == max) && inner.noNAs

  final def addData(value: Int): Unit = inner.addData(value.toLong)
  final def apply(index: Int): Int = inner(index).toInt

  def dataVect: BinaryAppendableVector[Int] = {
    val vect = IntBinaryVector.appendingVectorNoNA(inner.length)
    for { index <- 0 until length optimized } {
      vect.addData(inner(index).toInt)
    }
    vect
  }

  override def getVect: BinaryAppendableVector[Int] = {
    val vect = IntBinaryVector.appendingVector(inner.length)
    for { index <- 0 until length optimized } {
      if (inner.isAvailable(index)) vect.addData(inner(index).toInt) else vect.addNA()
    }
    vect
  }
}

class LongConstVector(base: Any, offset: Long, numBytes: Int) extends
ConstVector[Long](base, offset, numBytes) {
  private final val const = UnsafeUtils.getLong(base, dataOffset)
  final def apply(i: Int): Long = const
}

/**
 * A wrapper to return Longs from an Int vector... for when one can compress Long vectors as IntVectors
 */
class LongIntWrapper(inner: BinaryVector[Int]) extends BinaryVector[Long] {
  val base = inner.base
  val offset = inner.offset
  val numBytes = inner.numBytes

  final def apply(i: Int): Long = inner(i).toLong
  final def isAvailable(i: Int): Boolean = inner.isAvailable(i)
  override final def length: Int = inner.length
}
