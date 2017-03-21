package org.velvia.filo.vectors

import java.nio.ByteBuffer
import scala.language.postfixOps
import scalaxy.loops._

import org.velvia.filo._

object DoubleVector {
  /**
   * Creates a new MaskedDoubleAppendingVector, allocating a byte array of the right size for the max #
   * of elements plus a bit mask.
   * @param maxElements initial maximum number of elements this vector will hold. Will automatically grow.
   */
  def appendingVector(maxElements: Int): BinaryAppendableVector[Double] = {
    val bytesRequired = 8 + BitmapMask.numBytesRequired(maxElements) + 8 * maxElements
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(bytesRequired)
    GrowableVector(new MaskedDoubleAppendingVector(base, off, nBytes, maxElements))
  }

  /**
   * Creates a DoubleAppendingVector - does not grow and does not have bit mask. All values are marked
   * as available.
   */
  def appendingVectorNoNA(maxElements: Int): BinaryAppendableVector[Double] = {
    val bytesRequired = 4 + 8 * maxElements
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(bytesRequired)
    new DoubleAppendingVector(base, off, nBytes)
  }

  def apply(buffer: ByteBuffer): BinaryVector[Double] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    DoubleBinaryVector(base, off, len)
  }

  def masked(buffer: ByteBuffer): MaskedDoubleBinaryVector = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new MaskedDoubleBinaryVector(base, off, len)
  }

  def const(buffer: ByteBuffer): BinaryVector[Double] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new DoubleConstVector(base, off, len)
  }

  def fromIntBuf(buf: ByteBuffer): BinaryVector[Double] =
    new DoubleIntWrapper(IntBinaryVector(buf))
  def fromMaskedIntBuf(buf: ByteBuffer): BinaryVector[Double] =
    new DoubleIntWrapper(IntBinaryVector.masked(buf))

  /**
   * Produces a smaller BinaryVector if possible given combination of minimal nbits as well as
   * if all values are not NA.
   * Here are the things tried:
   *  1. If min and max are the same, then a DoubleConstVector is produced.
   *  2. If all values are integral, then IntBinaryVector is produced (and integer optimization done)
   *  3. If all values are filled (no NAs) then the bitmask is dropped
   */
  def optimize(vect: BinaryAppendableVector[Double]): BinaryAppendableVector[Double] = {
    // TODO: optimization logic
    vect
  }
}

case class DoubleBinaryVector(base: Any, offset: Long, numBytes: Int) extends BinaryVector[Double] {
  override val length: Int = (numBytes - 4) / 8
  final def isAvailable(index: Int): Boolean = true
  final def apply(index: Int): Double = UnsafeUtils.getDouble(base, offset + 4 + index * 8)
}

class MaskedDoubleBinaryVector(val base: Any, val offset: Long, val numBytes: Int) extends
BitmapMaskVector[Double] {
  val bitmapOffset = offset + 4L
  val subVectOffset = UnsafeUtils.getInt(base, offset)
  private val dblVect = DoubleBinaryVector(base, offset + subVectOffset, numBytes - subVectOffset)

  override final def length: Int = dblVect.length
  final def apply(index: Int): Double = dblVect.apply(index)
}

class DoubleAppendingVector(base: Any, offset: Long, maxBytes: Int)
extends PrimitiveAppendableVector[Double](base, offset, maxBytes, 64, true) {
  final def addNA(): Unit = addData(0.0)
  final def addData(data: Double): Unit = {
    checkOffset()
    UnsafeUtils.setDouble(base, writeOffset, data)
    writeOffset += 8
  }

  private final val readVect = new DoubleBinaryVector(base, offset, maxBytes)
  final def apply(index: Int): Double = readVect.apply(index)
  final def isAvailable(index: Int): Boolean = true

  override final def addVector(other: BinaryVector[Double]): Unit = other match {
    case v: MaskedDoubleAppendingVector =>
      addVector(v.subVect)
    case v: BinaryVector[Double] =>
      // Optimization: this vector does not support NAs so just add the data
      require(v.numBytes <= maxBytes,
             s"Not enough space to add ${v.length} elems; need ${maxBytes-v.numBytes} bytes")
      for { i <- 0 until v.length optimized } { addData(v(i)) }
  }

  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Double] =
    new DoubleBinaryVector(newBase, newOff, numBytes)
}

class MaskedDoubleAppendingVector(base: Any,
                                  val offset: Long,
                                  val maxBytes: Int,
                                  maxElements: Int) extends
// First four bytes: offset to DoubleBinaryVector
BitmapMaskAppendableVector[Double](base, offset + 4L, maxElements) {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE

  val subVect = new DoubleAppendingVector(base, offset + subVectOffset, maxBytes - subVectOffset)

  final def minMax: (Double, Double) = {
    var min = Double.MaxValue
    var max = Double.MinValue
    for { index <- 0 until length optimized } {
      if (isAvailable(index)) {
        val data = subVect.apply(index)
        if (data < min) min = data
        if (data > max) max = data
      }
    }
    (min, max)
  }

  override def newInstance(growFactor: Int = 2): BinaryAppendableVector[Double] = {
    val (newbase, newoff, nBytes) = BinaryVector.allocWithMagicHeader(maxBytes * growFactor)
    new MaskedDoubleAppendingVector(newbase, newoff, maxBytes * growFactor, maxElements * growFactor)
  }

  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Double] = {
    // Don't forget to write the new subVectOffset
    UnsafeUtils.setInt(newBase, newOff, (bitmapOffset + bitmapBytes - offset).toInt)
    new MaskedDoubleBinaryVector(newBase, newOff, 4 + bitmapBytes + subVect.numBytes)
  }
}

class DoubleConstVector(base: Any, offset: Long, numBytes: Int) extends
ConstVector[Double](base, offset, numBytes) {
  def apply(i: Int): Double = UnsafeUtils.getDouble(base, dataOffset)
}

/**
 * A wrapper to return Doubles from an Int vector... for when one can compress Double vectors as IntVectors
 */
class DoubleIntWrapper(inner: BinaryVector[Int]) extends BinaryVector[Double] {
  val base = inner.base
  val offset = inner.offset
  val numBytes = inner.numBytes

  final def apply(i: Int): Double = inner(i).toDouble
  final def isAvailable(i: Int): Boolean = inner.isAvailable(i)
  override final def length: Int = inner.length
}
