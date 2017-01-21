package org.velvia.filo.vectors

import org.velvia.filo._
import org.velvia.filo.TypedBufferReader._

object IntBinaryVector {
  /**
   * Creates a new MaskedIntAppendingVector, allocating a byte array of the right size for the max #
   * of elements.
   * @param maxElements maximum number of elements this vector will hold.  If more are appended then
   *                    an exception will be thrown.
   */
  def appendingVector(maxElements: Int,
                      nbits: Short = 32,
                      signed: Boolean = true): BinaryAppendableVector[Int] = {
    val bytesRequired = 4 + BitmapMask.numBytesRequired(maxElements) + 4 + maxElements * 4
    val bytes = new Array[Byte](bytesRequired)
    new MaskedIntAppendingVector(bytes, UnsafeUtils.arayOffset, bytesRequired, maxElements,
                                 nbits, signed)
  }

  /**
   * Same as appendingVector but uses a SimpleAppendingVector with no ability to hold NA mask
   */
  def appendingVectorNoNA(maxElements: Int,
                          nbits: Short = 32,
                          signed: Boolean = true): IntAppendingVector = {
    val bytesRequired = 4 + maxElements * 4
    val bytes = new Array[Byte](bytesRequired)
    appendingVectorNoNA(bytes, UnsafeUtils.arayOffset, bytesRequired, nbits, signed)
  }

  def appendingVectorNoNA(base: Any,
                          offset: Long,
                          maxBytes: Int,
                          nbits: Short,
                          signed: Boolean): IntAppendingVector = nbits match {
    case 32 => new IntAppendingVector(base, offset, maxBytes, nbits, signed) {
      final def addValue(v: Int): Unit = {
        UnsafeUtils.setInt(base, offset + numBytes, v)
        numBytes += 4
      }
    }
    case 16 => new IntAppendingVector(base, offset, maxBytes, nbits, signed) {
      final def addValue(v: Int): Unit = {
        UnsafeUtils.setShort(base, offset + numBytes, v.toShort)
        numBytes += 2
      }
    }
    case 8 => new IntAppendingVector(base, offset, maxBytes, nbits, signed) {
      final def addValue(v: Int): Unit = {
        UnsafeUtils.setByte(base, offset + numBytes, v.toByte)
        numBytes += 1
      }
    }
  }

  /**
   * Creates a BinaryVector[Int] with no NAMask
   */
  def apply(base: Any, offset: Long, numBytes: Int): BinaryVector[Int] = {
    val nbits = UnsafeUtils.getShort(base, offset)
    // offset+2: nonzero = signed integral vector
    if (UnsafeUtils.getByte(base, offset + 2) != 0) {
      nbits match {
        case 32 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = UnsafeUtils.getInt(base, bufOffset + index * 4)
        }
        case 16 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = UnsafeUtils.getShort(base, bufOffset + index * 2).toInt
        }
        case 8 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = UnsafeUtils.getByte(base, bufOffset + index).toInt
        }
      }
    } else {
      nbits match {
        case 32 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = UnsafeUtils.getInt(base, bufOffset + index * 4)
        }
        case 16 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = (UnsafeUtils.getShort(base, bufOffset + index * 2) & 0x0ffff).toInt
        }
        case 8 => new IntBinaryVector(base, offset, numBytes, nbits) {
          final def apply(index: Int): Int = (UnsafeUtils.getByte(base, bufOffset + index) & 0x00ff).toInt
        }
      }
    }
  }

  def masked(base: Any, offset: Long, numBytes: Int): MaskedIntBinaryVector =
    new MaskedIntBinaryVector(base, offset, numBytes)
}

abstract class IntBinaryVector(val base: Any,
                               val offset: Long,
                               val numBytes: Int,
                               nbits: Short) extends BinaryVector[Int] {
  final val bufOffset = offset + 4
  // This length method works assuming nbits is divisible into 32
  override def length: Int = (numBytes - 4) * 8 / nbits
  final def isAvailable(index: Int): Boolean = true
}

class MaskedIntBinaryVector(val base: Any, val offset: Long, val numBytes: Int) extends BitmapMaskVector[Int] {
  // First four bytes: offset to SimpleIntBinaryVector
  val bitmapOffset = offset + 4L
  val intVectOffset = UnsafeUtils.getInt(base, offset)
  private val intVect = IntBinaryVector(base, offset + intVectOffset, numBytes - intVectOffset)

  override final def length: Int = intVect.length
  final def apply(index: Int): Int = intVect.apply(index)
}

abstract class IntAppendingVector(base: Any,
                                  offset: Long,
                                  maxBytes: Int,
                                  nbits: Short,
                                  signed: Boolean)
extends PrimitiveAppendableVector[Int](base, offset, maxBytes, nbits, signed) {
  final def addNA(): Unit = addData(0)

  private final val readVect = IntBinaryVector(base, offset, maxBytes)
  final def apply(index: Int): Int = readVect.apply(index)
  final def isAvailable(index: Int): Boolean = true
}

class MaskedIntAppendingVector(base: Any,
                               val offset: Long,
                               val maxBytes: Int,
                               maxElements: Int,
                               nbits: Short,
                               signed: Boolean) extends
// First four bytes: offset to SimpleIntBinaryVector
BitmapMaskAppendableVector[Int](base, offset + 4L, maxElements) {
  val intVectOffset = 4 + bitmapMaskBufferSize
  UnsafeUtils.setInt(base, offset, intVectOffset)
  private val intVect = IntBinaryVector.appendingVectorNoNA(base, offset + intVectOffset,
                                                            maxBytes - intVectOffset,
                                                            nbits, signed)

  override final def length: Int = intVect.length
  final def numBytes: Int = 4 + bitmapBytes + intVect.numBytes
  final def apply(index: Int): Int = intVect.apply(index)
  final def addEmptyValue(): Unit = intVect.addNA()
  final def addDataValue(data: Int): Unit = intVect.addData(data)
}