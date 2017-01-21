package org.velvia.filo

import scala.language.postfixOps
import scalaxy.loops._

/**
 * This is really the same as FiloVector, but supports off-heap easier.
 * An immutable, zero deserialization, minimal/zero allocation, insanely fast binary sequence.
 * TODO: maybe merge this and FiloVector, or just make FiloVector support ZeroCopyBinary.
 */
trait BinaryVector[@specialized A] extends FiloVectorCore[A] with ZeroCopyBinary

object BinaryVector {
  // Adds Traversable ops to BinaryVector without polluting BV classes themselves.
  // Also lets us get out of using Traversable easily in the future.
  implicit class RichBinaryVector[A](bv: BinaryVector[A]) extends Traversable[A] {
    def foreach[B](fn: A => B): Unit = {
      for { i <- 0 until bv.length optimized } {
        if (bv.isAvailable(i)) fn(bv.apply(i))
      }
    }

  def optionIterator(): Iterator[Option[A]] =
    for { index <- (0 until bv.length).toIterator } yield { bv.get(index) }
  }
}

/**
 * A BinaryVector with an NaMask bitmap for NA values (1/on for missing NA values), 64-bit chunks
 */
trait BitmapMaskVector[A] extends BinaryVector[A] {
  def base: Any
  def bitmapOffset: Long   // NOTE: should be offset + n

  final def isAvailable(index: Int): Boolean = {
    // NOTE: length of bitMask may be less than (length / 64) longwords.
    val maskIndex = index >> 6
    val maskVal = UnsafeUtils.getLong(base, bitmapOffset + maskIndex * 8)
    (maskVal & (1L << (index & 63))) == 0
  }
}

object BitmapMask {
  def numBytesRequired(elements: Int): Int = ((elements + 63) / 64) * 8
}

/**
 * A BinaryVector that you can append to.  Has some notion of a maximum size (max # of items or bytes)
 * and the user is responsible for resizing if necessary.
 *
 * Replaces the current VectorBuilder API, and greatly simplifies overall APIs in Filo.  AppendableVectors
 * are still FiloVectors so they could be used as they are being built.
 */
trait BinaryAppendableVector[@specialized A] extends BinaryVector[A] {
  import RowReader._

  /** Max size that current buffer can grow to */
  def maxBytes: Int

  /** Add a Not Available (null) element to the builder. */
  def addNA(): Unit

  /** Add a value of type T to the builder.  It will be marked as available. */
  def addData(value: A): Unit

  /** Adds an element from a RowReader */
  final def add(row: RowReader, colNo: Int)(implicit extractor: TypedFieldExtractor[A]): Unit = {
    if (row.notNull(colNo)) { addData(extractor.getField(row, colNo)) }
    else                    { addNA() }
  }

  /** Returns true if every element added is NA, or no elements have been added */
  def isAllNA: Boolean

  /**
   * Freeze or make immutable this current BinaryVector.
   */
  def immute(): BinaryVector[A] = this.asInstanceOf[BinaryVector[A]]
}

/**
 * A BinaryAppendableVector for simple primitive types, ie where each element has a fixed length
 */
abstract class PrimitiveAppendableVector[@specialized A](val base: Any,
                                                         val offset: Long,
                                                         val maxBytes: Int,
                                                         nbits: Short,
                                                         signed: Boolean)
extends BinaryAppendableVector[A] {
  var numBytes: Int = 4
  var _len = 0
  override final def length: Int = _len

  UnsafeUtils.setShort(base, offset, nbits.toShort)
  UnsafeUtils.setByte(base, offset + 2, if (signed) 1 else 0)

  // Responsible for adding a single value and updating numBytes
  def addValue(v: A): Unit

  final def isAllNA: Boolean = (numBytes <= 4)
  final def addData(data: A): Unit = {
    require(numBytes < maxBytes)
    addValue(data)
    _len += 1
  }
}

/**
 * Maintains a fast NA bitmap mask as we append elements
 */
abstract class BitmapMaskAppendableVector[@specialized A](val base: Any,
                                                          val bitmapOffset: Long,
                                                          maxElements: Int)
extends BitmapMaskVector[A] with BinaryAppendableVector[A] {
  def addEmptyValue(): Unit
  def addDataValue(data: A): Unit

  val bitmapMaskBufferSize = BitmapMask.numBytesRequired(maxElements)
  var curBitmapOffset = 0
  var curMask: Long = 1L

  UnsafeUtils.unsafe.setMemory(base, bitmapOffset, bitmapMaskBufferSize, 0)

  // The number of bytes taken up by the bitmap mask right now
  final def bitmapBytes: Int = curBitmapOffset + 8

  final def nextMaskIndex(): Unit = {
    curMask = curMask << 1
    if (curMask == 0) {
      curMask = 1L
      curBitmapOffset += 8
    }
  }

  final def addNA(): Unit = {
    require(curBitmapOffset < bitmapMaskBufferSize)
    val maskVal = UnsafeUtils.getLong(base, bitmapOffset + curBitmapOffset)
    UnsafeUtils.setLong(base, bitmapOffset + curBitmapOffset, maskVal | curMask)
    addEmptyValue()
    nextMaskIndex()
  }

  final def addData(value: A): Unit = {
    addDataValue(value)
    nextMaskIndex()
  }

  final def isAllNA: Boolean = {
    for { word <- 0 until curBitmapOffset/8 optimized } {
      if (UnsafeUtils.getLong(base, bitmapOffset + word * 8) != -1L) return false
    }
    val naMask = curMask - 1
    (UnsafeUtils.getLong(base, bitmapOffset + curBitmapOffset) & naMask) == naMask
  }
}
