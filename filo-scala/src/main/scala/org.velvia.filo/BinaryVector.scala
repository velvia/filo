package org.velvia.filo

import java.nio.ByteBuffer
import scala.language.postfixOps
import scalaxy.loops._

import RowReader._

/**
 * This is really the same as FiloVector, but supports off-heap easier.
 * An immutable, zero deserialization, minimal/zero allocation, insanely fast binary sequence.
 * TODO: maybe merge this and FiloVector, or just make FiloVector support ZeroCopyBinary.
 */
trait BinaryVector[@specialized A] extends FiloVector[A] with ZeroCopyBinary

object BinaryVector {
  /** Used to reserve memory location "4-byte header will go here" */
  val HeaderMagic = 0x87654300  // The lower 8 bits should not be 00

  /**
   * Allocate a byte array with nBytes usable bytes, prepended with a 4-byte HeaderMagic
   * to reserve space for writing the FiloVector header.
   * @return (base, offset, numBytes) tuple for the appendingvector
   */
  def allocWithMagicHeader(nBytes: Int): (Any, Long, Int) = {
    val newBytes = new Array[Byte](nBytes + 4)
    UnsafeUtils.setInt(newBytes, UnsafeUtils.arayOffset, HeaderMagic)
    (newBytes, UnsafeUtils.arayOffset + 4, nBytes)
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

  final def add(data: Option[A]): Unit = if (data.nonEmpty) addData(data.get) else addNA()

  /** Returns true if every element added is NA, or no elements have been added */
  def isAllNA: Boolean

  /** Returns true if no elements are NA at all, ie every element is available. */
  def noNAs: Boolean

  /**
   * A method to add in bulk all the items from another BinaryVector of the same type.
   * This is a separate method to allow for more efficient implementations such as the ability to
   * copy all the bytes if the data layout are exactly the same.
   *
   * The default implementation is generic but inefficient: does not take advantage of data layout
   */
  def addVector(other: BinaryVector[A]): Unit = {
    for { i <- 0 until other.length optimized } {
      if (other.isAvailable(i)) { addData(other(i))}
      else                      { addNA() }
    }
  }

  /**
   * Returns the number of bytes required for a compacted AppendableVector
   * The default implementation must be overridden if freeze() is also overridden.
   */
  def frozenSize: Int =
    if (numBytes >= primaryMaxBytes) numBytes - (primaryMaxBytes - primaryBytes) else numBytes

  /**
   * Compact the bytes of this BinaryVector into smallest space possible, and return an immutable
   * version of this FiloVector that cannot be appended to.
   * The default implementation assumes the following common case:
   *  - AppendableBinaryVectors usually are divided into a fixed primary area and a secondary variable
   *    area that can extend up to maxBytes.  Ex.: the area for bitmap masks or UTF8 offets/lengths.
   *  - primary area can extend up to primaryMaxBytes but is currently at primaryBytes
   *  - compaction works by moving secondary area up (primaryMaxBytes - primaryBytes) bytes, either
   *    in place or to a new location (and the primary area would be copied too).
   *  - finishCompaction is called to instantiate the new BinaryVector and do any other cleanup.
   *
   * @param newBaseOffset optionally, compact not in place but to a new location.  If left as None,
   *                      any compaction will be done "in-place" in the same buffer.
   */
  def freeze(newBaseOffset: Option[(Any, Long)] = None): BinaryVector[A] =
    if (newBaseOffset.isEmpty && numBytes == frozenSize) { this.asInstanceOf[BinaryVector[A]] }
    else {
      val (newBase, newOffset) = newBaseOffset.getOrElse((base, offset))
      if (newBaseOffset.nonEmpty) copyTo(newBase, newOffset, n = primaryBytes)
      if (numBytes > primaryMaxBytes) {
        copyTo(newBase, newOffset + primaryBytes, primaryMaxBytes, numBytes - primaryMaxBytes)
      }
      finishCompaction(newBase, newOffset)
    }

  // The defaults below only work for vectors with no variable/secondary area.  Override as needed.
  def primaryBytes: Int = numBytes
  def primaryMaxBytes: Int = maxBytes

  // Does any necessary metadata adjustments and instantiates immutable BinaryVector
  def finishCompaction(newBase: Any, newOff: Long): BinaryVector[A] = ???

  /** The major and subtype bytes as defined in WireFormat that will go into the FiloVector header */
  def vectMajorType: Int
  def vectSubType: Int

  /**
   * Produce a FiloVector with the four-byte header.   Includes "freeze" action.
   */
  def toFiloBuffer(): ByteBuffer = {
    val frozenVect = freeze()
    // Check if magic word written to header location.  Then write header, and wrap all the bytes
    // in a ByteBuffer and return that.  Assumes byte array properly allocated beforehand.
    // If that doesn't work, copy bytes to new array first then write header.
    val byteArray = if (base.isInstanceOf[Array[Byte]] && offset == (UnsafeUtils.arayOffset + 4) &&
                        UnsafeUtils.getInt(base, offset - 4) == BinaryVector.HeaderMagic) {
      UnsafeUtils.setInt(base, offset - 4, WireFormat(vectMajorType, vectSubType))
      base.asInstanceOf[Array[Byte]]
    } else {
      val bytes = new Array[Byte](frozenVect.numBytes + 4)
      frozenVect.copyTo(bytes, UnsafeUtils.arayOffset + 4)
      UnsafeUtils.setInt(bytes, UnsafeUtils.arayOffset, WireFormat(vectMajorType, vectSubType))
      bytes
    }
    val bb = ByteBuffer.wrap(byteArray)
    bb.limit(frozenVect.numBytes + 4)
    bb
  }
}

/**
 * Wrapper around a BinaryAppendableVector that fits the VectorBuilder APIs.
 * toFiloBuffer needs to be implemented for each specific type.
 */
abstract class BinaryVectorBuilder[@specialized A: TypedFieldExtractor](inner: BinaryAppendableVector[A])
extends VectorBuilderBase {
  type T = A

  final def addNA(): Unit = inner.addNA()
  final def addData(value: T): Unit = inner.addData(value)
  final def isAllNA: Boolean = inner.isAllNA
  final def length: Int = inner.length
  final def reset(): Unit = {}

  val extractor: TypedFieldExtractor[A] = implicitly[TypedFieldExtractor[A]]
}

/**
 * A BinaryAppendableVector for simple primitive types, ie where each element has a fixed length
 * and every element is available (there is no bitmap NA mask).
 */
abstract class PrimitiveAppendableVector[@specialized A](val base: Any,
                                                         val offset: Long,
                                                         val maxBytes: Int,
                                                         nbits: Short,
                                                         signed: Boolean)
extends BinaryAppendableVector[A] {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE_NOMASK
  var numBytes: Int = 4
  var _len = 0
  override final def length: Int = _len

  UnsafeUtils.setShort(base, offset, nbits.toShort)
  UnsafeUtils.setByte(base, offset + 2, if (signed) 1 else 0)

  // Responsible for adding a single value and updating numBytes
  def addValue(v: A): Unit

  final def isAllNA: Boolean = (_len == 0)
  final def noNAs: Boolean = (_len > 0)
  final def addData(data: A): Unit = {
    require(numBytes < maxBytes, s"Not enough space: $numBytes < $maxBytes")
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
  final def bitmapBytes: Int = curBitmapOffset + (if (curMask == 1L) 0 else 8)

  final def nextMaskIndex(): Unit = {
    curMask = curMask << 1
    if (curMask == 0) {
      curMask = 1L
      curBitmapOffset += 8
    }
  }

  final def addNA(): Unit = {
    require(curBitmapOffset < bitmapMaskBufferSize, s"bitmapOverflow: $curBitmapOffset < $bitmapMaskBufferSize")
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

  final def noNAs: Boolean = {
    for { word <- 0 until curBitmapOffset/8 optimized } {
      if (UnsafeUtils.getLong(base, bitmapOffset + word * 8) != 0) return false
    }
    val naMask = curMask - 1
    (UnsafeUtils.getLong(base, bitmapOffset + curBitmapOffset) & naMask) == 0
  }

  override def primaryBytes: Int = (bitmapOffset - offset).toInt + bitmapBytes
  override def primaryMaxBytes: Int = (bitmapOffset - offset).toInt + bitmapMaskBufferSize

  final def copyMaskFrom(other: BitmapMaskAppendableVector[A]): Unit = {
    require(other.bitmapBytes <= this.bitmapMaskBufferSize)
    UnsafeUtils.unsafe.copyMemory(other.base, other.bitmapOffset,
                                  base, bitmapOffset,
                                  other.bitmapBytes)
    curBitmapOffset = other.curBitmapOffset
    curMask = other.curMask
  }
}
