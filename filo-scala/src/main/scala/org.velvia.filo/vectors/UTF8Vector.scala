package org.velvia.filo.vectors

import org.velvia.filo._
import scala.language.postfixOps
import scalaxy.loops._

/**
 * Constructor methods for UTF8 vector types, as well as UTF8/binary blob utilities
 */
object UTF8Vector {
  /**
   * Creates a standard UTF8Vector from any memory location.
   */
  def apply(base: Any, offset: Long, nBytes: Int): UTF8Vector =
    new UTF8Vector(base, offset) { val numBytes = nBytes }

  /**
   * Creates an appendable UTF8 string vector given the max capacity and max elements.
   * Be conservative.  The amount of space needed is at least 4 + 4 * #strings + the space needed
   * for the strings themselves; add another 4 bytes per string when more than 32KB is needed.
   */
  def appendingVector(maxElements: Int, maxBytes: Int): UTF8AppendableVector = {
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(maxBytes)
    new UTF8AppendableVector(base, off, nBytes, maxElements)
  }

  val MaxSmallOffset = 0x7fff
  val MaxSmallLen    = 0xffff
  val EmptyBlob      = 0x80000000

  // Create the fixed-field int for variable length data blobs.  If the result is negative (bit 31 set),
  // then the offset and length are both packed in; otherwise, the fixed int is just an offset to a
  // 4-byte int containing length, followed by the actual blob
  final def blobFixedInt(offset: Int, blobLength: Int): Int =
    if (offset <= MaxSmallOffset && blobLength <= MaxSmallLen) {
      0x80000000 | (offset << 16) | blobLength
    } else {
      offset
    }
}

/**
 * A BinaryVector holding UTF8Strings or blobs.
 * It has two advantages over the FBB-based SimpleStringVector:
 * 1) UTF8Strings, no need to serialize/deserialize
 * 2) More compact, can store shorter strings with 4-byte overhead instead of 8
 *
 * Layout:
 * +0   word       number of elements
 * +4...nElems*4   each string has 32-bit word, which contains both offset+length or just offset
 */
abstract class UTF8Vector(val base: Any, val offset: Long) extends
BinaryVector[ZeroCopyUTF8String] {
  import UTF8Vector._

  override def length: Int = UnsafeUtils.getInt(base, offset)

  final def apply(index: Int): ZeroCopyUTF8String = {
    val fixedData = UnsafeUtils.getInt(base, offset + 4 + index * 4)
    val utf8off = offset + (if (fixedData < 0) ((fixedData & 0x7fff0000) >> 16) else (fixedData + 4))
    val utf8len = if (fixedData < 0) fixedData & 0xffff else UnsafeUtils.getInt(base, offset + fixedData)
    new ZeroCopyUTF8String(base, utf8off, utf8len)
  }

  final def isAvailable(index: Int): Boolean =
    UnsafeUtils.getInt(base, offset + 4 + index * 4) != EmptyBlob
}

/**
 * The appendable (and readable) version of UTF8Vector, with some goodies including finding min/max lengths
 * of all strings/blobs, and estimating or getting set of unique strings
 */
class UTF8AppendableVector(base: Any, offset: Long, val maxBytes: Int, maxElements: Int) extends
UTF8Vector(base, offset) with BinaryAppendableVector[ZeroCopyUTF8String] {
  import UTF8Vector._

  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_UTF8

  private var _len = 0
  override final def length: Int = _len
  private val fixedNumBytes = 4 + (maxElements * 4)
  private var curFixedOffset = offset + 4
  var numBytes: Int = fixedNumBytes

  private def bumpLen(): Unit = {
    _len += 1
    curFixedOffset += 4
    UnsafeUtils.setInt(base, offset, _len)
  }

  final def addData(data: ZeroCopyUTF8String): Unit = {
    require(length < maxElements)
    val fixedData = appendBlob(data)
    UnsafeUtils.setInt(base, curFixedOffset, fixedData)
    bumpLen()
  }

  final def addNA(): Unit = {
    require(length < maxElements)
    UnsafeUtils.setInt(base, curFixedOffset, EmptyBlob)
    bumpLen()
  }

  final def isAllNA: Boolean = {
    var fixedOffset = offset + 4
    while (fixedOffset < curFixedOffset) {
      if (UnsafeUtils.getInt(base, fixedOffset) != EmptyBlob) return false
      fixedOffset += 4
    }
    return true
  }

  final def noNAs: Boolean = {
    var fixedOffset = offset + 4
    while (fixedOffset < curFixedOffset) {
      if (UnsafeUtils.getInt(base, fixedOffset) == EmptyBlob) return false
      fixedOffset += 4
    }
    return true
  }

  /**
   * Returns the minimum and maximum length (# bytes) of all the elements.
   * Useful for calculating which type of UTF8Vector to use.
   * @return (Int, Int) = (minBytes, maxBytes) of all elements
   */
  final def minMaxStrLen: (Int, Int) = {
    var min = Int.MaxValue
    var max = 0
    for { index <- 0 until _len optimized } {
      val fixedData = UnsafeUtils.getInt(base, offset + 4 + index * 4)
      if (fixedData != EmptyBlob) {
        val utf8len = if (fixedData < 0) fixedData & 0xffff else UnsafeUtils.getInt(base, offset + fixedData)
        if (utf8len < min) min = utf8len
        if (utf8len > max) max = utf8len
      }
    }
    (min, max)
  }

  override final def freeze(): BinaryVector[ZeroCopyUTF8String] = {
    if (_len == maxElements) { this.asInstanceOf[BinaryVector[ZeroCopyUTF8String]] }
    else {
      val offsetDiff = (maxElements - _len) * 4
      // Move the blob data back
      copyTo(base, curFixedOffset, fixedNumBytes, numBytes - fixedNumBytes)
      // Adjust all the offsets back
      for { i <- 0 until _len optimized } {
        val fixedData = UnsafeUtils.getInt(base, offset + 4 + i * 4)
        val newData = if (fixedData < 0) {
          val newDelta = ((fixedData & 0x7fff0000) >> 16) - offsetDiff
          blobFixedInt(newDelta, fixedData & 0xffff)
        } else { fixedData - offsetDiff }
        UnsafeUtils.setInt(base, offset + 4 + i * 4, newData)
      }
      UTF8Vector(base, offset, numBytes - offsetDiff)
    }
  }

  /**
   * Reserves space from the variable length area at the end.  Space will always be word-aligned.
   * If it succeeds, the numBytes will be moved up at the end of the call.
   * @return the Long offset at which the variable space starts
   */
  private def reserveVarBytes(bytesToReserve: Int): Long = {
    val roundedLen = (bytesToReserve + 3) & -4
    require(numBytes + roundedLen <= maxBytes)
    val offsetToWrite = offset + numBytes
    numBytes += roundedLen
    offsetToWrite
  }

  /**
   * Appends a variable length blob to the end, returning the 32-bit fixed length data field that either
   * contains both offset and length or just the offset, in which case first 4 bytes in var section contains
   * the length.  Bytes will be copied from original blob.
   */
  private def appendBlob(blob: ZeroCopyBinary): Int = {
    // First, get the fixed int which encodes offset and len and see if we need another 4 bytes for offset
    val fixedData = blobFixedInt(numBytes, blob.length)
    val destOffset = reserveVarBytes(blob.length + (if (fixedData < 0) 0 else 4))
    if (fixedData < 0) {
      blob.copyTo(base, destOffset)
    } else {
      UnsafeUtils.setInt(base, destOffset, blob.length)
      blob.copyTo(base, destOffset + 4)
    }
    fixedData
  }
}