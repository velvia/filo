package org.velvia.filo.vectors

import java.nio.ByteBuffer
import org.velvia.filo._
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scalaxy.loops._

/**
 * Constructor methods for UTF8 vector types, as well as UTF8/binary blob utilities
 */
object UTF8Vector {
  /**
   * Creates a standard UTF8Vector from a ByteBuffer or any memory location
   */
  def apply(base: Any, offset: Long, nBytes: Int): UTF8Vector =
    new UTF8Vector(base, offset) { val numBytes = nBytes }

  def apply(buffer: ByteBuffer): UTF8Vector = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new UTF8Vector(base, off) { val numBytes = len }
  }

  def fixedMax(buffer: ByteBuffer): FixedMaxUTF8Vector = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new FixedMaxUTF8VectorReader(base, off, len)
  }

  def const(buffer: ByteBuffer): UTF8ConstVector = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    new UTF8ConstVector(base, off, len)
  }

  /**
   * Creates an appendable UTF8 string vector given the max capacity and max elements.
   * Be conservative.  The amount of space needed is at least 4 + 4 * #strings + the space needed
   * for the strings themselves; add another 4 bytes per string when more than 32KB is needed.
   * @param maxBytes the initial max # of bytes allowed.  Will grow as needed.
   */
  def appendingVector(maxElements: Int, maxBytes: Int): BinaryAppendableVector[ZeroCopyUTF8String] = {
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(maxBytes)
    new GrowableVector(new UTF8AppendableVector(base, off, nBytes, maxElements))
  }

  /**
   * Creates an appendable FixedMaxUTF8Vector given the max capacity and max bytes per item.
   * @param maxElements the initial max # of elements to add.  Can grow as needed.
   * @param maxBytesPerItem the max bytes for any one item
   */
  def fixedMaxAppending(maxElements: Int, maxBytesPerItem: Int): BinaryAppendableVector[ZeroCopyUTF8String] = {
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(1 + maxElements * (maxBytesPerItem + 1))
    new GrowableVector(new FixedMaxUTF8AppendableVector(base, off, nBytes, maxBytesPerItem + 1))
  }

  /**
   * A convenience function which adds a bunch of ZeroCopyUTF8Strings to a vector.
   * @param maxBytes the initial max number of bytes for the vector to grow to
   * @param bytesPerItem if defined, use a FixedMaxUTF8Vector.  In this case the maxBytes is ignored.
   */
  def appendingVector(strings: Seq[ZeroCopyUTF8String],
                      maxBytes: Int,
                      bytesPerItem: Option[Int] = None): BinaryAppendableVector[ZeroCopyUTF8String] = {
    val vect = bytesPerItem.map(n => fixedMaxAppending(strings.length, n))
                           .getOrElse(appendingVector(strings.length, maxBytes))
    strings.foreach { str =>
      if (ZeroCopyUTF8String.isNA(str)) vect.addNA() else vect.addData(str)
    }
    vect
  }

  /**
   * Optimize the source UTF8 strings, creating a more optimal, smaller vector if possible
   * eg using DictUTF8Vector.
   * See [[DictUTF8Vector.shouldMakeDict]] for the parameters.
   */
  def writeOptimizedBuffer(strings: Seq[ZeroCopyUTF8String],
                           spaceThreshold: Double = 0.6,
                           samplingRate: Double = 0.3): ByteBuffer = {
    val builder = new UTF8VectorBuilder
    strings.foreach(builder.addData)
    builder.optimizedBuffer(spaceThreshold, samplingRate)
  }

  val SmallOffsetNBits = 20
  val SmallLenNBits = 31 - SmallOffsetNBits
  val MaxSmallOffset = Math.pow(2, SmallOffsetNBits).toInt - 1
  val MaxSmallLen    = Math.pow(2, SmallLenNBits).toInt - 1
  val SmallOffsetMask = MaxSmallOffset << SmallLenNBits
  val EmptyBlob      = 0x80000000
  val NAShort        = 0xff00.toShort       // Used only for FixedMaxUTF8Vector.  Zero length str.

  // Create the fixed-field int for variable length data blobs.  If the result is negative (bit 31 set),
  // then the offset and length are both packed in; otherwise, the fixed int is just an offset to a
  // 4-byte int containing length, followed by the actual blob
  final def blobFixedInt(offset: Int, blobLength: Int): Int =
    if (offset <= MaxSmallOffset && blobLength <= MaxSmallLen) {
      0x80000000 | (offset << SmallLenNBits) | blobLength
    } else {
      offset
    }

  final def smallOff(fixedData: Int): Int = (fixedData & SmallOffsetMask) >> SmallLenNBits
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
    val utf8off = offset + (if (fixedData < 0) smallOff(fixedData) else (fixedData + 4))
    val utf8len = if (fixedData < 0) fixedData & MaxSmallLen else UnsafeUtils.getInt(base, offset + fixedData)
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
  override val primaryMaxBytes = 4 + (maxElements * 4)
  private var curFixedOffset = offset + 4
  var numBytes: Int = primaryMaxBytes

  final def reset(): Unit = {
    _len = 0
    curFixedOffset = offset + 4
    numBytes = primaryMaxBytes
    UnsafeUtils.setInt(base, offset, 0)
  }

  override def primaryBytes: Int = (curFixedOffset - offset).toInt

  private def bumpLen(): Unit = {
    _len += 1
    curFixedOffset += 4
    UnsafeUtils.setInt(base, offset, _len)
  }

  final def addData(data: ZeroCopyUTF8String): Unit = {
    checkSize(length + 1, maxElements)
    val fixedData = appendBlob(data)
    UnsafeUtils.setInt(base, curFixedOffset, fixedData)
    bumpLen()
  }

  final def addNA(): Unit = {
    checkSize(length + 1, maxElements)
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

  override def newInstance(growFactor: Int = 2): UTF8AppendableVector = {
    val (newbase, newoff, nBytes) = BinaryVector.allocWithMagicHeader(maxBytes * growFactor)
    new UTF8AppendableVector(newbase, newoff, maxBytes * growFactor, maxElements * growFactor)
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
        val utf8len = if (fixedData < 0) fixedData & MaxSmallLen else UnsafeUtils.getInt(base, offset + fixedData)
        if (utf8len < min) min = utf8len
        if (utf8len > max) max = utf8len
      }
    }
    (min, max)
  }

  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[ZeroCopyUTF8String] = {
    val offsetDiff = -((maxElements - _len) * 4)
    adjustOffsets(newBase, newOff, offsetDiff)
    UTF8Vector(newBase, newOff, numBytes + offsetDiff)
  }

  // WARNING: no checking for if delta pushes small offsets out.  Intended for compactions only.
  private def adjustOffsets(newBase: Any, newOff: Long, delta: Int): Unit = {
    for { i <- 0 until _len optimized } {
      val fixedData = UnsafeUtils.getInt(newBase, newOff + 4 + i * 4)
      val newData = if (fixedData < 0) {
        if (fixedData == EmptyBlob) { EmptyBlob } else {
          val newDelta = smallOff(fixedData) + delta
          blobFixedInt(newDelta, fixedData & MaxSmallLen)
        }
      } else { fixedData + delta }
      UnsafeUtils.setInt(newBase, newOff + 4 + i * 4, newData)
    }
  }

  /**
   * Reserves space from the variable length area at the end.
   * If it succeeds, the numBytes will be moved up at the end of the call.
   * @return the Long offset at which the variable space starts
   */
  private def reserveVarBytes(bytesToReserve: Int): Long = {
    checkSize(numBytes + bytesToReserve, maxBytes)
    val offsetToWrite = offset + numBytes
    numBytes += bytesToReserve
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

/**
 * FixedMaxUTF8Vector allocates a fixed number of bytes for each item, which is 1 more than the max allowed
 * length of each item.  The length of each item is the first byte of each slot.
 * If the length of items does not vary a lot, this could save significant space compared to normal UTF8Vector
 */
abstract class FixedMaxUTF8Vector(val base: Any, val offset: Long) extends BinaryVector[ZeroCopyUTF8String] {
  def bytesPerItem: Int    // includes length byte

  override def length: Int = (numBytes - 1) / bytesPerItem
  private final val itemsOffset = offset + 1

  final def apply(index: Int): ZeroCopyUTF8String = {
    val itemOffset = itemsOffset + index * bytesPerItem
    val itemLen = UnsafeUtils.getByte(base, itemOffset) & 0x00ff
    new ZeroCopyUTF8String(base, itemOffset + 1, itemLen)
  }

  final def isAvailable(index: Int): Boolean =
    UnsafeUtils.getShort(base, itemsOffset + index * bytesPerItem) != UTF8Vector.NAShort
}

class FixedMaxUTF8VectorReader(base: Any, offset: Long, val numBytes: Int) extends
FixedMaxUTF8Vector(base, offset) {
  val bytesPerItem = UnsafeUtils.getByte(base, offset) & 0x00ff
}

/**
 * An appendable FixedMax vector.  NOTE:
 * @param bytesPerItem the max number of bytes allowed per item + 1 (for the length byte)
 */
class FixedMaxUTF8AppendableVector(base: Any,
                                   offset: Long,
                                   val maxBytes: Int,
                                   val bytesPerItem: Int) extends
FixedMaxUTF8Vector(base, offset) with BinaryAppendableVector[ZeroCopyUTF8String] {
  require(bytesPerItem > 1 && bytesPerItem <= 255)

  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_FIXEDMAXUTF8

  UnsafeUtils.setByte(base, offset, bytesPerItem.toByte)
  var numBytes = 1

  final def reset(): Unit = { numBytes = 1 }

  final def addData(item: ZeroCopyUTF8String): Unit = {
    require(item.length < bytesPerItem)
    checkSize(numBytes + bytesPerItem, maxBytes)
    // Easy way to ensure byte after length byte is zero (so cannot be NA)
    UnsafeUtils.setShort(base, offset + numBytes, item.length.toShort)
    item.copyTo(base, offset + numBytes + 1)
    numBytes += bytesPerItem
  }

  final def addNA(): Unit = {
    UnsafeUtils.setShort(base, offset + numBytes, UTF8Vector.NAShort)
    numBytes += bytesPerItem
  }

  // Not needed as this vector will not be optimized further
  final def isAllNA: Boolean = ???
  final def noNAs: Boolean = ???
}

class UTF8ConstVector(base: Any, offset: Long, numBytes: Int) extends
ConstVector[ZeroCopyUTF8String](base, offset, numBytes) {
  private final val _utf8 = new ZeroCopyUTF8String(base, dataOffset, numBytes - 4)
  def apply(i: Int): ZeroCopyUTF8String = _utf8
}

class UTF8VectorBuilder extends VectorBuilderBase {
  type T = ZeroCopyUTF8String

  // Start with a larger initial size to avoid resizing penalty.
  private val strings = new ArrayBuffer[ZeroCopyUTF8String](2000)
  private var numNAs: Int = 0
  private var numBytes: Int = 8      // Be conservative, dict encoding requires extra element
  private var maxStrLen: Int = 0

  final def addNA(): Unit = {
    strings += ZeroCopyUTF8String.NA
    numBytes += 4
    numNAs += 1
  }

  final def addData(value: T): Unit = {
    strings += value
    numBytes += 4 + value.length +
                (if (numBytes > 0xffff || value.length > 2047) 4 else 0)
    maxStrLen = Math.max(maxStrLen, value.length)
  }

  final def isAllNA: Boolean = numNAs == strings.length
  final def length: Int = strings.length
  final def reset(): Unit = { strings.clear }

  val extractor = RowReader.UTF8StringFieldExtractor

  def toFiloBuffer(hint: BuilderEncoder.EncodingHint): ByteBuffer = hint match {
    case BuilderEncoder.SimpleEncoding =>
      UTF8Vector.appendingVector(strings, numBytes).toFiloBuffer

    case BuilderEncoder.DictionaryEncoding =>
      optimizedBuffer(spaceThreshold=1.1)

    case other: Any =>
      optimizedBuffer()
  }

  def optimizedBuffer(spaceThreshold: Double = 0.6,
                      samplingRate: Double = 0.3): ByteBuffer =
    DictUTF8Vector.shouldMakeDict(strings, spaceThreshold, samplingRate, numBytes).map { dictInfo =>
      if (numNAs == 0 && dictInfo.codeMap.size == 1) {
        val (b, o, n) = ConstVector.make(length, strings.head.length) { case (base, off) =>
          strings.head.copyTo(base, off)
        }
        new UTF8ConstVector(b, o, n).toFiloBuffer
      } else { DictUTF8Vector.makeBuffer(dictInfo) }
    }.getOrElse {
      val fixedMaxSize = 1 + (maxStrLen + 1) * strings.length
      val maxSizeOpt = if (fixedMaxSize < numBytes && maxStrLen < 255) Some(Math.max(maxStrLen, 1)) else None
      UTF8Vector.appendingVector(strings, numBytes, maxSizeOpt).toFiloBuffer()
    }
}