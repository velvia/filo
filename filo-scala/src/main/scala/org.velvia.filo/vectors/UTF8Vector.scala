package org.velvia.filo.vectors

import java.nio.ByteBuffer
import org.velvia.filo._
import scala.collection.mutable.ArrayBuffer
import scalaxy.loops._

/**
 * Constructor methods for UTF8 vector types, as well as UTF8/binary blob utilities
 */
object UTF8Vector {
  /**
   * Creates a UTF8Vector that holds references to original UTF8 strings, but can optimize to final forms.
   * Typical usage:  {{{ UTF8Vector(strings).optimize().toFiloBuffer }}}
   * Or to control dictionary encoding:  use optimizedVector(...)
   */
  def apply(strings: Seq[ZeroCopyUTF8String]): BinaryAppendableVector[ZeroCopyUTF8String] = {
    val vect = appendingVector(strings.length)
    strings.foreach { str =>
      if (ZeroCopyUTF8String.isNA(str)) vect.addNA() else vect.addData(str)
    }
    vect
  }

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
   * Creates an appendable UTF8 vector which stores references only, but is a good starting point for
   * optimizing into other more optimized UTF8 vector types.
   */
  def appendingVector(maxElements: Int): BinaryAppendableVector[ZeroCopyUTF8String] = {
    val maxBytes = maxElements * ObjectVector.objectRefSize
    // Be sure to store this on the heap.  Object refs and offheap don't mix
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(maxBytes, offheap=false)
    new GrowableVector(new UTF8PtrAppendable(base, off, maxBytes))
  }

  /**
   * Creates an appendable UTF8 string vector given the max capacity and max elements.
   * This can be written to wire but not as optimized as FixedMax and DictUTF8 vectors.
   * Be conservative.  The amount of space needed is at least 4 + 4 * #strings + the space needed
   * for the strings themselves; add another 4 bytes per string when more than 32KB is needed.
   * @param maxBytes the initial max # of bytes allowed.  Will grow as needed.
   * @param offheap if true, allocate the space for the vector off heap.  User will have to dispose.
   */
  def flexibleAppending(maxElements: Int,
                        maxBytes: Int,
                        offheap: Boolean = false): BinaryAppendableVector[ZeroCopyUTF8String] = {
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(maxBytes, offheap)
    new GrowableVector(new UTF8AppendableVector(base, off, nBytes, maxElements))
  }

  /**
   * Creates an appendable FixedMaxUTF8Vector given the max capacity and max bytes per item.
   * @param maxElements the initial max # of elements to add.  Can grow as needed.
   * @param maxBytesPerItem the max bytes for any one item
   * @param offheap if true, allocate the space for the vector off heap.  User will have to dispose.
   */
  def fixedMaxAppending(maxElements: Int,
                        maxBytesPerItem: Int,
                        offheap: Boolean = false): BinaryAppendableVector[ZeroCopyUTF8String] = {
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(1 + maxElements * (maxBytesPerItem + 1),
                                                                offheap)
    new GrowableVector(new FixedMaxUTF8AppendableVector(base, off, nBytes, maxBytesPerItem + 1))
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
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_UTF8
  val maybeNAs = true

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
 * The appendable (and readable) version of UTF8Vector.  Copies original strings into new space - so be
 * sure this is what you want.
 */
class UTF8AppendableVector(base: Any, offset: Long, val maxBytes: Int, maxElements: Int) extends
UTF8Vector(base, offset) with BinaryAppendableVector[ZeroCopyUTF8String] {
  import UTF8Vector._

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
    val (newbase, newoff, nBytes) = BinaryVector.reAlloc(base, maxBytes * growFactor)
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

import BuilderEncoder._

/**
 * Not a vector that can be sent over the wire, instead it is used to append source UTF8String objects
 * quickly without serializing, and as a basis for optimizing into one of the other UTF8 vectors.
 */
class UTF8PtrAppendable(base: Any, offset: Long, maxBytes: Int) extends
ObjectVector[ZeroCopyUTF8String](base, offset, maxBytes) {
  private var maxStrLen = 0
  var flexBytes = 0

  override def addData(data: ZeroCopyUTF8String): Unit = if (data != UnsafeUtils.ZeroPointer) {
    super.addData(data)
    flexBytes += 4 + data.length +
                 (if (numBytes > 0xffff || data.length > 2047) 4 else 0)
    maxStrLen = Math.max(maxStrLen, data.length)
  } else {
    addNA()
  }

  override def addNA(): Unit = {
    super.addNA()
    flexBytes += 4
  }

  def suboptimize(hint: EncodingHint = AutoDetect): BinaryVector[ZeroCopyUTF8String] = hint match {
    case AutoDictString(spaceThreshold, samplingRate) => optimizedVector(spaceThreshold, samplingRate)
    case BuilderEncoder.DictionaryEncoding            => optimizedVector(spaceThreshold=1.1)
    case BuilderEncoder.SimpleEncoding =>
      val newVect = UTF8Vector.flexibleAppending(length, flexBytes)
      newVect.addVector(this)
      newVect.optimize()
    case hint: Any => optimizedVector()
  }

  override def newInstance(growFactor: Int = 2): ObjectVector[ZeroCopyUTF8String] = {
    val (newbase, newoff, nBytes) = BinaryVector.reAlloc(base, maxBytes * growFactor)
    new UTF8PtrAppendable(newbase, newoff, nBytes)
  }

  def optimizedVector(spaceThreshold: Double = 0.6,
                      samplingRate: Double = 0.3): BinaryVector[ZeroCopyUTF8String] =
    DictUTF8Vector.shouldMakeDict(this, spaceThreshold, samplingRate, flexBytes + 512).map { dictInfo =>
      if (noNAs && dictInfo.codeMap.size == 1) {
        (new UTF8ConstAppendingVect(apply(0), length)).optimize()
      } else { DictUTF8Vector.makeVector(dictInfo, isOffheap) }
    }.getOrElse {
      val fixedMaxSize = 1 + (maxStrLen + 1) * length
      val vect = if (fixedMaxSize < flexBytes && maxStrLen < 255) {
        UTF8Vector.fixedMaxAppending(length, Math.max(maxStrLen, 1), isOffheap)
      } else {
        UTF8Vector.flexibleAppending(length, flexBytes, isOffheap)
      }
      vect.addVector(this)
      vect.optimize()
    }
}

/**
 * FixedMaxUTF8Vector allocates a fixed number of bytes for each item, which is 1 more than the max allowed
 * length of each item.  The length of each item is the first byte of each slot.
 * If the length of items does not vary a lot, this could save significant space compared to normal UTF8Vector
 */
abstract class FixedMaxUTF8Vector(val base: Any, val offset: Long) extends BinaryVector[ZeroCopyUTF8String] {
  def bytesPerItem: Int    // includes length byte

  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_FIXEDMAXUTF8
  val maybeNAs = true

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

  def finishCompaction(newBase: Any, newOff: Long): BinaryVector[ZeroCopyUTF8String] =
    new FixedMaxUTF8VectorReader(newBase, newOff, numBytes)
}

class UTF8ConstVector(base: Any, offset: Long, numBytes: Int) extends
ConstVector[ZeroCopyUTF8String](base, offset, numBytes) {
  private final val _utf8 = new ZeroCopyUTF8String(base, dataOffset, numBytes - 4)
  def apply(i: Int): ZeroCopyUTF8String = _utf8
}

class UTF8ConstAppendingVect(value: ZeroCopyUTF8String, initLen: Int = 0) extends
ConstAppendingVector(value, value.length, initLen) {
  def fillBytes(base: Any, offset: Long): Unit = value.copyTo(base, offset)
  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[ZeroCopyUTF8String] =
    new UTF8ConstVector(newBase, newOff, numBytes)
}

class UTF8VectorBuilder(inner: BinaryAppendableVector[ZeroCopyUTF8String])
extends BinaryVectorBuilder[ZeroCopyUTF8String](inner)