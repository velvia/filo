package org.velvia.filo

import java.nio.ByteBuffer
import scalaxy.loops._

import RowReader._
import BuilderEncoder._

/**
 * This is really the same as FiloVector, but supports off-heap easier.
 * An immutable, zero deserialization, minimal/zero allocation, insanely fast binary sequence.
 * TODO: maybe merge this and FiloVector, or just make FiloVector support ZeroCopyBinary.
 */
trait BinaryVector[@specialized(Int, Long, Double, Boolean) A] extends FiloVector[A] with ZeroCopyBinary {
  /** The major and subtype bytes as defined in WireFormat that will go into the FiloVector header */
  def vectMajorType: Int
  def vectSubType: Int

  /**
   * Should return false if this vector definitely has no NAs, and true if it might have some or is
   * designed to support NAs.
   * Returning false allows optimizations for aggregations.
   */
  def maybeNAs: Boolean

  def isOffheap: Boolean = base == UnsafeUtils.ZeroPointer

  /**
   * Frees up memory used by this BinaryVector if it was offheap memory.  Otherwise do nothing
   */
  def dispose(): Unit = {
    if (base == UnsafeUtils.ZeroPointer) UnsafeUtils.freeOffheap(offset)
  }

  /**
   * Produce a FiloVector ByteBuffer with the four-byte header.  The resulting buffer can be used for I/O
   * and fed to FiloVector.apply() to be parsed back.  For most BinaryVectors returned by optimize() and
   * freeze(), a copy is not necessary so this is usually a very inexpensive operation.
   */
  def toFiloBuffer: ByteBuffer = {
    def copyIt(): Array[Byte] = {
      val bytes = new Array[Byte](numBytes + 4)
      copyTo(bytes, UnsafeUtils.arayOffset + 4)
      UnsafeUtils.setInt(bytes, UnsafeUtils.arayOffset, WireFormat(vectMajorType, vectSubType))
      bytes
    }
    // Check if magic word written to header location.  Then write header, and wrap all the bytes
    // in a ByteBuffer and return that.  Assumes byte array properly allocated beforehand.
    // If that doesn't work, copy bytes to new array first then write header.
    val byteArray = base match {
      case a: Array[Byte] if offset == (UnsafeUtils.arayOffset + 4) &&
                             UnsafeUtils.getInt(base, offset - 4) == BinaryVector.HeaderMagic =>
        UnsafeUtils.setInt(base, offset - 4, WireFormat(vectMajorType, vectSubType))
        a
      case x: Any => copyIt()
      case UnsafeUtils.ZeroPointer => copyIt()
    }
    val bb = ByteBuffer.wrap(byteArray)
    bb.limit(numBytes + 4)
    bb.order(java.nio.ByteOrder.LITTLE_ENDIAN)
    bb
  }
}

trait PrimitiveVector[@specialized(Int, Long, Double, Boolean) A] extends BinaryVector[A] {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE_NOMASK
  val maybeNAs = false
}

object BinaryVector {
  /** Used to reserve memory location "4-byte header will go here" */
  val HeaderMagic = 0x87654300  // The lower 8 bits should not be 00

  /**
   * Allocate a byte array with nBytes usable bytes, prepended with a 4-byte HeaderMagic
   * to reserve space for writing the FiloVector header.
   * @param offheap if true, allocate offheap storage, which means vector must be freed.
   * @param initialize if true, zero out memory before allocation.  Only meaningful when offheap=true
   * @return (base, offset, numBytes) tuple for the appendingvector
   */
  def allocWithMagicHeader(nBytes: Int,
                           offheap: Boolean = false,
                           initialize: Boolean = true): (Any, Long, Int) = {
    if (offheap) {
      (UnsafeUtils.ZeroPointer, UnsafeUtils.allocOffheap(nBytes, initialize), nBytes)
      // TODO: modify toFiloBuffer to work with offheap?  But cannot have ByteBuffer that refers to
      // truly offheap memory.
    } else {
      val newBytes = new Array[Byte](nBytes + 4)
      UnsafeUtils.setInt(newBytes, UnsafeUtils.arayOffset, HeaderMagic)
      (newBytes, UnsafeUtils.arayOffset + 4, nBytes)
    }
  }

  def reAlloc(base: Any, nBytes: Int): (Any, Long, Int) =
    if (base == UnsafeUtils.ZeroPointer) { allocWithMagicHeader(nBytes, offheap=true) }
    else                                 { allocWithMagicHeader(nBytes, offheap=false) }
}

/**
 * A BinaryVector with an NaMask bitmap for NA values (1/on for missing NA values), 64-bit chunks
 */
trait BitmapMaskVector[A] extends BinaryVector[A] {
  def base: Any
  def bitmapOffset: Long   // NOTE: should be offset + n
  val maybeNAs = true

  final def isAvailable(index: Int): Boolean = {
    // NOTE: length of bitMask may be less than (length / 64) longwords.
    val maskIndex = index >> 6
    val maskVal = UnsafeUtils.getLong(base, bitmapOffset + maskIndex * 8)
    (maskVal & (1L << (index & 63))) == 0
  }
}

trait PrimitiveMaskVector[@specialized(Int, Long, Double, Boolean) A] extends BitmapMaskVector[A] {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE
}

object BitmapMask {
  def numBytesRequired(elements: Int): Int = ((elements + 63) / 64) * 8
}

case class VectorTooSmall(bytesNeeded: Int, bytesHave: Int) extends
Exception(s"Need $bytesNeeded bytes, but only have $bytesHave")

/**
 * A BinaryVector that you can append to.  Has some notion of a maximum size (max # of items or bytes)
 * and the user is responsible for resizing if necessary (but see GrowableVector).
 *
 * Replaces the current VectorBuilder API, and greatly simplifies overall APIs in Filo.  AppendableVectors
 * are still FiloVectors so they could be read as they are being built -- and the new optimize() APIS
 * take advantage of this for more immutable optimization.  This approach also lets a user select the
 * amount of CPU to spend optimizing.
 *
 * NOTE: AppendableVectors are not designed to be multi-thread safe for writing.  They are designed for
 * high single-thread performance.
 *
 * ## Use Cases and LifeCycle
 *
 * The AppendingVector is mutable and can be reset().  THe idea is to call freeze() or optimize() to produce
 * more compact forms for I/O or longer term storage, but all the forms are readable.  This gives the user
 * great flexibility to choose compression tradeoffs.
 *
 * 1. Fill up one of the appendingVectors and query as is.  Use it like a Seq[].  No need to optimize at all.
 * 2.  --> freeze() --> toFiloBuffer ...  compresses pointer/bitmask only
 * 3.  --> optimize() --> toFiloBuffer ... aggressive compression using all techniques available
 */
trait BinaryAppendableVector[@specialized(Int, Long, Double, Boolean) A] extends BinaryVector[A] {
  import RowReader._

  /** Max size that current buffer can grow to */
  def maxBytes: Int

  /** Add a Not Available (null) element to the builder. */
  def addNA(): Unit

  /** Add a value of type T to the builder.  It will be marked as available. */
  def addData(value: A): Unit

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
   * Checks to see if enough bytes or need to allocate more space.
   */
  def checkSize(need: Int, have: Int): Unit =
    if (!(need <= have)) throw VectorTooSmall(need, have)

  /**
   * Allocates a new instance of itself growing by factor growFactor.
   * Needs to be defined for any vectors that GrowableVector wraps.
   */
  def newInstance(growFactor: Int = 2): BinaryAppendableVector[A] = ???

  /**
   * Returns the number of bytes required for a compacted AppendableVector
   * The default implementation must be overridden if freeze() is also overridden.
   */
  def frozenSize: Int =
    if (numBytes >= primaryMaxBytes) numBytes - (primaryMaxBytes - primaryBytes) else numBytes

  /**
   * Compact this vector, removing unused space and return an immutable version of this FiloVector
   * that cannot be appended to, by default in a new space using up minimum space.
   *
   * WARNING: by default this copies the bytes so that the current bytes are not modified.  Do not set
   *          copy to false unless you know what you are doing, this may move bytes in place and cause
   *          heartache and unusable AppendableVectors.
   *
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
  def freeze(copy: Boolean = true): BinaryVector[A] = {
    if (copy) {
      val (base, off, _) = BinaryVector.allocWithMagicHeader(frozenSize, isOffheap)
      freeze(Some((base, off)))
    } else {
      freeze(None)
    }
  }

  def freeze(newBaseOffset: Option[(Any, Long)]): BinaryVector[A] =
    if (newBaseOffset.isEmpty && numBytes == frozenSize) {
      // It is better to return a simple read-only BinaryVector, for two reasons:
      // 1) It cannot be cast into an AppendingVector and made mutable
      // 2) It is probably higher performance for reads
      finishCompaction(base, offset)
    } else {
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
  def finishCompaction(newBase: Any, newOff: Long): BinaryVector[A]

  /**
   * Run some heuristics over this vector and automatically determine and return a more optimized
   * vector if possible.  The default implementation just does freeze(), but for example
   * an IntVector could return a vector with a smaller bit packed size given a max integer.
   * This always produces a new, frozen copy and takes more CPU than freeze() but can result in dramatically
   * smaller vectors using advanced techniques such as delta-delta or dictionary encoding.
   */
  def optimize(hint: EncodingHint = AutoDetect): BinaryVector[A] = freeze()

  /**
   * Clears the elements so one can start over.
   */
  def reset(): Unit
}

case class GrowableVector[@specialized(Int, Long, Double, Boolean) A](var inner: BinaryAppendableVector[A])
extends AppendableVectorWrapper[A, A] {
  def addData(value: A): Unit = {
    try {
      inner.addData(value)
    } catch {
      case e: VectorTooSmall =>
        val newInst = inner.newInstance()
        newInst.addVector(inner)
        inner.dispose()   // free memory in case it's off-heap .. just before reference is lost
        inner = newInst
        inner.addData(value)
    }
  }

  override def addNA(): Unit = {
    try {
      inner.addNA()
    } catch {
      case e: VectorTooSmall =>
        val newInst = inner.newInstance()
        newInst.addVector(inner)
        inner = newInst
        inner.addNA()
    }
  }

  def apply(index: Int): A = inner(index)
}

trait AppendableVectorWrapper[A, I] extends BinaryAppendableVector[A] {
  def inner: BinaryAppendableVector[I]

  def addNA(): Unit = inner.addNA()

  final def isAvailable(index: Int): Boolean = inner.isAvailable(index)
  final def base: Any = inner.base
  final def numBytes: Int = inner.numBytes
  final def offset: Long = inner.offset
  final override def length: Int = inner.length

  final def maybeNAs: Boolean = inner.maybeNAs
  final def maxBytes: Int = inner.maxBytes
  def isAllNA: Boolean = inner.isAllNA
  def noNAs: Boolean = inner.noNAs
  override def primaryBytes: Int = inner.primaryBytes
  override def primaryMaxBytes: Int = inner.primaryMaxBytes
  def vectMajorType: Int = inner.vectMajorType
  def vectSubType: Int = inner.vectSubType
  override def finishCompaction(newBase: Any, newOff: Long): BinaryVector[A] =
    inner.finishCompaction(newBase, newOff).asInstanceOf[BinaryVector[A]]
  override def frozenSize: Int = inner.frozenSize
  final def reset(): Unit = inner.reset()
  override def optimize(hint: EncodingHint = AutoDetect): BinaryVector[A] =
    inner.optimize(hint).asInstanceOf[BinaryVector[A]]
}

/**
 * Wrapper around a BinaryAppendableVector that fits the VectorBuilder APIs.
 * toFiloBuffer needs to be implemented for each specific type.
 */
abstract class BinaryVectorBuilder[@specialized(Int, Long, Double, Boolean) A: TypedFieldExtractor]
  (inner: BinaryAppendableVector[A]) extends VectorBuilderBase {
  type T = A

  final def addNA(): Unit = inner.addNA()
  final def addData(value: T): Unit = inner.addData(value)
  final def isAllNA: Boolean = inner.isAllNA
  final def length: Int = inner.length
  final def reset(): Unit = inner.reset()

  def toFiloBuffer(hint: BuilderEncoder.EncodingHint): ByteBuffer = inner match {
    case GrowableVector(inside: BinaryAppendableVector[A]) => inside.optimize(hint).toFiloBuffer
    case v: BinaryAppendableVector[A] =>   v.optimize(hint).toFiloBuffer
  }

  val extractor: TypedFieldExtractor[A] = implicitly[TypedFieldExtractor[A]]
}

/**
 * A BinaryAppendableVector for simple primitive types, ie where each element has a fixed length
 * and every element is available (there is no bitmap NA mask).
 */
abstract class PrimitiveAppendableVector[@specialized(Int, Long, Double, Boolean) A]
  (val base: Any, val offset: Long, val maxBytes: Int, nbits: Short, signed: Boolean)
extends BinaryAppendableVector[A] {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType = WireFormat.SUBTYPE_PRIMITIVE_NOMASK
  var writeOffset: Long = offset + 4
  var bitShift: Int = 0
  override final def length: Int = ((writeOffset - offset - 4).toInt * 8 + bitShift) / nbits

  UnsafeUtils.setShort(base, offset, nbits.toShort)
  UnsafeUtils.setByte(base, offset + 2, if (signed) 1 else 0)

  def numBytes: Int = (writeOffset - offset).toInt + (if (bitShift != 0) 1 else 0)

  private final val dangerZone = offset + maxBytes
  final def checkOffset(): Unit =
    if (writeOffset >= dangerZone) throw VectorTooSmall((writeOffset - offset).toInt, maxBytes)

  protected final def bumpBitShift(): Unit = {
    bitShift = (bitShift + nbits) % 8
    if (bitShift == 0) writeOffset += 1
    UnsafeUtils.setByte(base, offset + 3, bitShift.toByte)
  }

  final def isAvailable(index: Int): Boolean = true

  override final def addVector(other: BinaryVector[A]): Unit = other match {
    case v: BitmapMaskAppendableVector[A] =>
      addVector(v.subVect)
    case v: BinaryVector[A] =>
      // Optimization: this vector does not support NAs so just add the data
      require(numBytes + (nbits * v.length / 8) <= maxBytes,
             s"Not enough space to add ${v.length} elems; nbits=$nbits; need ${maxBytes-numBytes} bytes")
      for { i <- 0 until v.length optimized } { addData(v(i)) }
  }

  final def isAllNA: Boolean = (length == 0)
  final def noNAs: Boolean = (length > 0)
  val maybeNAs = false

  def reset(): Unit = {
    writeOffset = offset + 4
    bitShift = 0
  }
}

/**
 * Maintains a fast NA bitmap mask as we append elements
 */
abstract class BitmapMaskAppendableVector[@specialized(Int, Long, Double, Boolean) A]
  (val base: Any, val bitmapOffset: Long, maxElements: Int)
extends BitmapMaskVector[A] with BinaryAppendableVector[A] {
  // The base vector holding the actual values
  def subVect: BinaryAppendableVector[A]

  val bitmapMaskBufferSize = BitmapMask.numBytesRequired(maxElements)
  var curBitmapOffset = 0
  var curMask: Long = 1L

  UnsafeUtils.unsafe.setMemory(base, bitmapOffset, bitmapMaskBufferSize, 0)

  val subVectOffset = 4 + bitmapMaskBufferSize
  UnsafeUtils.setInt(base, offset, subVectOffset)

  // The number of bytes taken up by the bitmap mask right now
  final def bitmapBytes: Int = curBitmapOffset + (if (curMask == 1L) 0 else 8)

  final def nextMaskIndex(): Unit = {
    curMask = curMask << 1
    if (curMask == 0) {
      curMask = 1L
      curBitmapOffset += 8
    }
  }

  final def resetMask(): Unit = {
    UnsafeUtils.unsafe.setMemory(base, bitmapOffset, bitmapMaskBufferSize, 0)
    curBitmapOffset = 0
    curMask = 1L
  }

  final def reset(): Unit = {
    subVect.reset()
    resetMask()
  }

  override final def length: Int = subVect.length
  final def numBytes: Int = 4 + bitmapMaskBufferSize + subVect.numBytes
  final def apply(index: Int): A = subVect.apply(index)

  final def addNA(): Unit = {
    checkSize(curBitmapOffset, bitmapMaskBufferSize)
    val maskVal = UnsafeUtils.getLong(base, bitmapOffset + curBitmapOffset)
    UnsafeUtils.setLong(base, bitmapOffset + curBitmapOffset, maskVal | curMask)
    subVect.addNA()
    nextMaskIndex()
  }

  final def addData(value: A): Unit = {
    subVect.addData(value)
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

  override final def addVector(other: BinaryVector[A]): Unit = other match {
    // Optimized case: we are empty, so just copy over entire bitmap from other one
    case v: BitmapMaskAppendableVector[A] if length == 0 =>
      copyMaskFrom(v)
      subVect.addVector(v.subVect)
    // Non-optimized  :(
    case v: BinaryVector[A] =>
      super.addVector(other)
  }

  final def copyMaskFrom(other: BitmapMaskAppendableVector[A]): Unit = {
    checkSize(other.bitmapBytes, this.bitmapMaskBufferSize)
    UnsafeUtils.unsafe.copyMemory(other.base, other.bitmapOffset,
                                  base, bitmapOffset,
                                  other.bitmapBytes)
    curBitmapOffset = other.curBitmapOffset
    curMask = other.curMask
  }
}
