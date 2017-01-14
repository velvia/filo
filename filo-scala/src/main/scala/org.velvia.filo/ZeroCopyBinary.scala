package org.velvia.filo

import net.jpountz.xxhash.XXHashFactory
import scala.language.postfixOps
import scalaxy.loops._

/**
 * Essentially like a (void *) pointer to an untyped binary blob which supports very basic operations.
 * Allows us to do zero-copy comparisons and other ops.
 * Intended for small blobs like UTF8-encoded strings.
 * The API is immutable, but the underlying bytes could change- but that is not recommended.
 */
trait ZeroCopyBinary extends Ordered[ZeroCopyBinary] {
  import ZeroCopyBinary._

  def base: Any
  def offset: Long
  def numBytes: Int

  def length: Int = numBytes

  /**
   * Compares byte by byte starting with byte 0
   */
  def compare(other: ZeroCopyBinary): Int = {
    val minLen = Math.min(numBytes, other.numBytes)
    // TODO: compare 4 bytes at a time, or even 8
    val minLenAligned = minLen & -4
    val wordComp = UnsafeUtils.wordCompare(base, offset, other.base, other.offset, minLenAligned)
    if (wordComp == 0) {
      for { i <- minLenAligned until minLen optimized } {
        val res = getByte(i) - other.getByte(i)
        if (res != 0) return res
      }
      return numBytes - other.numBytes
    } else wordComp
  }

  final def getByte(byteNum: Int): Byte = UnsafeUtils.getByte(base, offset + byteNum)

  final def copyTo(dest: Any, destOffset: Long, delta: Int = 0, n: Int = numBytes): Unit =
    UnsafeUtils.unsafe.copyMemory(base, offset + delta, dest, destOffset, n)

  final def asNewByteArray: Array[Byte] = {
    val newArray = new Array[Byte](numBytes)
    copyTo(newArray, UnsafeUtils.arayOffset)
    newArray
  }

  /**
   * Returns an array of bytes.  If this ZeroCopyBinary is already a byte array
   * with exactly numBytes bytes, then just return that, to avoid another copy.
   * Otherwise, call asNewByteArray to return a copy.
   */
  def bytes: Array[Byte] = {
    //scalastyle:off
    if (base != null && base.isInstanceOf[Array[Byte]] && offset == UnsafeUtils.arayOffset) {
      //scalastyle:on
      base.asInstanceOf[Array[Byte]]
    } else {
      asNewByteArray
    }
  }

  override def equals(other: Any): Boolean = other match {
    case z: ZeroCopyBinary =>
      (numBytes == z.numBytes) && UnsafeUtils.equate(base, offset, z.base, z.offset, numBytes)
    case o: Any =>
      false
  }

  // Ideally, hash without copying to another byte array, esp if the base storage is a byte array already
  lazy val cachedHash32: Int = {
    base match {
      case a: Array[Byte] => hasher32.hash(a, offset.toInt - UnsafeUtils.arayOffset, numBytes, Seed)
      case o: Any         => hasher32.hash(asNewByteArray, 0, numBytes, Seed)
    }
  }

  // Also, for why using lazy val is not that bad: https://dzone.com/articles/cost-laziness
  // The cost of computing the hash (and say using it in a bloom filter) is much higher.
  lazy val cachedHash64: Long = {
    base match {
      case a: Array[Byte] => hasher64.hash(a, offset.toInt - UnsafeUtils.arayOffset, numBytes, Seed)
      case o: Any         => hasher64.hash(asNewByteArray, 0, numBytes, Seed)
    }
  }

  override def hashCode: Int = cachedHash32
}

object ZeroCopyBinary {
  val xxhashFactory = XXHashFactory.fastestInstance
  val hasher32 = xxhashFactory.hash32
  val hasher64 = xxhashFactory.hash64
  val Seed = 0x9747b28c
}

/**
 * A zero-copy UTF8 string class
 * Not intended for general purpose use, mostly for fast comparisons and sorts without the need to
 * deserialize to a regular Java string
 */
final class ZeroCopyUTF8String(val base: Any, val offset: Long, val numBytes: Int)
extends ZeroCopyBinary {
  import ZeroCopyUTF8String._

  final def asNewString: String = new String(asNewByteArray)
  override def toString: String = asNewString

  final def numChars: Int = {
    var len = 0
    var i = 0
    while (i < numBytes) {
      len += 1
      i += numBytesForFirstByte(getByte(i))
    }
    len
  }

  /**
   * Returns a substring of this.  The returned string does not have bytes copied; simply different
   * pointers to the same area of memory.  This is possible because ZCB's are immutable.
   * @param start the position of first code point
   * @param until the position after last code point, exclusive.
   */
  final def substring(start: Int, until: Int): ZeroCopyUTF8String = {
    if (until <= start || start >= numBytes) {
      empty
    } else {
      var i = 0
      var c = 0
      while (i < numBytes && c < start) {
        i += numBytesForFirstByte(getByte(i))
        c += 1
      }

      val j = i
      while (i < numBytes && c < until) {
        i += numBytesForFirstByte(getByte(i))
        c += 1
      }

      if (i > j) new ZeroCopyUTF8String(base, offset + j, i - j) else empty
    }
  }

  private def matchAt(s: ZeroCopyUTF8String, pos: Int): Boolean =
    if (s.numBytes + pos > numBytes || pos < 0) { false }
    else { UnsafeUtils.equate(base, offset + pos, s.base, s.offset, s.numBytes) }

  final def startsWith(prefix: ZeroCopyUTF8String): Boolean = matchAt(prefix, 0)

  final def endsWith(suffix: ZeroCopyUTF8String): Boolean = matchAt(suffix, numBytes - suffix.numBytes)

  final def contains(substring: ZeroCopyUTF8String): Boolean =
    if (substring.numBytes == 0) { true }
    else {
      val firstByte = substring.getByte(0)
      for { i <- 0 to (numBytes - substring.numBytes) optimized } {
        if (getByte(i) == firstByte && matchAt(substring, i)) return true
      }
      false
    }
}

object ZeroCopyUTF8String {
  def apply(bytes: Array[Byte]): ZeroCopyUTF8String =
    new ZeroCopyUTF8String(bytes, UnsafeUtils.arayOffset, bytes.size)

  def apply(bytes: Array[Byte], offset: Int, len: Int): ZeroCopyUTF8String = {
    require(offset + len <= bytes.size, s"offset + len ($offset + $len) exceeds size ${bytes.size}")
    new ZeroCopyUTF8String(bytes, UnsafeUtils.arayOffset + offset, len)
  }

  def apply(str: String): ZeroCopyUTF8String = apply(str.getBytes("UTF-8"))

  val empty = ZeroCopyUTF8String("")

  val bytesOfCodePointInUTF8 = Array(2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
    4, 4, 4, 4, 4, 4, 4, 4,
    5, 5, 5, 5,
    6, 6)

  /**
   * Returns the number of bytes for a code point with the first byte as `b`
   */
  def numBytesForFirstByte(b: Byte): Int = {
    val offset = (b & 0xFF) - 192;
    if (offset >= 0) bytesOfCodePointInUTF8(offset) else 1
  }

  implicit object ZeroCopyUTF8BinaryOrdering extends Ordering[ZeroCopyUTF8String] {
    def compare(a: ZeroCopyUTF8String, b: ZeroCopyUTF8String): Int = a.compare(b)
  }
}