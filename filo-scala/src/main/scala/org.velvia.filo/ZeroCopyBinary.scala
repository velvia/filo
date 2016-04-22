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
    for { i <- 0 until minLen optimized } {
      val res = (UnsafeUtils.getByte(base, offset + i) & 0xFF) -
                (UnsafeUtils.getByte(other.base, other.offset + i) & 0xFF)
      if (res != 0) return res
    }
    return numBytes - other.numBytes
  }

  def copyTo(dest: Any, destOffset: Long): Unit =
    UnsafeUtils.unsafe.copyMemory(base, offset, dest, destOffset, numBytes)

  def asNewByteArray: Array[Byte] = {
    val newArray = new Array[Byte](numBytes)
    copyTo(newArray, UnsafeUtils.arayOffset)
    newArray
  }

  override def equals(other: Any): Boolean = other match {
    case z: ZeroCopyBinary => compare(z) == 0
  }

  // Ideally, hash without copying to another byte array, esp if the base storage is a byte array already
  private def computeHash32(): Int = {
    base match {
      case a: Array[Byte] => hasher32.hash(a, offset.toInt - UnsafeUtils.arayOffset, numBytes, Seed)
      case o: Any         => hasher32.hash(asNewByteArray, 0, numBytes, Seed)
    }
  }

  // TODO(velvia): cache the hashcode if it really helps performance
  override def hashCode: Int = computeHash32()
}

object ZeroCopyBinary {
  val xxhashFactory = XXHashFactory.fastestInstance
  val hasher32 = xxhashFactory.hash32
  val Seed = 0x9747b28c
}

/**
 * A zero-copy UTF8 string class
 * Not intended for general purpose use, mostly for fast comparisons and sorts without the need to
 * deserialize to a regular Java string
 */
final class ZeroCopyUTF8String(val base: Any, val offset: Long, val numBytes: Int)
extends ZeroCopyBinary {
  def asNewString: String = new String(asNewByteArray)
}

object ZeroCopyUTF8String {
  def apply(bytes: Array[Byte]): ZeroCopyUTF8String =
    new ZeroCopyUTF8String(bytes, UnsafeUtils.arayOffset, bytes.size)

  def apply(bytes: Array[Byte], offset: Int, len: Int): ZeroCopyUTF8String = {
    require(offset + len <= bytes.size, s"offset + len ($offset + $len) exceeds size ${bytes.size}")
    new ZeroCopyUTF8String(bytes, UnsafeUtils.arayOffset + offset, len)
  }

  def apply(str: String): ZeroCopyUTF8String = apply(str.getBytes("UTF-8"))

  implicit object ZeroCopyUTF8BinaryOrdering extends Ordering[ZeroCopyUTF8String] {
    def compare(a: ZeroCopyUTF8String, b: ZeroCopyUTF8String): Int = a.compare(b)
  }
}