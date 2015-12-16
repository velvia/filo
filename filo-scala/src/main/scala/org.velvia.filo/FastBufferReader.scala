package org.velvia.filo

import sun.misc.Unsafe
import java.nio.ByteBuffer

/**
 * TODO: use something like Agrona so the classes below will work with non-array-based ByteBuffers
 * Fast (machine-speed/intrinsic) readers for ByteBuffer values, assuming bytebuffers are vectors
 * of fixed size.
 */
object FastBufferReader {
  val field = classOf[Unsafe].getDeclaredField("theUnsafe")
  field.setAccessible(true)
  val unsafe = field.get(null).asInstanceOf[Unsafe]

  val arayOffset = unsafe.arrayBaseOffset(classOf[Array[Byte]])

  /**
   * Instantiates the correct BufferReader implementation:
   * - FastUnsafeArrayBufferReader is used if the ByteBuffer is backed by an array
   * - SlowBufferReader just uses ByteBuffer
   *
   * This allows future-proof implementations: for example for JDK9 / better Unsafe changes, and
   * for extending to DirectMemory allocated ByteBuffers, for example.
   *
   * @param buf the ByteBuffer containing an array of fixed values to wrap for fast access
   */
  def apply(buf: ByteBuffer): FastBufferReader = {
    if (buf.hasArray) { new FastUnsafeArrayBufferReader(buf) }
    else              { throw new RuntimeException("Cannot support this ByteBuffer") }
  }

  def apply(long: Long): FastBufferReader = new FastLongBufferReader(long)
}

trait FastBufferReader {
  def readByte(i: Int): Byte
  def readShort(i: Int): Short
  def readInt(i: Int): Int
  def readLong(i: Int): Long
  def readDouble(i: Int): Double
  def readFloat(i: Int): Float
}

import FastBufferReader._

class FastUnsafeArrayBufferReader(buf: ByteBuffer) extends FastBufferReader {
  val offset = arayOffset + buf.arrayOffset + buf.position
  val byteArray = buf.array()
  final def readByte(i: Int): Byte = unsafe.getByte(byteArray, (offset + i).toLong)
  final def readShort(i: Int): Short = unsafe.getShort(byteArray, (offset + i * 2).toLong)
  final def readInt(i: Int): Int = unsafe.getInt(byteArray, (offset + i * 4).toLong)
  final def readLong(i: Int): Long = unsafe.getLong(byteArray, (offset + i * 8).toLong)
  final def readDouble(i: Int): Double = unsafe.getDouble(byteArray, (offset + i * 8).toLong)
  final def readFloat(i: Int): Float = unsafe.getFloat(byteArray, (offset + i * 4).toLong)
}

class FastLongBufferReader(long: Long) extends FastBufferReader {
  def readByte(i: Int): Byte = (long >> (8 * i)).toByte
  def readShort(i: Int): Short = (long >> (16 * i)).toShort
  def readInt(i: Int): Int = (long >> (32 * i)).toInt
  def readLong(i: Int): Long = long
  def readDouble(i: Int): Double = ???
  def readFloat(i: Int): Float = ???
}
