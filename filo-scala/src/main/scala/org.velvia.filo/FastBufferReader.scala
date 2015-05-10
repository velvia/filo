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
}

import FastBufferReader._

class FastBufferReader(buf: ByteBuffer) {
  val offset = arayOffset + buf.arrayOffset
  val byteArray = buf.array()
  final def readByte(i: Int): Byte = unsafe.getByte(byteArray, (offset + i).toLong)
  final def readShort(i: Int): Short = unsafe.getShort(byteArray, (offset + i * 2).toLong)
  final def readInt(i: Int): Int = unsafe.getInt(byteArray, (offset + i * 4).toLong)
  final def readLong(i: Int): Long = unsafe.getLong(byteArray, (offset + i * 8).toLong)
  final def readDouble(i: Int): Double = unsafe.getDouble(byteArray, (offset + i * 8).toLong)
}
