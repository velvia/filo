package org.velvia.filo

import sun.misc.Unsafe
import java.nio.ByteBuffer

/**
 * TODO: use something like Agrona so the classes below will work with non-array-based ByteBuffers
 */
object FastBufferReader {
  val field = classOf[Unsafe].getDeclaredField("theUnsafe")
  field.setAccessible(true)
  val unsafe = field.get(null).asInstanceOf[Unsafe]

  val arayOffset = unsafe.arrayBaseOffset(classOf[Array[Byte]])
}

import FastBufferReader._

class FastByteBufferReader(buf: ByteBuffer) {
  val offset = buf.arrayOffset
  val byteArray = buf.array()
  final def read(i: Int): Byte = unsafe.getByte(byteArray, arayOffset + (offset + i).toLong)
}

class FastShortBufferReader(buf: ByteBuffer) {
  val offset = buf.arrayOffset
  val byteArray = buf.array()
  final def read(i: Int): Short = unsafe.getShort(byteArray, arayOffset + (offset + i * 2).toLong)
}

class FastIntBufferReader(buf: ByteBuffer) {
  val offset = buf.arrayOffset
  val byteArray = buf.array()
  final def read(i: Int): Int = unsafe.getInt(byteArray, arayOffset + (offset + i * 4).toLong)
}