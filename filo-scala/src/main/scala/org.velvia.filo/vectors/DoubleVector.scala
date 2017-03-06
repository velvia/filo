package org.velvia.filo.vectors

import java.nio.ByteBuffer

import org.velvia.filo._

object DoubleVector {
  def fromIntBuf(buf: ByteBuffer): BinaryVector[Double] =
    new DoubleIntWrapper(IntBinaryVector(buf))
  def fromMaskedIntBuf(buf: ByteBuffer): BinaryVector[Double] =
    new DoubleIntWrapper(IntBinaryVector.masked(buf))
}

class DoubleIntWrapper(inner: BinaryVector[Int]) extends BinaryVector[Double] {
  val base = inner.base
  val offset = inner.offset
  val numBytes = inner.numBytes

  final def apply(i: Int): Double = inner(i).toDouble
  final def isAvailable(i: Int): Boolean = inner.isAvailable(i)
  override final def length: Int = inner.length
}
