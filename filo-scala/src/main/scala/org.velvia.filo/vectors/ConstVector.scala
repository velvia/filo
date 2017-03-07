package org.velvia.filo.vectors

import org.velvia.filo._

object ConstVector {
  /**
   * Allocates and returns bytes for a ConstVector.
   * @param len the logical length or # of repeats
   * @param neededBytes the bytes needed for one element
   * @param fillBytes a function to fill out bytes at given base and offset
   * @return the (base, offset, numBytes) for the ConstVector
   */
  def make(len: Int, neededBytes: Int)(fillBytes: (Any, Long) => Unit): (Any, Long, Int) = {
    val (base, off, nBytes) = BinaryVector.allocWithMagicHeader(4 + neededBytes)
    UnsafeUtils.setInt(base, off, len)
    fillBytes(base, off + 4)
    (base, off, nBytes)
  }
}

/**
 * A vector which holds the value of one element repeated n times.
 * Note that this is actually an AppendableVector, needed for some APIs.
 */
abstract class ConstVector[A](val base: Any, val offset: Long, val numBytes: Int) extends
BinaryAppendableVector[A] {
  val maxBytes = numBytes
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType   = WireFormat.SUBTYPE_REPEATED

  override val length = UnsafeUtils.getInt(base, offset)
  protected val dataOffset = offset + 4
  final def isAvailable(i: Int): Boolean = true
  def addData(value: A): Unit = ???
  def addNA(): Unit = ???
  def isAllNA: Boolean = ???
  def noNAs: Boolean = ???
}