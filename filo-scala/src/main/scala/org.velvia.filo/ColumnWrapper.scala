package org.velvia.filo

import com.google.flatbuffers.Table
import java.nio.ByteBuffer
import org.velvia.filo.column._
import scala.collection.Traversable
import scala.util.Try

/**
 * A ColumnWrapper gives collection API semantics around the binary Filo format vector.
 */
trait ColumnWrapper[@specialized(Int, Double, Long, Short) A] extends Traversable[A] {
  // Returns true if the element at position index is available, false if NA
  def isAvailable(index: Int): Boolean

  // Calls fn for each available element in the column.  Will call 0 times if column is empty.
  def foreach[B](fn: A => B): Unit

  /**
   * Returns the element at a given index.  If the element is not available, the value returned
   * is undefined.  This is a very low level function intended for speed, not safety.
   * @param index the index in the column to pull from.  No bounds checking is done.
   */
  def apply(index: Int): A

  /**
   * Returns the number of elements in the column.
   */
  def length: Int

  /**
   * A "safe" but slower get-element-at-position method.
   * @param index the index in the column to get
   * @return Some(a) if index is within bounds and element is not missing
   */
  def get(index: Int): Option[A] =
    if (index >= 0 && index < length && isAvailable(index)) { Some(apply(index)) }
    else                                                    { None }

  /**
   * Returns an Iterator[Option[A]] over the Filo bytebuffer.  This basically calls
   * get() at each index, so it returns Some(A) when the value is defined and None
   * if it is NA.
   */
  def optionIterator(): Iterator[Option[A]] =
    for { index <- (0 until length).toIterator } yield { get(index) }
}

class EmptyColumnWrapper[A] extends ColumnWrapper[A] {
  final def isAvailable(index: Int): Boolean = false
  final def foreach[B](fn: A => B): Unit = {}
  final def apply(index: Int): A = throw new ArrayIndexOutOfBoundsException
  final def length: Int = 0
}

// TODO: separate this out into traits for AllZeroes vs mixed ones for speed, no need to do
// if sc.naMask.maskType lookup every time
// TODO: wrap bitMask with FastBufferReader for speed
trait NaMaskAvailable {
  val naMask: NaMask
  lazy val maskLen = naMask.bitMaskLength()
  // could be much more optimized, obviously
  final def isAvailable(index: Int): Boolean = {
    if (naMask.maskType == MaskType.AllZeroes) {
      true
    } else {
      // NOTE: length of bitMask may be less than (length / 64) longwords.
      val maskIndex = index >> 5
      val maskVal = if (maskIndex < maskLen) naMask.bitMask(maskIndex) else 0L
      (maskVal & (1 << (index & 63))) == 0
    }
  }
}

abstract class SimpleColumnWrapper[A](sc: SimpleColumn, vector: Table)
    extends ColumnWrapper[A] with NaMaskAvailable {
  val naMask = sc.naMask

  final def length: Int = VectorUtils.getLength(vector)

  final def foreach[B](fn: A => B): Unit = {
    for { i <- 0 until length } { if (isAvailable(i)) fn(apply(i)) }
  }
}

object DictStringColumnWrapper {
  // Used to represent no string value or NA.  Better than using null.
  val NoString = ""
}

abstract class DictStringColumnWrapper(val dsc: DictStringColumn, vector: Table)
    extends ColumnWrapper[String] with NaMaskAvailable {
  import DictStringColumnWrapper._

  // To be mixed in depending on type of code vector
  def getCode(index: Int): Int

  val naMask = dsc.naMask

  // Cache the Strings so we only pay cost of deserializing each unique string once
  val strCache = Array.fill(dsc.dictionaryLength())(NoString)

  final private def dictString(code: Int): String = {
    if (strCache(code) == NoString) strCache(code) = dsc.dictionary(code)
    strCache(code)
  }

  final def apply(index: Int): String = dictString(getCode(index))

  final def length: Int = VectorUtils.getLength(vector)

  final def foreach[B](fn: String => B): Unit = {
    for { i <- 0 until length } { if (isAvailable(i)) fn(apply(i)) }
  }
}