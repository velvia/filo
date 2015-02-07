package org.velvia.filo

import java.nio.ByteBuffer
import org.velvia.filo.column._
import scala.collection.Traversable
import scala.util.Try

object ColumnParser {
  def parseAsSimpleColumn[A](buf: ByteBuffer)(implicit veb: VectorExtractor[A]): ColumnWrapper[A] = {
    val column = Column.getRootAsColumn(buf)
    require(column.colType == AnyColumn.SimpleColumn,
            "Not a SimpleColumn, but a " + AnyColumn.name(column.colType))
    val sc = new SimpleColumn
    column.col(sc)
    if (sc.naMask.maskType == MaskType.AllOnes) {
      new EmptyColumnWrapper[A]
    } else {
      new SimpleColumnWrapper[A](sc)
    }
  }
}

// TODO: implement Framian's Column API instead
/**
 * A ColumnWrapper gives collection API semantics around the binary Filo format vector.
 */
trait ColumnWrapper[A] extends Traversable[A] {
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
}

class EmptyColumnWrapper[A] extends ColumnWrapper[A] {
  final def isAvailable(index: Int): Boolean = false
  final def foreach[B](fn: A => B): Unit = {}
  final def apply(index: Int): A = throw new ArrayIndexOutOfBoundsException
  final def length: Int = 0
}

class SimpleColumnWrapper[A](sc: SimpleColumn)(implicit veb: VectorExtractor[A])
    extends ColumnWrapper[A] {
  val vector = VectorUtils.getVectorFromType(sc.vectorType)
  sc.vector(vector)
  val atIndex = veb.getExtractor(vector)

  final def length: Int = VectorUtils.getLength(vector)

  final def apply(index: Int): A = atIndex(index)

  // could be much more optimized, obviously
  final def isAvailable(index: Int): Boolean = {
    if (sc.naMask.maskType == MaskType.AllZeroes) {
      true
    } else {
      // NOTE: length of bitMask may be less than (length / 64) longwords.
      (Try(sc.naMask.bitMask(index >> 5)).getOrElse(0L) & (1 << (index & 63))) == 0
    }
  }

  final def foreach[B](fn: A => B): Unit = {
    for { i <- 0 until length } { if (isAvailable(i)) fn(atIndex(i)) }
  }
}