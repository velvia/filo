package org.velvia.filo

import java.nio.ByteBuffer
import org.velvia.filo.column._
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
trait ColumnWrapper[A] {
  // Returns true if the element at position index is available, false if NA
  def isAvailable(index: Int): Boolean

  // Calls fn for each available element in the column.  Will call 0 times if column is empty.
  def foreach[B](fn: A => B)
}

class EmptyColumnWrapper[A] extends ColumnWrapper[A] {
  def isAvailable(index: Int): Boolean = false
  def foreach[B](fn: A => B) {}
}

class SimpleColumnWrapper[A](sc: SimpleColumn)(implicit veb: VectorExtractor[A])
    extends ColumnWrapper[A] {
  val atIndex = veb.getExtractor(sc.vectorType)
  val vector = VectorUtils.getVectorFromType(sc.vectorType)
  sc.vector(vector)
  val length = VectorUtils.getLength(vector, sc.vectorType)

  // could be much more optimized, obviously
  final def isAvailable(index: Int): Boolean = {
    if (sc.naMask.maskType == MaskType.AllZeroes) {
      true
    } else {
      // NOTE: length of bitMask may be less than (length / 64) longwords.
      (Try(sc.naMask.bitMask(index >> 64)).getOrElse(0L) & (1 << (index & 63))) == 0
    }
  }

  final def foreach[B](fn: A => B) {
    for (i <- 0 until length) { if (isAvailable(i)) fn(atIndex(vector, i)) }
  }
}