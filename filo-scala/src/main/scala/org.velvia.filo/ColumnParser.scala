package org.velvia.filo

import com.google.flatbuffers.Table
import java.nio.ByteBuffer
import org.velvia.filo.column._
import scala.collection.Traversable
import scala.util.Try

trait ColumnMaker[A] {
  def makeColumn(buf: ByteBuffer): ColumnWrapper[A]
}

trait SimpleColumnMaker[A] extends ColumnMaker[A] {
  def makeSimpleCol(sc: SimpleColumn, vector: Table): ColumnWrapper[A]
  def makeColumn(buf: ByteBuffer): ColumnWrapper[A] = {
    val column = Column.getRootAsColumn(buf)
    require(column.colType == AnyColumn.SimpleColumn,
            "Not a SimpleColumn, but a " + AnyColumn.name(column.colType))
    val sc = new SimpleColumn
    column.col(sc)
    if (sc.naMask.maskType == MaskType.AllOnes) {
      new EmptyColumnWrapper[A]
    } else {
      val vector = VectorUtils.getVectorFromType(sc.vectorType)
      sc.vector(vector)
      makeSimpleCol(sc, vector)
    }
  }
}

object ColumnParser {
  def parseAsSimpleColumn[A](buf: ByteBuffer)(implicit cm: ColumnMaker[A]): ColumnWrapper[A] = {
    cm.makeColumn(buf)
  }

  implicit object IntSimpleColumnMaker extends SimpleColumnMaker[Int] {
    def makeSimpleCol(sc: SimpleColumn, vector: Table): ColumnWrapper[Int] = vector match {
      case v: IntVector =>
        new SimpleColumnWrapper[Int](sc, vector) {
          val reader = new FastBufferReader(v.dataAsByteBuffer())
          final def apply(i: Int): Int = reader.readInt(i)
        }
      case v: ShortVector =>
        new SimpleColumnWrapper[Int](sc, vector) {
          val reader = new FastBufferReader(v.dataAsByteBuffer())
          final def apply(i: Int): Int = reader.readShort(i).toInt
        }
      case v: ByteVector if v.dataType == ByteDataType.TByte =>
        new SimpleColumnWrapper[Int](sc, vector) {
          val reader = new FastBufferReader(v.dataAsByteBuffer())
          final def apply(i: Int): Int = reader.readByte(i).toInt
        }
    }
  }

  implicit object LongSimpleColumnMaker extends SimpleColumnMaker[Long] {
    def makeSimpleCol(sc: SimpleColumn, vector: Table): ColumnWrapper[Long] = vector match {
      case v: LongVector =>
        new SimpleColumnWrapper[Long](sc, vector) {
          val reader = new FastBufferReader(v.dataAsByteBuffer())
          final def apply(i: Int): Long = reader.readLong(i)
        }
    }
  }

  implicit object DoubleSimpleColumnMaker extends SimpleColumnMaker[Double] {
    def makeSimpleCol(sc: SimpleColumn, vector: Table): ColumnWrapper[Double] = vector match {
      case v: DoubleVector =>
        new SimpleColumnWrapper[Double](sc, vector) {
          val reader = new FastBufferReader(v.dataAsByteBuffer())
          final def apply(i: Int): Double = reader.readDouble(i)
        }
    }
  }

  implicit object StringSimpleColumnMaker extends SimpleColumnMaker[String] {
    def makeSimpleCol(sc: SimpleColumn, vector: Table): ColumnWrapper[String] = vector match {
      case v: StringVector =>
        new SimpleColumnWrapper[String](sc, vector) {
          final def apply(i: Int): String = v.data(i)
        }
    }
  }
}

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

abstract class SimpleColumnWrapper[A](sc: SimpleColumn, vector: Table)
    extends ColumnWrapper[A] {

  final def length: Int = VectorUtils.getLength(vector)

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
    for { i <- 0 until length } { if (isAvailable(i)) fn(apply(i)) }
  }
}