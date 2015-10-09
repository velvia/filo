package org.velvia.filo

import com.google.flatbuffers.FlatBufferBuilder
import java.nio.ByteBuffer
import scala.collection.mutable.BitSet

/**
 * A bunch of builders for row-oriented ingestion to create columns in parallel
 * Use these for support of missing/NA values
 * @param empty The empty value to insert for an NA or missing value
 */
sealed abstract class ColumnBuilder[A](empty: A) {
  // True for a row number (or bit is part of the set) if data for that row is not available
  val naMask = new BitSet
  val data = new collection.mutable.ArrayBuffer[A]

  def addNA(): Unit = {
    naMask += data.length
    data += empty
  }

  def addData(value: A): Unit = { data += value }

  def addOption(value: Option[A]): Unit = {
    value.foreach { v => addData(v) }
    value.orElse  { addNA(); None }
  }

  final def add(row: RowReader, colNo: Int): Unit = {
    if (row.notNull(colNo)) { addData(fromReader(row, colNo)) }
    else                    { addNA() }
  }

  def fromReader(row: RowReader, colNo: Int): A

  def reset(): Unit = {
    naMask.clear
    data.clear
  }
}

sealed abstract class MinMaxColumnBuilder[A: Ordering](minValue: A,
                                                       maxValue: A,
                                                       val zero: A) extends ColumnBuilder(zero) {
  val ordering = implicitly[Ordering[A]]

  var min: A = maxValue
  var max: A = minValue

  override def addData(value: A): Unit = {
    super.addData(value)
    if (ordering.compare(value, max) > 0) max = value
    if (ordering.compare(value, min) < 0) min = value
  }
}

// Please add your builder here when you add a type
object ColumnBuilder {
  def apply(dataType: Class[_]): ColumnBuilder[_] = dataType match {
    case Classes.Int    => new IntColumnBuilder
    case Classes.Long   => new LongColumnBuilder
    case Classes.Double => new DoubleColumnBuilder
    case Classes.String => new StringColumnBuilder
  }
}

class IntColumnBuilder extends MinMaxColumnBuilder(Int.MinValue, Int.MaxValue, 0) {
  final def fromReader(row: RowReader, colNo: Int): Int = row.getInt(colNo)
}

class LongColumnBuilder extends MinMaxColumnBuilder(Long.MinValue, Long.MaxValue, 0L) {
  final def fromReader(row: RowReader, colNo: Int): Long = row.getLong(colNo)
}

class DoubleColumnBuilder extends MinMaxColumnBuilder(Double.MinValue, Double.MaxValue, 0.0) {
  final def fromReader(row: RowReader, colNo: Int): Double = row.getDouble(colNo)
}

class FloatColumnBuilder extends MinMaxColumnBuilder(Float.MinValue, Float.MaxValue, 0.0F) {
  final def fromReader(row: RowReader, colNo: Int): Float = row.getFloat(colNo)
}

class StringColumnBuilder extends ColumnBuilder("") {
  // For dictionary encoding. NOTE: this set does NOT include empty value
  val stringSet = new collection.mutable.HashSet[String]

  final def fromReader(row: RowReader, colNo: Int): String = row.getString(colNo)

  override def addData(value: String): Unit = {
    stringSet += value
    super.addData(value)
  }

  override def reset(): Unit = {
    stringSet.clear
    super.reset()
  }
}
