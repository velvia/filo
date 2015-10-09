package org.velvia.filo

import com.google.flatbuffers.FlatBufferBuilder
import java.nio.ByteBuffer
import scala.collection.mutable.BitSet
import scala.reflect.ClassTag

import RowReader._

/**
 * A bunch of builders for row-oriented ingestion to create columns in parallel
 * Use these for support of missing/NA values.
 */
sealed trait ColumnBuilderBase {
  type T

  /** Add a Not Available (null) element to the builder. */
  def addNA(): Unit

  /** Add a value of type T to the builder.  It will be marked as available. */
  def addData(value: T): Unit

  /** If value is defined, then use addData, otherwise use addNA */
  def addOption(value: Option[T]): Unit = {
    value.foreach { v => addData(v) }
    value.orElse  { addNA(); None }
  }

  implicit val extractor: TypedFieldExtractor[T]

  /** Adds an element from a RowReader */
  final def add(row: RowReader, colNo: Int): Unit = {
    if (row.notNull(colNo)) { addData(extractor.getField(row, colNo)) }
    else                    { addNA() }
  }

  /** Resets the builder state to build a new column */
  def reset(): Unit

  /** Number of elements added so far */
  def length: Int

  /** Returns true if every element added is NA, or no elements have been added */
  def isAllNA: Boolean

  implicit val builder: BuilderEncoder[T]

  /**
   * Produces a binary Filo vector as a ByteBuffer, using default encoding hints
   */
  def toFiloBuffer(): ByteBuffer = toFiloBuffer(BuilderEncoder.AutoDetect)

  /**
   * Produces a binary Filo vector as a ByteBuffer, with a specific encoding hint
   */
  def toFiloBuffer(hint: BuilderEncoder.EncodingHint): ByteBuffer = builder.encode(this, hint)
}

/**
 * A concrete implementation of ColumnBuilderBase based on ArrayBuffer and BitSet for a mask
 * @param empty The empty value to insert for an NA or missing value
 */
sealed abstract class ColumnBuilder[A](empty: A) extends ColumnBuilderBase {
  type T = A

  // True for a row number (or bit is part of the set) if data for that row is not available
  val naMask = new BitSet
  val data = new collection.mutable.ArrayBuffer[A]

  def addNA(): Unit = {
    naMask += data.length
    data += empty
  }

  def addData(value: A): Unit = { data += value }

  def reset(): Unit = {
    naMask.clear
    data.clear
  }

  def length: Int = data.length
  def isAllNA: Boolean = Utils.isAllNA(naMask, data.length)
}

sealed abstract class TypedColumnBuilder[A](empty: A)
   (implicit val extractor: TypedFieldExtractor[A],
    implicit val builder: BuilderEncoder[A]) extends ColumnBuilder(empty)

sealed abstract class MinMaxColumnBuilder[A](minValue: A,
                                             maxValue: A,
                                             val zero: A)
                                            (implicit val ordering: Ordering[A],
                                             implicit val extractor: TypedFieldExtractor[A],
                                             implicit val builder: BuilderEncoder[A])
extends ColumnBuilder(zero) {
  var min: A = maxValue
  var max: A = minValue

  override def addData(value: A): Unit = {
    super.addData(value)
    if (ordering.compare(value, max) > 0) max = value
    if (ordering.compare(value, min) < 0) min = value
  }
}

object ColumnBuilder {
  /**
   * Creates a ColumnBuilder dynamically based on a passed in class.
   * Please add your builder here when you add a type
   */
  def apply[A](dataType: Class[_]): ColumnBuilder[A] = dataType match {
    case Classes.Int    => (new IntColumnBuilder).asInstanceOf[ColumnBuilder[A]]
    case Classes.Long   => (new LongColumnBuilder).asInstanceOf[ColumnBuilder[A]]
    case Classes.Double => (new DoubleColumnBuilder).asInstanceOf[ColumnBuilder[A]]
    case Classes.Float  => (new FloatColumnBuilder).asInstanceOf[ColumnBuilder[A]]
    case Classes.String => (new StringColumnBuilder).asInstanceOf[ColumnBuilder[A]]
  }

  import BuilderEncoder._

  /**
   * Builds a ColumnBuilder automatically from a scala collection.
   * All values will be marked available.
   */
  def apply[A: ClassTag: BuilderEncoder](seq: collection.Seq[A]): ColumnBuilderBase = {
    val builder = apply[A](implicitly[ClassTag[A]].runtimeClass)
    seq.foreach(builder.addData)
    builder
  }

  /**
   * Encodes a sequence of type Option[A] to a Filo format ByteBuffer.
   * Elements which are None will get encoded as NA bits.
   */
  def fromOptions[A: ClassTag: BuilderEncoder](seq: collection.Seq[Option[A]]): ColumnBuilderBase = {
    val builder = apply[A](implicitly[ClassTag[A]].runtimeClass)
    seq.foreach(builder.addOption)
    builder
  }
}

class IntColumnBuilder extends MinMaxColumnBuilder(Int.MinValue, Int.MaxValue, 0)
class LongColumnBuilder extends MinMaxColumnBuilder(Long.MinValue, Long.MaxValue, 0L)
class DoubleColumnBuilder extends MinMaxColumnBuilder(Double.MinValue, Double.MaxValue, 0.0)
class FloatColumnBuilder extends MinMaxColumnBuilder(Float.MinValue, Float.MaxValue, 0.0F)

class StringColumnBuilder extends TypedColumnBuilder("") {
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
