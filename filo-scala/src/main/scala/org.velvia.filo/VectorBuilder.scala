package org.velvia.filo

import com.google.flatbuffers.FlatBufferBuilder
import java.nio.ByteBuffer
import java.sql.Timestamp
import org.joda.time.DateTime
import scala.collection.mutable.BitSet
import scala.reflect.ClassTag

import RowReader._

/**
 * A builder for FiloVectors.  Used to build up elements of a vector before freezing it as an
 * immutable, extremely fast for reading FiloVector.
 */
sealed trait VectorBuilderBase {
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
 * A concrete implementation of VectorBuilderBase based on ArrayBuffer and BitSet for a mask
 * @param empty The empty value to insert for an NA or missing value
 */
sealed abstract class VectorBuilder[A](empty: A) extends VectorBuilderBase {
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

sealed abstract class TypedVectorBuilder[A](empty: A)
   (implicit val extractor: TypedFieldExtractor[A],
    implicit val builder: BuilderEncoder[A]) extends VectorBuilder(empty)

sealed abstract class MinMaxVectorBuilder[A](minValue: A,
                                             maxValue: A,
                                             val zero: A)
                                            (implicit val ordering: Ordering[A],
                                             implicit val extractor: TypedFieldExtractor[A],
                                             implicit val builder: BuilderEncoder[A])
extends VectorBuilder(zero) {
  var min: A = maxValue
  var max: A = minValue

  override def addData(value: A): Unit = {
    super.addData(value)
    if (ordering.compare(value, max) > 0) max = value
    if (ordering.compare(value, min) < 0) min = value
  }
}

object VectorBuilder {
  type BuilderMap = Map[Class[_], () => VectorBuilderBase]
  /**
   * Creates a VectorBuilder dynamically based on a passed in class.
   * @param builderMap the map of classes to Builders.  Being able to pass one in
   *                   allows for customization.
   */
  def apply(dataType: Class[_],
            builderMap: BuilderMap = defaultBuilderMap): VectorBuilderBase =
    builderMap(dataType)()

  // Please add your builder here when you add a type
  val defaultBuilderMap = Map[Class[_], () => VectorBuilderBase](
    Classes.Boolean      -> (() => new BoolVectorBuilder),
    Classes.Int          -> (() => new IntVectorBuilder),
    Classes.Long         -> (() => new LongVectorBuilder),
    Classes.Double       -> (() => new DoubleVectorBuilder),
    Classes.Float        -> (() => new FloatVectorBuilder),
    Classes.String       -> (() => new StringVectorBuilder),
    Classes.DateTime     -> (() => new DateTimeVectorBuilder),
    Classes.SqlTimestamp -> (() => new SqlTimestampVectorBuilder)
  )

  import BuilderEncoder._

  val FifteenMinMillis = 15 * org.joda.time.DateTimeConstants.MILLIS_PER_MINUTE

  /**
   * Builds a VectorBuilder automatically from a scala collection.
   * All values will be marked available.
   */
  def apply[A: ClassTag: BuilderEncoder](seq: collection.Seq[A]): VectorBuilderBase = {
    val builder = apply(implicitly[ClassTag[A]].runtimeClass).asInstanceOf[VectorBuilderBase { type T = A}]
    seq.foreach(builder.addData)
    builder
  }

  /**
   * Encodes a sequence of type Option[A] to a Filo format ByteBuffer.
   * Elements which are None will get encoded as NA bits.
   */
  def fromOptions[A: ClassTag: BuilderEncoder](seq: collection.Seq[Option[A]]): VectorBuilderBase = {
    val builder = apply(implicitly[ClassTag[A]].runtimeClass).asInstanceOf[VectorBuilderBase { type T = A}]
    seq.foreach(builder.addOption)
    builder
  }
}

import DefaultValues._

class BoolVectorBuilder extends MinMaxVectorBuilder(false, true, DefaultBool)
class IntVectorBuilder extends MinMaxVectorBuilder(Int.MinValue, Int.MaxValue, DefaultInt)
class LongVectorBuilder extends MinMaxVectorBuilder(Long.MinValue, Long.MaxValue, DefaultLong)
class DoubleVectorBuilder extends MinMaxVectorBuilder(Double.MinValue, Double.MaxValue, DefaultDouble)
class FloatVectorBuilder extends MinMaxVectorBuilder(Float.MinValue, Float.MaxValue, DefaultFloat)

class StringVectorBuilder extends TypedVectorBuilder(DefaultString) {
  // For dictionary encoding. NOTE: this set does NOT include empty value
  val stringSet = new collection.mutable.HashSet[String]

  final def fromReader(row: RowReader, colNo: Int): String = row.getString(colNo)

  override def addData(value: String): Unit = {
    if (value == null) {
      addNA()
    } else {
      stringSet += value
      super.addData(value)
    }
  }

  override def reset(): Unit = {
    stringSet.clear
    super.reset()
  }
}

abstract class NestedVectorBuilder[A, I](val innerBuilder: VectorBuilderBase { type T = I })
                                        (implicit val extractor: TypedFieldExtractor[A],
                                         implicit val builder: BuilderEncoder[A]) extends VectorBuilderBase {
  type T = A

  def addNA(): Unit = innerBuilder.addNA()

  def reset(): Unit = innerBuilder.reset()

  def length: Int = innerBuilder.length
  def isAllNA: Boolean = innerBuilder.isAllNA
}

class DateTimeVectorBuilder extends NestedVectorBuilder[DateTime, Long](new LongVectorBuilder) {
  val millisBuilder = innerBuilder.asInstanceOf[LongVectorBuilder]
  val tzBuilder = new IntVectorBuilder

  def addData(value: DateTime): Unit = {
    millisBuilder.addData(value.getMillis)
    tzBuilder.addData(value.getZone.getOffset(0) / VectorBuilder.FifteenMinMillis)
  }

  override def addNA(): Unit = {
    millisBuilder.addNA()
    tzBuilder.addNA()
  }

  override def reset(): Unit = {
    millisBuilder.reset()
    tzBuilder.reset()
  }
}

class SqlTimestampVectorBuilder extends NestedVectorBuilder[Timestamp, Long](new LongVectorBuilder) {
  val millisBuilder = innerBuilder.asInstanceOf[LongVectorBuilder]
  def addData(value: Timestamp): Unit = innerBuilder.addData(value.getTime)
}
