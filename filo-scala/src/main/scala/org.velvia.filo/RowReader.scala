package org.velvia.filo

import java.nio.ByteBuffer
import java.sql.Timestamp
import org.joda.time.DateTime
import scala.reflect.ClassTag

/**
 * A generic trait for reading typed values out of a row of data.
 * Used for both reading out of Filo vectors as well as for RowToVectorBuilder,
 * which means it can be used to compose heterogeneous Filo vectors together.
 */
trait RowReader {
  def notNull(columnNo: Int): Boolean
  def getBoolean(columnNo: Int): Boolean
  def getInt(columnNo: Int): Int
  def getLong(columnNo: Int): Long
  def getDouble(columnNo: Int): Double
  def getFloat(columnNo: Int): Float
  def getString(columnNo: Int): String
  def getAny(columnNo: Int): Any

  // Please override final def if your RowReader has a faster implementation
  def getUTF8String(columnNo: Int): ZeroCopyUTF8String = ZeroCopyUTF8String(getString(columnNo))

  /**
   * This method serves two purposes.
   * For RowReaders that need to parse from some input source, such as CSV,
   * the ClassTag gives a way for per-type parsing for non-primitive types.
   * For RowReaders for fast reading paths, such as Spark, the default
   * implementation serves as a fast way to read from objects.
   */
  def as[T: ClassTag](columnNo: Int): T = getAny(columnNo).asInstanceOf[T]
}

/**
 * An example of a RowReader that can read from Scala tuples containing Option[_]
 */
case class TupleRowReader(tuple: Product) extends RowReader {
  def notNull(columnNo: Int): Boolean =
    tuple.productElement(columnNo).asInstanceOf[Option[Any]].nonEmpty

  def getBoolean(columnNo: Int): Boolean = tuple.productElement(columnNo) match {
    case Some(x: Boolean) => x
  }

  def getInt(columnNo: Int): Int = tuple.productElement(columnNo) match {
    case Some(x: Int) => x
  }

  def getLong(columnNo: Int): Long = tuple.productElement(columnNo) match {
    case Some(x: Long) => x
  }

  def getDouble(columnNo: Int): Double = tuple.productElement(columnNo) match {
    case Some(x: Double) => x
  }

  def getFloat(columnNo: Int): Float = tuple.productElement(columnNo) match {
    case Some(x: Float) => x
  }

  def getString(columnNo: Int): String = tuple.productElement(columnNo) match {
    case Some(x: String) => x
  }

  def getAny(columnNo: Int): Any =
    tuple.productElement(columnNo).asInstanceOf[Option[Any]].getOrElse(null)
}

/**
 * A RowReader for working with OpenCSV or anything else that emits string[]
 */
case class ArrayStringRowReader(strings: Array[String]) extends RowReader {
  //scalastyle:off
  def notNull(columnNo: Int): Boolean = strings(columnNo) != null && strings(columnNo) != ""
  //scalastyle:on
  def getBoolean(columnNo: Int): Boolean = strings(columnNo).toBoolean
  def getInt(columnNo: Int): Int = strings(columnNo).toInt
  def getLong(columnNo: Int): Long = strings(columnNo).toLong
  def getDouble(columnNo: Int): Double = strings(columnNo).toDouble
  def getFloat(columnNo: Int): Float = strings(columnNo).toFloat
  def getString(columnNo: Int): String = strings(columnNo)
  def getAny(columnNo: Int): Any = strings(columnNo)

  override def as[T: ClassTag](columnNo: Int): T = {
    (implicitly[ClassTag[T]].runtimeClass match {
      case Classes.DateTime => new DateTime(strings(columnNo))
      case Classes.SqlTimestamp => new Timestamp(DateTime.parse(strings(columnNo)).getMillis)
    }).asInstanceOf[T]
  }
}

/**
 * A RowReader that changes the column numbers around of an original RowReader.  It could be used to
 * present a subset of the original columns, for example.
 * @param columnRoutes an array of original column numbers for the column in question.  For example:
 *                     Array(0, 2, 5) means an getInt(1) would map to a getInt(2) for the original RowReader
 */
case class RoutingRowReader(origReader: RowReader, columnRoutes: Array[Int]) extends RowReader {
  def notNull(columnNo: Int): Boolean    = origReader.notNull(columnRoutes(columnNo))
  def getBoolean(columnNo: Int): Boolean = origReader.getBoolean(columnRoutes(columnNo))
  def getInt(columnNo: Int): Int         = origReader.getInt(columnRoutes(columnNo))
  def getLong(columnNo: Int): Long       = origReader.getLong(columnRoutes(columnNo))
  def getDouble(columnNo: Int): Double   = origReader.getDouble(columnRoutes(columnNo))
  def getFloat(columnNo: Int): Float     = origReader.getFloat(columnRoutes(columnNo))
  def getString(columnNo: Int): String   = origReader.getString(columnRoutes(columnNo))
  def getAny(columnNo: Int): Any         = origReader.getAny(columnRoutes(columnNo))

  override def equals(other: Any): Boolean = other match {
    case RoutingRowReader(orig, _) => orig.equals(origReader)
    case r: RowReader              => r.equals(origReader)
    case other: Any                => false
  }
}

object RowReader {
  // Type class for extracting a field of a specific type .. and comparing a field from two RowReaders
  trait TypedFieldExtractor[@specialized F] {
    def getField(reader: RowReader, columnNo: Int): F
    def compare(reader: RowReader, other: RowReader, columnNo: Int): Int
  }

  implicit object BooleanFieldExtractor extends TypedFieldExtractor[Boolean] {
    final def getField(reader: RowReader, columnNo: Int): Boolean = reader.getBoolean(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      java.lang.Boolean.compare(getField(reader, columnNo), getField(other, columnNo))
  }

  implicit object LongFieldExtractor extends TypedFieldExtractor[Long] {
    final def getField(reader: RowReader, columnNo: Int): Long = reader.getLong(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      java.lang.Long.compare(getField(reader, columnNo), getField(other, columnNo))
  }

  implicit object IntFieldExtractor extends TypedFieldExtractor[Int] {
    final def getField(reader: RowReader, columnNo: Int): Int = reader.getInt(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      java.lang.Integer.compare(getField(reader, columnNo), getField(other, columnNo))
  }

  implicit object DoubleFieldExtractor extends TypedFieldExtractor[Double] {
    final def getField(reader: RowReader, columnNo: Int): Double = reader.getDouble(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      java.lang.Double.compare(getField(reader, columnNo), getField(other, columnNo))
  }

  implicit object FloatFieldExtractor extends TypedFieldExtractor[Float] {
    final def getField(reader: RowReader, columnNo: Int): Float = reader.getFloat(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      java.lang.Float.compare(getField(reader, columnNo), getField(other, columnNo))
  }

  implicit object StringFieldExtractor extends TypedFieldExtractor[String] {
    final def getField(reader: RowReader, columnNo: Int): String = reader.getString(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      getField(reader, columnNo).compareTo(getField(other, columnNo))
  }

  implicit object UTF8StringFieldExtractor extends TypedFieldExtractor[ZeroCopyUTF8String] {
    final def getField(reader: RowReader, columnNo: Int): ZeroCopyUTF8String =
      reader.getUTF8String(columnNo)
    // TODO: do UTF8 comparison so we can avoid having to deserialize
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      getField(reader, columnNo).compareTo(getField(other, columnNo))
  }

  implicit object DateTimeFieldExtractor extends TypedFieldExtractor[DateTime] {
    final def getField(reader: RowReader, columnNo: Int): DateTime = reader.as[DateTime](columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      getField(reader, columnNo).compareTo(getField(other, columnNo))
  }

  implicit object TimestampFieldExtractor extends TypedFieldExtractor[Timestamp] {
    final def getField(reader: RowReader, columnNo: Int): Timestamp = reader.as[Timestamp](columnNo)
    // TODO: compare the Long, instead of deserializing and comparing Timestamp object
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      getField(reader, columnNo).compareTo(getField(other, columnNo))
  }
}
