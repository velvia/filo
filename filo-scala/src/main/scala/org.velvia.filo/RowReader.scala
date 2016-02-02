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

object RowReader {
  // Type class for extracting a field of a specific type
  trait TypedFieldExtractor[F] {
    def getField(reader: RowReader, columnNo: Int): F
  }

  implicit object BooleanFieldExtractor extends TypedFieldExtractor[Boolean] {
    final def getField(reader: RowReader, columnNo: Int): Boolean = reader.getBoolean(columnNo)
  }

  implicit object LongFieldExtractor extends TypedFieldExtractor[Long] {
    final def getField(reader: RowReader, columnNo: Int): Long = reader.getLong(columnNo)
  }

  implicit object IntFieldExtractor extends TypedFieldExtractor[Int] {
    final def getField(reader: RowReader, columnNo: Int): Int = reader.getInt(columnNo)
  }

  implicit object DoubleFieldExtractor extends TypedFieldExtractor[Double] {
    final def getField(reader: RowReader, columnNo: Int): Double = reader.getDouble(columnNo)
  }

  implicit object FloatFieldExtractor extends TypedFieldExtractor[Float] {
    final def getField(reader: RowReader, columnNo: Int): Float = reader.getFloat(columnNo)
  }

  implicit object StringFieldExtractor extends TypedFieldExtractor[String] {
    final def getField(reader: RowReader, columnNo: Int): String = reader.getString(columnNo)
  }

  implicit object DateTimeFieldExtractor extends TypedFieldExtractor[DateTime] {
    final def getField(reader: RowReader, columnNo: Int): DateTime = reader.as[DateTime](columnNo)
  }

  implicit object TimestampFieldExtractor extends TypedFieldExtractor[Timestamp] {
    final def getField(reader: RowReader, columnNo: Int): Timestamp = reader.as[Timestamp](columnNo)
  }
}

/**
 * A RowReader designed for iteration over rows of multiple Filo vectors, ideally all
 * with the same length.
 * An Iterator[RowReader] sets the rowNo and returns this RowReader, and
 * the application is responsible for calling the right method to extract each value.
 * For example, a Spark Row can inherit from RowReader.
 */
trait FiloRowReader extends RowReader {
  def parsers: Array[FiloVector[_]]
  def rowNo: Int
  def setRowNo(newRowNo: Int)
}

/**
 * Just a concrete implementation.
 * Designed to minimize allocation by having iterator repeatedly set/update rowNo.
 * Thus, this is not appropriate for Seq[RowReader] or conversion to Seq.
 */
class FastFiloRowReader(chunks: Array[ByteBuffer],
                        classes: Array[Class[_]],
                        emptyLen: Int = 0) extends FiloRowReader {
  var rowNo: Int = -1
  def setRowNo(newRowNo: Int): Unit = { rowNo = newRowNo }

  val parsers = FiloVector.makeVectors(chunks, classes, emptyLen)

  final def notNull(columnNo: Int): Boolean = parsers(columnNo).isAvailable(rowNo)
  final def getBoolean(columnNo: Int): Boolean = parsers(columnNo).asInstanceOf[FiloVector[Boolean]](rowNo)
  final def getInt(columnNo: Int): Int = parsers(columnNo).asInstanceOf[FiloVector[Int]](rowNo)
  final def getLong(columnNo: Int): Long = parsers(columnNo).asInstanceOf[FiloVector[Long]](rowNo)
  final def getDouble(columnNo: Int): Double = parsers(columnNo).asInstanceOf[FiloVector[Double]](rowNo)
  final def getFloat(columnNo: Int): Float = parsers(columnNo).asInstanceOf[FiloVector[Float]](rowNo)
  final def getString(columnNo: Int): String = parsers(columnNo).asInstanceOf[FiloVector[String]](rowNo)
  final def getAny(columnNo: Int): Any = parsers(columnNo).boxed(rowNo)
}
