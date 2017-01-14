package org.velvia.filo

import org.joda.time.DateTime
import java.sql.Timestamp
import org.scalatest.FunSpec
import org.scalatest.Matchers

class RowReaderTest extends FunSpec with Matchers {
  val schema = Seq(
                 VectorInfo("name", classOf[String]),
                 VectorInfo("age",  classOf[Int]),
                 VectorInfo("timestamp", classOf[Timestamp])
               )

  val rows = Seq(
               (Some("Matthew Perry"),     Some(18), Some(new Timestamp(10000L))),
               (Some("Michelle Pfeiffer"), None,     Some(new Timestamp(10010L))),
               (Some("George C"),          Some(59), None),
               (Some("Rich Sherman"),      Some(26), Some(new Timestamp(10000L)))
             )

  val csvRows = Seq(
    "Matthew Perry,18,1973-01-25",
    "Michelle Pfeiffer,,1970-07-08",
    "George C,59,",
    "Rich Sherman,26,1991-10-12"
  ).map(str => (str.split(',') :+ "").take(3))

  def readValues[T](r: FastFiloRowReader, len: Int)(f: FiloRowReader => T): Seq[T] = {
    (0 until len).map { i =>
      r.rowNo = i
      f(r)
    }
  }

  it("should extract from columns back to rows") {
    val columnData = RowToVectorBuilder.buildFromRows(rows.map(TupleRowReader).toIterator,
                                                      schema,
                                                      BuilderEncoder.SimpleEncoding)
    val chunks = Array(columnData("name"), columnData("age"), columnData("timestamp"))
    val types = schema.map(_.dataType)
    val reader = new FastFiloRowReader(chunks, types.toArray)

    readValues(reader, 4)(_.getString(0)) should equal (
      Seq("Matthew Perry", "Michelle Pfeiffer", "George C", "Rich Sherman"))

    reader.rowNo = 1
    reader.notNull(1) should equal (false)
    reader.as[Timestamp](2) should equal (new Timestamp(10010L))
  }

  it("should write to columns from ArrayStringRowReader and read back properly") {
    val columnData = RowToVectorBuilder.buildFromRows(csvRows.map(ArrayStringRowReader).toIterator,
                                                      schema,
                                                      BuilderEncoder.SimpleEncoding)
    val chunks = Array(columnData("name"), columnData("age"), columnData("timestamp"))
    val types = schema.map(_.dataType)
    val reader = new FastFiloRowReader(chunks, types.toArray)

    readValues(reader, 4)(_.getString(0)) should equal (
      Seq("Matthew Perry", "Michelle Pfeiffer", "George C", "Rich Sherman"))

    reader.rowNo = 1
    reader.notNull(1) should equal (false)
    reader.as[Timestamp](2) should equal (new Timestamp(DateTime.parse("1970-07-08").getMillis))
  }

  it("should read longs from timestamp strings from ArrayStringRowReader") {
    ArrayStringRowReader(csvRows.head).getLong(2) should equal (96796800000L)
  }

  import RowReader._
  it("should compare RowReaders using TypedFieldExtractor") {
    val readers = rows.map(TupleRowReader)
    StringFieldExtractor.compare(readers(1), readers(2), 0) should be > (0)
    IntFieldExtractor.compare(readers(0), readers(2), 1) should be < (0)
    TimestampFieldExtractor.compare(readers(0), readers(3), 2) should equal (0)

    // Ok, we should be able to compare the reader with the NA / None too
    IntFieldExtractor.compare(readers(1), readers(2), 1) should be < (0)
  }
}