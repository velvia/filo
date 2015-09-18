package org.velvia.filo

import org.scalatest.FunSpec
import org.scalatest.Matchers

class RowReaderTest extends FunSpec with Matchers {
  val schema = Seq(
                 IngestColumn("name", classOf[String]),
                 IngestColumn("age",  classOf[Int])
               )

  val rows = Seq(
               (Some("Matthew Perry"), Some(18)),
               (Some("Michelle Pfeiffer"), None),
               (Some("George C"), Some(59)),
               (Some("Rich Sherman"), Some(26))
             )

  def readValues[T](r: FastFiloRowReader, len: Int)(f: FiloRowReader => T): Seq[T] = {
    (0 until len).map { i =>
      r.rowNo = i
      f(r)
    }
  }

  it("should extract from columns back to rows") {
    val columnData = RowToColumnBuilder.buildFromRows(rows.map(TupleRowReader).toIterator,
                                                      schema,
                                                      BuilderEncoder.SimpleEncoding)
    val chunks = Array(columnData("name"), columnData("age"))
    val types = schema.map(_.dataType)
    val reader = new FastFiloRowReader(chunks, types.toArray)

    readValues(reader, 4)(_.getString(0)) should equal (
      Seq("Matthew Perry", "Michelle Pfeiffer", "George C", "Rich Sherman"))

    reader.rowNo = 1
    reader.notNull(1) should equal (false)
  }
}