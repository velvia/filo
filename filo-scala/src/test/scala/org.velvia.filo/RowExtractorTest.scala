package org.velvia.filo

import org.scalatest.FunSpec
import org.scalatest.Matchers

class RowExtractorTest extends FunSpec with Matchers {
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

  it("should extract from columns back to rows") {
    val columnData = RowToColumnBuilder.buildFromRows(rows, schema, TupleRowIngestSupport,
                                                      BuilderEncoder.SimpleEncoding)
    val chunks = Array(columnData("name"), columnData("age"))
    val types = schema.map(_.dataType)
    val extractors = RowExtractors.getRowExtractors(chunks, types, ArrayStringRowSetter)

    extractors should have length (2)
    val row = Array("foo", "bar")
    extractors(0).extractToRow(0, row)
    extractors(1).extractToRow(0, row)
    row should equal (Array("Matthew Perry", "18"))

    extractors(0).extractToRow(1, row)
    extractors(1).extractToRow(1, row)
    row should equal (Array("Michelle Pfeiffer", ""))
  }
}