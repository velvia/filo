package org.velvia.filo

import org.scalatest.FunSpec
import org.scalatest.Matchers

class RowToColumnBuilderTest extends FunSpec with Matchers {
  val schema = Seq(
                 IngestColumn("name", new StringColumnBuilder),
                 IngestColumn("age", new IntColumnBuilder)
               )

  val rows = Seq(
               (Some("Matthew Perry"), Some(18)),
               (Some("Michelle Pfeiffer"), None),
               (Some("George C"), Some(59)),
               (Some("Rich Sherman"), Some(26))
             )

  describe("RowToColumnBuilder") {
    import ColumnParser._

    it("should add rows and convert them to Filo binary Seqs") {
      val rtcb = new RowToColumnBuilder(schema, TupleRowIngestSupport)
      rows.foreach(rtcb.addRow)
      rtcb.addEmptyRow()
      val columnData = rtcb.convertToBytes()

      columnData.keys should equal (Set("name", "age"))
      val nameBinSeq = ColumnParser.parseAsSimpleColumn[String](columnData("name"))
      nameBinSeq.toList should equal (List("Matthew Perry", "Michelle Pfeiffer",
                                                 "George C", "Rich Sherman"))
      val ageBinSeq = ColumnParser.parseAsSimpleColumn[Int](columnData("age"))
      ageBinSeq should have length (5)
      ageBinSeq(0) should equal (18)
      ageBinSeq.toList should equal (List(18, 59, 26))
    }
  }
}