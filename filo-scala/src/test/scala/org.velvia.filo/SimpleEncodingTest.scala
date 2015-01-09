package org.velvia.filo

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

class SimpleEncodingTest extends FunSpec with ShouldMatchers {
  describe("Int encoding") {
    it("should encode an empty list and decode back to empty") (pending)

    it("should encode a list of all NAs and decode back to all NAs") (pending)

    it("should encode a mix of NAs and Ints and decode iterate and skip NAs") {
      val cb = new IntColumnBuilder
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(103)
      cb.addNA
      val buf = BuilderEncoder.encodeToBuffer(cb)
      val sc = ColumnParser.parseAsSimpleColumn(buf)

      sc.isAvailable(0) should equal (false)
      sc.isAvailable(1) should equal (true)
      sc.isAvailable(4) should equal (false)
      sc.toList should equal (List(101, 102, 103))
    }
  }
}