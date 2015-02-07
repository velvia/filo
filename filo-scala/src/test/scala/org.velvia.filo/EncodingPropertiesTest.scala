package org.velvia.filo

import org.scalatest.FunSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks

class EncodingPropertiesTest extends FunSpec with Matchers with PropertyChecks {
  it("Filo format int vectors should match length and sum") {
    forAll { (s: List[Int]) =>
      val buf = BuilderEncoder.seqToBuffer(s)
      val binarySeq = ColumnParser.parseAsSimpleColumn[Int](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  it("Filo format long vectors should match length and sum") {
    forAll { (s: List[Long]) =>
      val buf = BuilderEncoder.seqToBuffer(s)
      val binarySeq = ColumnParser.parseAsSimpleColumn[Long](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  it("Filo format double vectors should match length and sum") {
    forAll { (s: List[Double]) =>
      val buf = BuilderEncoder.seqToBuffer(s)
      val binarySeq = ColumnParser.parseAsSimpleColumn[Double](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }
}