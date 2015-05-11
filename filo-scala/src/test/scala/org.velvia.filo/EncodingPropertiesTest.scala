package org.velvia.filo

import org.scalatest.FunSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks

class EncodingPropertiesTest extends FunSpec with Matchers with PropertyChecks {
  import BuilderEncoder._
  import ColumnParser._

  it("Filo format int vectors should match length and sum") {
    forAll { (s: List[Int]) =>
      val buf = seqToBuffer(s, SimpleEncoding)
      val binarySeq = ColumnParser.parseAsSimpleColumn[Int](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  it("Filo format long vectors should match length and sum") {
    forAll { (s: List[Long]) =>
      val buf = seqToBuffer(s, SimpleEncoding)
      val binarySeq = ColumnParser.parseAsSimpleColumn[Long](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  it("Filo format double vectors should match length and sum") {
    forAll { (s: List[Double]) =>
      val buf = seqToBuffer(s, SimpleEncoding)
      val binarySeq = ColumnParser.parseAsSimpleColumn[Double](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  it("should match elements and length for Int vectors with missing/NA elements") {
    forAll { (s: List[Option[Int]]) =>
      val buf = seqOptionToBuffer(s, SimpleEncoding)
      val binarySeq = ColumnParser.parseAsSimpleColumn[Int](buf)

      binarySeq.length should equal (s.length)
      val elements = binarySeq.optionIterator.toSeq
      elements should equal (s)
    }
  }

  it("should match elements and length for simple string vectors with missing/NA elements") {
    forAll { (s: List[Option[String]]) =>
      val buf = seqOptionToBuffer(s, SimpleEncoding)
      val binarySeq = ColumnParser.parseAsSimpleColumn[String](buf)

      binarySeq.length should equal (s.length)
      val elements = binarySeq.optionIterator.toSeq
      elements should equal (s)
    }
  }

  it("should match elements and length for dictionary string vectors with missing/NA elements") {
    forAll { (s: List[Option[String]]) =>
      val buf = seqOptionToBuffer(s, DictionaryEncoding)
      val binarySeq = ColumnParser.parseAsSimpleColumn[String](buf)

      binarySeq.length should equal (s.length)
      val elements = binarySeq.optionIterator.toSeq
      elements should equal (s)
    }
  }
}