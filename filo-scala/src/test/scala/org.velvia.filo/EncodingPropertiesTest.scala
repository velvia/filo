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
      val binarySeq = ColumnParser.parse[Int](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  it("Filo format long vectors should match length and sum") {
    forAll { (s: List[Long]) =>
      val buf = seqToBuffer(s, SimpleEncoding)
      val binarySeq = ColumnParser.parse[Long](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  it("Filo format double vectors should match length and sum") {
    forAll { (s: List[Double]) =>
      val buf = seqToBuffer(s, SimpleEncoding)
      val binarySeq = ColumnParser.parse[Double](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  import org.scalacheck._
  import Gen._
  import Arbitrary.arbitrary

  // Write our own generator to force frequent NA elements
  def noneOrThing[T](implicit a: Arbitrary[T]): Gen[Option[T]] =
    Gen.frequency((5, arbitrary[T].map(Some(_))),
                  (1, const(None)))

  def optionList[T](implicit a: Arbitrary[T]): Gen[Seq[Option[T]]] =
    Gen.containerOf[Seq, Option[T]](noneOrThing[T])

  it("should match elements and length for Int vectors with missing/NA elements") {
    forAll(optionList[Int]) { s =>
      val buf = seqOptionToBuffer(s, SimpleEncoding)
      val binarySeq = ColumnParser.parse[Int](buf)

      binarySeq.length should equal (s.length)
      val elements = binarySeq.optionIterator.toSeq
      elements should equal (s)
    }
  }

  it("should match elements and length for simple string vectors with missing/NA elements") {
    forAll(optionList[String]) { s =>
      val buf = seqOptionToBuffer(s, SimpleEncoding)
      val binarySeq = ColumnParser.parse[String](buf)

      binarySeq.length should equal (s.length)
      val elements = binarySeq.optionIterator.toSeq
      elements should equal (s)
    }
  }

  it("should match elements and length for dictionary string vectors with missing/NA elements") {
    forAll(optionList[String]) { s =>
      val buf = seqOptionToBuffer(s, DictionaryEncoding)
      val binarySeq = ColumnParser.parse[String](buf)

      binarySeq.length should equal (s.length)
      val elements = binarySeq.optionIterator.toSeq
      elements should equal (s)
    }
  }
}