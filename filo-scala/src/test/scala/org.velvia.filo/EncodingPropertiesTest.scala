package org.velvia.filo

import org.scalatest.FunSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks

class EncodingPropertiesTest extends FunSpec with Matchers with PropertyChecks {
  import BuilderEncoder._
  import VectorReader._

  it("Filo format int vectors should match length and sum") {
    forAll { (s: List[Int]) =>
      val buf = VectorBuilder(s).toFiloBuffer
      val binarySeq = FiloVector[Int](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  it("Filo format long vectors should match length and sum") {
    forAll { (s: List[Long]) =>
      val buf = VectorBuilder(s).toFiloBuffer
      val binarySeq = FiloVector[Long](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  it("Filo format double vectors should match length and sum") {
    forAll { (s: List[Double]) =>
      val buf = VectorBuilder(s).toFiloBuffer
      val binarySeq = FiloVector[Double](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  it("Filo format float vectors should match length and sum") {
    forAll { (s: List[Float]) =>
      val buf = VectorBuilder(s).toFiloBuffer
      val binarySeq = FiloVector[Float](buf)

      binarySeq.length should equal (s.length)
      binarySeq.sum should equal (s.sum)
    }
  }

  it("Filo format boolean vectors should match length and number of true values") {
    forAll { (s: List[Boolean]) =>
      val buf = VectorBuilder(s).toFiloBuffer
      val binarySeq = FiloVector[Boolean](buf)

      binarySeq.length should equal (s.length)
      binarySeq.filter(x => x) should equal (s.filter(x => x))
    }
  }

  import org.scalacheck._
  import Arbitrary.arbitrary

  // Generate a list of bounded integers, every time bound it slightly differently
  // (to test different int compression techniques)
  def boundedIntList: Gen[Seq[Option[Int]]] =
    for {
      minVal <- Gen.oneOf(Int.MinValue, -65536, -32768, -256, -128, 0)
      maxVal <- Gen.oneOf(15, 127, 255, 32767, Int.MaxValue)
      seqOptList <- Gen.containerOf[Seq, Option[Int]](
                      noneOrThing[Int](Arbitrary(Gen.choose(minVal, maxVal))))
    } yield { seqOptList }

  // Write our own generator to force frequent NA elements
  def noneOrThing[T](implicit a: Arbitrary[T]): Gen[Option[T]] =
    Gen.frequency((5, arbitrary[T].map(Some(_))),
                  (1, Gen.const(None)))

  def optionList[T](implicit a: Arbitrary[T]): Gen[Seq[Option[T]]] =
    Gen.containerOf[Seq, Option[T]](noneOrThing[T])

  it("should match elements and length for Int vectors with missing/NA elements") {
    forAll(boundedIntList) { s =>
      val buf = VectorBuilder.fromOptions(s).toFiloBuffer
      val binarySeq = FiloVector[Int](buf)

      binarySeq.length should equal (s.length)
      val elements = binarySeq.optionIterator.toSeq
      elements should equal (s)
    }
  }

  it("should match elements and length for simple string vectors with missing/NA elements") {
    forAll(optionList[String]) { s =>
      val buf = VectorBuilder.fromOptions(s).toFiloBuffer(SimpleEncoding)
      val binarySeq = FiloVector[String](buf)

      binarySeq.length should equal (s.length)
      val elements = binarySeq.optionIterator.toSeq
      elements should equal (s)
    }
  }

  it("should match elements and length for dictionary string vectors with missing/NA elements") {
    forAll(optionList[String]) { s =>
      val buf = VectorBuilder.fromOptions(s).toFiloBuffer(DictionaryEncoding)
      val binarySeq = FiloVector[String](buf)

      binarySeq.length should equal (s.length)
      val elements = binarySeq.optionIterator.toSeq
      elements should equal (s)
    }
  }
}