package org.velvia.filo

import org.scalatest.FunSpec
import org.scalatest.Matchers

class SimpleEncodingTest extends FunSpec with Matchers {
  import BuilderEncoder.SimpleEncoding
  import ColumnParser._

  describe("Int encoding") {
    it("should encode an empty list and decode back to empty") {
      val cb = new IntColumnBuilder
      val buf = BuilderEncoder.builderToBuffer(cb, SimpleEncoding)
      val sc = ColumnParser.parse[Int](buf)

      sc.length should equal (0)
      sc.get(0) should equal (None)
      sc.toList should equal (Nil)
    }

    it("should decode a null ByteBuffer as an empty vector") {
      val sc = ColumnParser.parse[Int](null)
      sc.length should equal (0)
      sc.toList should equal (Nil)
    }

    it("should encode a list of all NAs and decode back to all NAs") {
      val cb = new IntColumnBuilder
      cb.addNA
      val buf = BuilderEncoder.builderToBuffer(cb, SimpleEncoding)
      val sc = ColumnParser.parse[Int](buf)

      sc.length should equal (1)
      sc(0)   // Just to make sure this does not throw an exception
      intercept[ArrayIndexOutOfBoundsException] { sc(1) }
      sc.isAvailable(0) should equal (false)
      sc.toList should equal (Nil)
      sc.optionIterator.toSeq should equal (Seq(None))
    }

    it("should encode a mix of NAs and Ints and decode iterate and skip NAs") {
      val cb = ColumnBuilder(classOf[Int]).asInstanceOf[ColumnBuilder[Int]]
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(103)
      cb.addNA
      val buf = BuilderEncoder.builderToBuffer(cb, SimpleEncoding)
      val sc = ColumnParser.parse[Int](buf)

      sc.length should equal (5)
      sc.isAvailable(0) should equal (false)
      sc.isAvailable(1) should equal (true)
      sc.isAvailable(4) should equal (false)
      sc(1) should equal (101)
      sc.boxed(2) should equal (102)
      sc.boxed(2) shouldBe a [java.lang.Integer]
      sc.get(0) should equal (None)
      sc.get(-1) should equal (None)
      sc.get(2) should equal (Some(102))
      sc.toList should equal (List(101, 102, 103))
    }

    it("should encode and decode back a sequence starting with NAs") {
      val orig = Seq(None, None, None, Some(10), Some(15))
      val buf = BuilderEncoder.seqOptionToBuffer(orig, SimpleEncoding)
      val binarySeq = ColumnParser.parse[Int](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.toSeq should equal (Seq(10, 15))
      binarySeq.optionIterator.toSeq should equal (orig)
    }

    it("should encode and decode back a Seq[Int]") {
      val orig = Seq(1, 2, -5, 101)
      val buf = BuilderEncoder.seqToBuffer(orig)
      val binarySeq = ColumnParser.parse[Int](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.sum should equal (orig.sum)
    }

    it("should handle NAs properly for Seq[Int] with more than 64 elements") {
      val orig: Seq[Option[Int]] = Seq(None, None) ++ (2 to 77).map(Some(_)) ++ Seq(None, None)
      val buf = BuilderEncoder.seqOptionToBuffer(orig, SimpleEncoding)
      val binarySeq = ColumnParser.parse[Int](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.isAvailable(77) should be (true)
      binarySeq.isAvailable(78) should be (false)
      binarySeq.optionIterator.toSeq should equal (orig)
      binarySeq.sum should equal ((2 to 77).sum)
    }
  }

  describe("Long encoding") {
    it("should encode and decode back a Seq[Long]") {
      val orig = Seq(0L, 0L)
      val buf = BuilderEncoder.seqToBuffer(orig)
      val binarySeq = ColumnParser.parse[Long](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.sum should equal (orig.sum)
    }
  }

  describe("String Encoding") {
    it("should encode and decode back a Seq[String]") {
      val orig = Seq("apple", "banana")
      val buf = BuilderEncoder.seqToBuffer(orig, SimpleEncoding)
      val binarySeq = ColumnParser.parse[String](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.toSeq should equal (orig)
    }
  }
}