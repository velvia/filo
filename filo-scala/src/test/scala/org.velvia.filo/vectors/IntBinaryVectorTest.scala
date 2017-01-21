package org.velvia.filo.vectors

import org.scalatest.{FunSpec, Matchers}

class IntBinaryVectorTest extends FunSpec with Matchers {
  describe("Int32AppendingVector") {
    it("should append a mix of Ints and read them all back") {
      val builder = IntBinaryVector.appendingVectorNoNA(4)
      val orig = Seq(1, 2, -5, 101)
      orig.foreach(builder.addData)
      builder.length should equal (4)
      builder.immute.toSeq should equal (orig)
    }
  }

  describe("MaskedIntAppendingVector") {
    it("should append a list of all NAs and read all NAs back") {
      val builder = IntBinaryVector.appendingVector(4)
      builder.addNA
      val sc = builder.immute
      sc.length should equal (1)
      sc(0)   // Just to make sure this does not throw an exception
      sc.isAvailable(0) should equal (false)
      sc.toList should equal (Nil)
      sc.optionIterator.toSeq should equal (Seq(None))
    }

    it("should encode a mix of NAs and Ints and decode iterate and skip NAs") {
      val cb = IntBinaryVector.appendingVector(5)
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(103)
      cb.addNA
      val sc = cb.immute

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

    it("should be able to append lots of ints") {
      val numInts = 1000
      val builder = IntBinaryVector.appendingVector(numInts)
      (0 until numInts).foreach(builder.addData)
      builder.length should equal (numInts)
    }
  }
}