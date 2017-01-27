package org.velvia.filo.vectors

import org.scalatest.{FunSpec, Matchers}
import org.velvia.filo.FiloVector

class IntBinaryVectorTest extends FunSpec with Matchers {
  describe("IntAppendingVector") {
    it("should append a mix of Ints and read them all back") {
      val builder = IntBinaryVector.appendingVectorNoNA(4)
      val orig = Seq(1, 2, -5, 101)
      orig.foreach(builder.addData)
      builder.length should equal (4)
      builder.freeze.toSeq should equal (orig)
    }

    it("should append 16-bit Ints and read them back") {
      val builder = IntBinaryVector.appendingVectorNoNA(5)
      val orig = Seq(1, 0, -127, Short.MaxValue, Short.MinValue)
      orig.foreach(builder.addData)
      builder.length should equal (5)
      builder.freeze.toSeq should equal (orig)
    }

    it("should append bytes and read them back") {
      val builder = IntBinaryVector.appendingVectorNoNA(4)
      val orig = Seq(1, 0, -128, 127)
      orig.foreach(builder.addData)
      builder.length should equal (4)
      builder.freeze.toSeq should equal (orig)
    }

    it("should be able to create new FiloVector from frozen appending vector") {
      val builder = IntBinaryVector.appendingVectorNoNA(4)
      val orig = Seq(1, 0, -128, 127)
      orig.foreach(builder.addData)
      val readVect = IntBinaryVector(builder.base, builder.offset, builder.numBytes)
      readVect.length should equal (4)
      readVect.toSeq should equal (orig)

      val frozen = builder.freeze()
      frozen.length should equal (4)
      frozen.toSeq should equal (orig)
    }
  }

  describe("MaskedIntAppendingVector") {
    it("should append a list of all NAs and read all NAs back") {
      val builder = IntBinaryVector.appendingVector(4)
      builder.addNA
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
      val sc = builder.freeze
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
      cb.isAllNA should be (false)
      cb.noNAs should be (false)
      val sc = cb.freeze

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
      builder.isAllNA should be (false)
      builder.noNAs should be (true)
    }

    it("should be able to return minMax accurately with NAs") {
      val cb = IntBinaryVector.appendingVector(5)
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(103)
      cb.addNA
      cb.asInstanceOf[MaskedIntAppendingVector].minMax should equal ((101, 103))
    }

    it("should be able to freeze() and minimize bytes used") {
      val builder = IntBinaryVector.appendingVector(100)
      // Test numBytes to make sure it's accurate
      builder.numBytes should equal (4 + 16 + 4)   // 2 long words needed for 100 bits
      (0 to 4).foreach(builder.addData)
      builder.numBytes should equal (4 + 16 + 4 + 20)
      val frozen = builder.freeze()
      frozen.numBytes should equal (4 + 8 + 4 + 20)  // bitmask truncated

      frozen.length should equal (5)
      frozen.toSeq should equal (0 to 4)
    }

    it("should toFiloBuffer() and read back using FiloVector.apply") {
      val cb = IntBinaryVector.appendingVector(5)
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(103)
      cb.addNA
      val buffer = IntBinaryVector.optimize(cb).toFiloBuffer()
      val readVect = FiloVector[Int](buffer)
      readVect.toSeq should equal (Seq(101, 102, 103))
    }

    it("should be able to optimize a 32-bit appending vector to smaller size") {
      val builder = IntBinaryVector.appendingVector(100)
      (0 to 4).foreach(builder.addData)
      val optimized = IntBinaryVector.optimize(builder)
      optimized.length should equal (5)
      optimized.toSeq should equal (0 to 4)
      optimized.noNAs should equal (true)

      val frozen = optimized.freeze()
      frozen.numBytes should equal (4 + 5)
    }
  }
}