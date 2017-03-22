package org.velvia.filo.vectors

import org.scalatest.{FunSpec, Matchers}
import org.velvia.filo.{FiloVector, GrowableVector, VectorTooSmall}

class LongVectorTest extends FunSpec with Matchers {
  def maxPlus(i: Int): Long = Int.MaxValue.toLong + i

  describe("LongMaskedAppendableVector") {
    it("should append a list of all NAs and read all NAs back") {
      val builder = LongBinaryVector.appendingVector(100)
      builder.addNA
      builder.isAllNA should be (true)
      builder.noNAs should be (false)
      val sc = builder.freeze()
      sc.length should equal (1)
      sc(0)   // Just to make sure this does not throw an exception
      sc.isAvailable(0) should equal (false)
      sc.toList should equal (Nil)
      sc.optionIterator.toSeq should equal (Seq(None))
    }

    it("should encode a mix of NAs and Longs and decode iterate and skip NAs") {
      val cb = LongBinaryVector.appendingVector(5)
      cb.addNA
      cb.addData(101)
      cb.addData(maxPlus(102))
      cb.addData(103)
      cb.addNA
      cb.isAllNA should be (false)
      cb.noNAs should be (false)
      val sc = cb.freeze()

      sc.length should equal (5)
      sc.isAvailable(0) should equal (false)
      sc.isAvailable(1) should equal (true)
      sc.isAvailable(4) should equal (false)
      sc(1) should equal (101)
      sc.boxed(2) should equal (maxPlus(102))
      sc.boxed(2) shouldBe a [java.lang.Long]
      sc.get(0) should equal (None)
      sc.get(-1) should equal (None)
      sc.get(2) should equal (Some(maxPlus(102)))
      sc.toList should equal (List(101, maxPlus(102), 103))
    }

    it("should be able to append lots of longs and grow vector") {
      val numInts = 1000
      val builder = LongBinaryVector.appendingVector(numInts / 2)
      (0 until numInts).map(_.toLong).foreach(builder.addData)
      builder.length should equal (numInts)
      builder.isAllNA should be (false)
      builder.noNAs should be (true)
    }

    it("should be able to return minMax accurately with NAs") {
      val cb = LongBinaryVector.appendingVector(5)
      cb.addNA
      cb.addData(-maxPlus(100))
      cb.addData(102)
      cb.addData(maxPlus(103))
      cb.addNA
      val inner = cb.asInstanceOf[GrowableVector[Long]].inner.asInstanceOf[MaskedLongAppendingVector]
      inner.minMax should equal ((-maxPlus(100), maxPlus(103)))
    }

    it("should be able to freeze() and minimize bytes used") {
      val builder = LongBinaryVector.appendingVector(100)
      // Test numBytes to make sure it's accurate
      builder.numBytes should equal (4 + 16 + 4)   // 2 long words needed for 100 bits
      (0 to 4).map(_.toLong).foreach(builder.addData)
      builder.numBytes should equal (4 + 16 + 4 + 40)
      val frozen = builder.freeze()
      frozen.numBytes should equal (4 + 8 + 4 + 40)  // bitmask truncated

      frozen.length should equal (5)
      frozen.toSeq should equal (0 to 4)
    }

    it("should toFiloBuffer() and read back using FiloVector.apply") {
      val cb = LongBinaryVector.appendingVector(5)
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(maxPlus(104))
      cb.addNA
      val buffer = cb.optimize().toFiloBuffer()
      val readVect = FiloVector[Long](buffer)
      readVect shouldBe a[MaskedLongBinaryVector]
      readVect.toSeq should equal (Seq(101, 102, maxPlus(104)))
    }

    it("should be able to optimize longs with fewer nbits to IntBinaryVector") {
      val builder = LongBinaryVector.appendingVector(100)
      (0 to 4).map(_.toLong).foreach(builder.addData)
      val optimized = builder.optimize()
      optimized.length should equal (5)
      optimized.toSeq should equal (0 to 4)
      optimized.noNAs should equal (true)

      val frozen = optimized.freeze()
      frozen.numBytes should equal (4 + 3)   // nbits=4, so only 3 extra bytes
    }

    it("should be able to optimize constant longs to an IntConstVector") {
      val builder = LongBinaryVector.appendingVector(100)
      val longVal = Int.MaxValue.toLong + 100
      (0 to 4).foreach(n => builder.addData(longVal))
      val buf = builder.optimize().toFiloBuffer
      val readVect = FiloVector[Long](buf)
      readVect shouldBe a[LongConstVector]
      readVect.toSeq should equal (Seq(longVal, longVal, longVal, longVal, longVal))
    }
  }
}