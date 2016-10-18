package org.velvia.filo

import java.nio.{ByteBuffer, ByteOrder}

import org.velvia.filo.codecs.SimplePrimitiveWrapper
import org.velvia.filo.vector._

import org.scalatest.FunSpec
import org.scalatest.Matchers

class SimpleEncodingTest extends FunSpec with Matchers {
  import BuilderEncoder.SimpleEncoding
  import VectorReader._

  private def checkVectorType(bb: ByteBuffer, majorType: Int, minorType: Int): Unit = {
    val headerBytes = bb.getInt(0)
    WireFormat.majorVectorType(headerBytes) should equal (majorType)
    WireFormat.vectorSubType(headerBytes) should equal (minorType)
  }

  describe("Simple Int encoding") {
    it("should encode an empty list and decode back to empty") {
      val cb = new IntVectorBuilder
      val buf = cb.toFiloBuffer(SimpleEncoding)
      val sc = FiloVector[Int](buf)

      sc.length should equal (0)
      sc.get(0) should equal (None)
      sc.toList should equal (Nil)
    }

    it("should decode a null ByteBuffer as an empty vector") {
      val sc = FiloVector[Int](null)
      sc.length should equal (0)
      sc.toList should equal (Nil)
    }

    it("should encode a list of all NAs and decode back to all NAs") {
      val cb = new IntVectorBuilder
      cb.addNA
      val buf = cb.toFiloBuffer(SimpleEncoding)
      val headerBytes = buf.getInt(0)
      WireFormat.majorVectorType(headerBytes) should equal (WireFormat.VECTORTYPE_EMPTY)
      buf.capacity should equal (4)
      val sc = FiloVector[Int](buf)

      sc.length should equal (1)
      sc(0)   // Just to make sure this does not throw an exception
      intercept[ArrayIndexOutOfBoundsException] { sc(1) }
      sc.isAvailable(0) should equal (false)
      sc.toList should equal (Nil)
      sc.optionIterator.toSeq should equal (Seq(None))
    }

    it("should encode a mix of NAs and Ints and decode iterate and skip NAs") {
      val cb = VectorBuilder(classOf[Int]).asInstanceOf[VectorBuilder[Int]]
      cb.addNA
      cb.addData(101)
      cb.addData(102)
      cb.addData(103)
      cb.addNA
      val buf = cb.toFiloBuffer(SimpleEncoding)
      checkVectorType(buf, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_PRIMITIVE)
      val sc = FiloVector[Int](buf)

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
      val buf = VectorBuilder.fromOptions(orig).toFiloBuffer(SimpleEncoding)
      val binarySeq = FiloVector[Int](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.toSeq should equal (Seq(10, 15))
      binarySeq.optionIterator.toSeq should equal (orig)
    }

    it("should encode and decode back a Seq[Int]") {
      val orig = Seq(1, 2, -5, 101)
      val buf = VectorBuilder(orig).toFiloBuffer(SimpleEncoding)
      checkVectorType(buf, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_PRIMITIVE)
      buf.capacity should equal (64)    // should encode to signed bytes
      val binarySeq = FiloVector[Int](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.sum should equal (orig.sum)
      val spw = binarySeq.asInstanceOf[SimplePrimitiveWrapper[Int]]
      spw.isEmptyMask should equal (true)
      (0 to 3).foreach { i => spw.isAvailable(i) should equal (true) }   // no NA bit set
    }

    it("should encode and decode back different length Seq[Int]") {
      // to test various byte alignments of byte-sized int vectors
      for { len <- 4 to 7 } {
        val orig = (0 until len).toSeq
        val buf = VectorBuilder(orig).toFiloBuffer(SimpleEncoding)
        checkVectorType(buf, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_PRIMITIVE)
        val bufLen = if (len == 4) 64 else 68
        buf.capacity should equal (bufLen)
        val binarySeq = FiloVector[Int](buf)

        binarySeq.length should equal (orig.length)
        binarySeq.sum should equal (orig.sum)
      }
    }

    it("should handle NAs properly for Seq[Int] with more than 64 elements") {
      val orig: Seq[Option[Int]] = Seq(None, None) ++ (2 to 77).map(Some(_)) ++ Seq(None, None)
      val buf = VectorBuilder.fromOptions(orig).toFiloBuffer(SimpleEncoding)
      val binarySeq = FiloVector[Int](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.isAvailable(77) should be (true)
      binarySeq.isAvailable(78) should be (false)
      binarySeq.optionIterator.toSeq should equal (orig)
      binarySeq.sum should equal ((2 to 77).sum)
    }

    it("should be able to parse same ByteBuffer many times") {
      val orig = Seq(1, 2, -5, 101)
      val buf = VectorBuilder(orig).toFiloBuffer

      val seq1 = FiloVector[Int](buf)
      seq1.length should equal (orig.length)
      seq1.sum should equal (orig.sum)

      buf.order(ByteOrder.BIG_ENDIAN)   // See if the byte order will be reset when reading
      val seq2 = FiloVector[Int](buf)
      seq2.length should equal (orig.length)
      seq2.sum should equal (orig.sum)

      val seq3 = FiloVector[Int](buf)
      seq3.length should equal (orig.length)
      seq3.sum should equal (orig.sum)
    }
  }

  describe("Long encoding") {
    it("should encode and decode back a Seq[Long]") {
      val orig = Seq(0L, 1L)
      val buf = VectorBuilder(orig).toFiloBuffer
      val binarySeq = FiloVector[Long](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.sum should equal (orig.sum)
    }

    it("should encode and decode back a Seq[Long] with const values and NAs") {
      val orig = Seq(0L, 0L)
      val buf = VectorBuilder(orig).toFiloBuffer
      checkVectorType(buf, WireFormat.VECTORTYPE_CONST, WireFormat.SUBTYPE_PRIMITIVE)
      val binarySeq = FiloVector[Long](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.sum should equal (orig.sum)

      val orig2 = Seq(None, Some(1L), None, None, Some(1L))
      val buf2 = VectorBuilder.fromOptions(orig2).toFiloBuffer
      checkVectorType(buf2, WireFormat.VECTORTYPE_CONST, WireFormat.SUBTYPE_PRIMITIVE)
      val binarySeq2 = FiloVector[Long](buf2)

      binarySeq2.length should equal (orig2.length)
      binarySeq2.optionIterator.toSeq should equal (orig2)
    }

    it("should encode and decode back appropriately short sequences") {
      val shortSeqs = Seq(Seq(Short.MaxValue.toLong, Short.MinValue.toLong, 0),
                          Seq(0, 65535L, 256L))
      for { orig <- shortSeqs } {
        val buf = VectorBuilder(orig).toFiloBuffer(SimpleEncoding)
        buf.capacity should equal (68)
        val binarySeq = FiloVector[Long](buf)

        binarySeq.length should equal (orig.length)
        binarySeq.sum should equal (orig.sum)
      }

      val intSeqs = Seq(Seq(Int.MaxValue.toLong, Int.MinValue.toLong, 0),
                        Seq(0L, 65536L, 65536L * 65536L - 1L))
      for { orig <- intSeqs } {
        val buf = VectorBuilder(orig).toFiloBuffer(SimpleEncoding)
        buf.capacity should equal (72)
        val binarySeq = FiloVector[Long](buf)

        binarySeq.length should equal (orig.length)
        binarySeq.sum should equal (orig.sum)
      }
    }
  }

  describe("Bool encoding") {
    it("should encode and decode back a Seq[Boolean]") {
      val orig = List(true, true, false, true, false, false, true, true,  // 0xcb
                      false, true, true, true, false, true, true, true,   // 0xee
                      false, false, true, true, true, true, false, true,  // 0xbc
                      true, true, false, false, false, true, false, false,  // 0x23
                      false, true, false, false, false, true, true, true, // 0xe2
                      true, false, false, false, true, true, false, true, // 0xb1
                      true, true, false, true, false, true, false, true,  // 0xab
                      false, true, true, true, true, true, true, true,    // 0xfe
                      // Notice how the 9th byte is all false.  Ensure there is a second long in bitmask.
                      false, false, false, false)                         // 0x00
      val buf = VectorBuilder(orig).toFiloBuffer
      val binarySeq = FiloVector[Boolean](buf)

      binarySeq.length should equal (orig.length)
      val spw = binarySeq.asInstanceOf[SimplePrimitiveWrapper[Boolean]]
      spw.isEmptyMask should equal (true)   // no NA bit set
      binarySeq.filter(b => b).size should equal (orig.filter(b => b).size)
    }
  }

  describe("String Encoding") {
    it("should encode and decode back a Seq[String]") {
      val orig = Seq("apple", "banana")
      val buf = VectorBuilder(orig).toFiloBuffer(SimpleEncoding)
      checkVectorType(buf, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_STRING)
      val binarySeq = FiloVector[String](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.toSeq should equal (orig)
    }

    it("should encode and decode back const string with NAs") {
      val orig = Seq(None, Some("apple"), Some("apple"), None)
      val buf = VectorBuilder.fromOptions(orig).toFiloBuffer
      checkVectorType(buf, WireFormat.VECTORTYPE_CONST, WireFormat.SUBTYPE_STRING)
      val binarySeq = FiloVector[String](buf)

      binarySeq.length should equal (orig.length)
      binarySeq.optionIterator.toSeq should equal (orig)
    }

    it("should read back strings in NA spots with default value") {
      val orig = Seq(None, Some("apple"), Some("banana"), None)
      val buf = VectorBuilder.fromOptions(orig).toFiloBuffer
      val binarySeq = FiloVector[String](buf)

      binarySeq(1) should equal ("apple")
      binarySeq(0) should equal ("")
    }
  }
}