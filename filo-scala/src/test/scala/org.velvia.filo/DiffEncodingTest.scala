package org.velvia.filo

import java.nio.{ByteBuffer, ByteOrder}

import org.scalatest.FunSpec
import org.scalatest.Matchers

class DiffEncodingTest extends FunSpec with Matchers {
  import BuilderEncoder.DiffEncoding
  import VectorReader._

  private def checkVectorType(bb: ByteBuffer, majorType: Int, minorType: Int): Unit = {
    val headerBytes = bb.getInt(0)
    WireFormat.majorVectorType(headerBytes) should equal (majorType)
    WireFormat.vectorSubType(headerBytes) should equal (minorType)
  }

  it("should not diff encode int and long sequences that could be simply encoded efficiently") {
    // sequence contains integers already within 256/2^16 etc boundaries
    val seq1 = Seq(0, 255)
    val buf1 = VectorBuilder(seq1).toFiloBuffer(DiffEncoding)
    checkVectorType(buf1, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_PRIMITIVE)

    val seq1a = Seq(0, 65432)
    val buf1a = VectorBuilder(seq1a).toFiloBuffer(DiffEncoding)
    checkVectorType(buf1a, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_PRIMITIVE)

    // sequence contains longs within 2^32 but min and max far apart
    val seq2 = Seq(0L, 65432 * 65432L)
    val buf2 = VectorBuilder(seq2).toFiloBuffer(DiffEncoding)
    checkVectorType(buf2, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_PRIMITIVE)

    // sequence contains integers beyond 2^16
    // (such that max - min might wrap around)
    val seq3 = Seq(Int.MinValue, Int.MaxValue)
    val buf3 = VectorBuilder(seq3).toFiloBuffer(DiffEncoding)
    checkVectorType(buf3, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_PRIMITIVE)

    val seq4 = Seq(Short.MinValue.toInt, Short.MaxValue.toInt + 1)
    val buf4 = VectorBuilder(seq4).toFiloBuffer(DiffEncoding)
    checkVectorType(buf4, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_PRIMITIVE)

    // sequence contains longs beyond 2^32 min max
    val seq5 = Seq(Int.MinValue.toLong, Int.MaxValue.toLong + 1)
    val buf5 = VectorBuilder(seq5).toFiloBuffer(DiffEncoding)
    checkVectorType(buf5, WireFormat.VECTORTYPE_SIMPLE, WireFormat.SUBTYPE_PRIMITIVE)
  }

  it("should correctly diff encode int and long sequences that fit criteria") {
    val seq1 = Seq(500, 254, 257)
    val buf1 = VectorBuilder(seq1).toFiloBuffer(DiffEncoding)
    buf1.capacity should equal (84)
    checkVectorType(buf1, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_PRIMITIVE)
    val binarySeq1 = FiloVector[Int](buf1)

    binarySeq1.length should equal (seq1.length)
    binarySeq1.sum should equal (seq1.sum)

    val maxUInt = 65536L * 65536L
    val seq2 = Seq(maxUInt + 1, maxUInt, maxUInt + 255, maxUInt + 3)
    val buf2 = VectorBuilder(seq2).toFiloBuffer(DiffEncoding)
    checkVectorType(buf2, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_PRIMITIVE)
    val binarySeq2 = FiloVector[Long](buf2)

    binarySeq2.length should equal (seq2.length)
    binarySeq2.sum should equal (seq2.sum)
  }
}
