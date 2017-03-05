package org.velvia.filo

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.Timestamp
import org.joda.time.{DateTime, DateTimeZone}

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
    checkVectorType(buf1, WireFormat.VECTORTYPE_BINSIMPLE, WireFormat.SUBTYPE_PRIMITIVE_NOMASK)

    val seq1a = Seq(0, 65432)
    val buf1a = VectorBuilder(seq1a).toFiloBuffer(DiffEncoding)
    checkVectorType(buf1, WireFormat.VECTORTYPE_BINSIMPLE, WireFormat.SUBTYPE_PRIMITIVE_NOMASK)

    // sequence contains longs within 2^32 but min and max far apart
    val seq2 = Seq(0L, 65432 * 65432L)
    val buf2 = VectorBuilder(seq2).toFiloBuffer(DiffEncoding)
    checkVectorType(buf1, WireFormat.VECTORTYPE_BINSIMPLE, WireFormat.SUBTYPE_PRIMITIVE_NOMASK)

    // sequence contains integers beyond 2^16
    // (such that max - min might wrap around)
    val seq3 = Seq(Int.MinValue, Int.MaxValue)
    val buf3 = VectorBuilder(seq3).toFiloBuffer(DiffEncoding)
    checkVectorType(buf1, WireFormat.VECTORTYPE_BINSIMPLE, WireFormat.SUBTYPE_PRIMITIVE_NOMASK)

    val seq4 = Seq(Short.MinValue.toInt, Short.MaxValue.toInt + 1)
    val buf4 = VectorBuilder(seq4).toFiloBuffer(DiffEncoding)
    checkVectorType(buf1, WireFormat.VECTORTYPE_BINSIMPLE, WireFormat.SUBTYPE_PRIMITIVE_NOMASK)

    // sequence contains longs beyond 2^32 min max
    val seq5 = Seq(Int.MinValue.toLong, Int.MaxValue.toLong + 1)
    val buf5 = VectorBuilder(seq5).toFiloBuffer(DiffEncoding)
    checkVectorType(buf1, WireFormat.VECTORTYPE_BINSIMPLE, WireFormat.SUBTYPE_PRIMITIVE_NOMASK)
  }

  it("should correctly diff encode int and long sequences that fit criteria") {
    val seq1 = Seq(500, 254, 257)
    val buf1 = VectorBuilder(seq1).toFiloBuffer(DiffEncoding)
    // TODO: disable parts of this test because diff encoding for IntBinaryVectors not done yet
    // buf1.capacity should equal (76)
    // checkVectorType(buf1, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_PRIMITIVE)
    val binarySeq1 = FiloVector[Int](buf1)

    binarySeq1.length should equal (seq1.length)
    binarySeq1.sum should equal (seq1.sum)

    val maxUInt = 65536L * 65536L
    val seq2 = Seq(maxUInt + 1, maxUInt, maxUInt + 255, maxUInt + 3)
    val buf2 = VectorBuilder(seq2).toFiloBuffer(DiffEncoding)
    buf2.capacity should equal (76)
    checkVectorType(buf2, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_PRIMITIVE)
    val binarySeq2 = FiloVector[Long](buf2)

    binarySeq2.length should equal (seq2.length)
    binarySeq2.sum should equal (seq2.sum)
  }

  val behindGmtZone = DateTimeZone.forOffsetHours(-5)
  val aheadGmtZone = DateTimeZone.forOffsetHours(4)
  val dt1 = new DateTime("2012-01-12T03:45Z", behindGmtZone)
  val ts1 = dt1.getMillis

  it("should correctly encode DateTime sequences in same time zone") {
    val seq1 = Seq(dt1, dt1.plusMillis(1), dt1.plusSeconds(2))
    val buf1 = VectorBuilder(seq1).toFiloBuffer
    checkVectorType(buf1, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_DATETIME)
    val binarySeq1 = FiloVector[DateTime](buf1)

    binarySeq1.length should equal (seq1.length)
    binarySeq1.toSeq should equal (seq1)

    val dt2 = dt1.withZone(aheadGmtZone)
    val seq2 = Seq(dt2, dt2.plusMinutes(1), dt2.minusSeconds(10))
    val buf2 = VectorBuilder(seq2).toFiloBuffer
    checkVectorType(buf2, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_DATETIME)
    val binarySeq2 = FiloVector[DateTime](buf2)

    binarySeq2.length should equal (seq2.length)
    binarySeq2.toSeq should equal (seq2)
  }

  it("should correctly encode DateTime sequences with mixed NAs") {
    val seq1 = Seq(None, None, Some(dt1), Some(dt1.plusMillis(150)))
    val buf1 = VectorBuilder.fromOptions(seq1).toFiloBuffer
    checkVectorType(buf1, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_DATETIME)
    val binarySeq1 = FiloVector[DateTime](buf1)

    binarySeq1.length should equal (seq1.length)
    binarySeq1.optionIterator.toSeq should equal (seq1)

    val seq2 = Seq(Some(dt1), None, Some(dt1.plusMillis(150)), None, None)
    val buf2 = VectorBuilder.fromOptions(seq2).toFiloBuffer
    checkVectorType(buf2, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_DATETIME)
    val binarySeq2 = FiloVector[DateTime](buf2)

    binarySeq2.length should equal (seq2.length)
    binarySeq2.optionIterator.toSeq should equal (seq2)
  }

  it("should correctly encode Timestamp sequences with mixed NAs") {
    val seq1 = Seq(None, Some(new Timestamp(ts1)), None, Some(new Timestamp(ts1 + 15000L)))
    val buf1 = VectorBuilder.fromOptions(seq1).toFiloBuffer
    checkVectorType(buf1, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_PRIMITIVE)
    val binarySeq1 = FiloVector[Timestamp](buf1)

    binarySeq1.length should equal (seq1.length)
    binarySeq1.optionIterator.toSeq should equal (seq1)
  }

  it("should correctly encode DateTime sequences with different time zones") {
    val dt2 = dt1.withZone(aheadGmtZone)
    val seq2 = Seq(dt1, dt2.plusMinutes(1), dt1.minusSeconds(5), dt2.minusSeconds(10))
    val buf2 = VectorBuilder(seq2).toFiloBuffer
    checkVectorType(buf2, WireFormat.VECTORTYPE_DIFF, WireFormat.SUBTYPE_DATETIME)
    val binarySeq2 = FiloVector[DateTime](buf2)

    binarySeq2.length should equal (seq2.length)
    binarySeq2.toSeq should equal (seq2)
  }
}
