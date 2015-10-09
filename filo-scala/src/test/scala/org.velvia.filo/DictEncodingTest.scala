package org.velvia.filo

import org.scalatest.FunSpec
import org.scalatest.Matchers

class DictEncodingTest extends FunSpec with Matchers {
  import BuilderEncoder.DictionaryEncoding
  import VectorReader._

  it("should encode and decode back an empty Seq") {
    val buf = VectorBuilder(Seq[String]()).toFiloBuffer(DictionaryEncoding)
    val binarySeq = FiloVector[String](buf)

    binarySeq.length should equal (0)
  }

  it("should encode and decode back a Seq[String]") {
    val orig = Seq("apple", "banana")
    val buf = VectorBuilder(orig).toFiloBuffer(DictionaryEncoding)
    val binarySeq = FiloVector[String](buf)

    binarySeq.length should equal (orig.length)
    binarySeq.toSeq should equal (orig)
  }

  it("should encode and decode back a Seq[Option[String]]") {
    val orig = Seq(Some("apple"), None, Some("banana"))
    val buf = VectorBuilder.fromOptions(orig).toFiloBuffer(DictionaryEncoding)
    val binarySeq = FiloVector[String](buf)

    binarySeq.length should equal (orig.length)
    binarySeq.toSeq should equal (Seq("apple", "banana"))
    binarySeq.optionIterator.toSeq should equal (orig)
  }

  it("should encode and decode back a sequence starting with NAs") {
    val orig = Seq(None, None, None, Some("apple"), Some("banana"))
    val buf = VectorBuilder.fromOptions(orig).toFiloBuffer(DictionaryEncoding)
    val binarySeq = FiloVector[String](buf)

    binarySeq.length should equal (orig.length)
    binarySeq.toSeq should equal (Seq("apple", "banana"))
    binarySeq.optionIterator.toSeq should equal (orig)
  }

  // Negative byte values might not get converted to ints properly, leading
  // to an ArrayOutOfBoundsException.
  it("should ensure proper conversion when there are 128-255 unique strings") {
    val orig = (0 to 130).map(_.toString).toSeq
    val buf = VectorBuilder(orig).toFiloBuffer(DictionaryEncoding)
    val binarySeq = FiloVector[String](buf)

    binarySeq.length should equal (orig.length)
    binarySeq.toSeq should equal (orig)
  }
}