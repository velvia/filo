package org.velvia.filo.vectors

import org.scalatest.{FunSpec, Matchers}
import org.velvia.filo.{FiloVector, ZeroCopyUTF8String}

class UTF8VectorTest extends FunSpec with Matchers {
  describe("UTF8Vector") {
    it("should be able to append all NAs") {
      val utf8vect = UTF8Vector.appendingVector(5, 1024)
      utf8vect.addNA()
      utf8vect.addNA()
      utf8vect.length should equal (2)
      utf8vect.isAvailable(0) should equal (false)
      utf8vect.isAvailable(1) should equal (false)

      // should be able to apply read back NA values and get back empty string
      utf8vect(0).length should equal (0)
      utf8vect.isAllNA should equal (true)
      utf8vect.noNAs should equal (false)
    }

    it("should be able to append mix of strings and NAs") {
      val strs = Seq("apple", "", "Charlie").map(ZeroCopyUTF8String.apply)
      val utf8vect = UTF8Vector.appendingVector(5, 1024)
      utf8vect.addNA()
      strs.foreach(utf8vect.addData)
      utf8vect.addNA()

      utf8vect.length should equal (5)
      utf8vect.toSeq should equal (strs)
      utf8vect.isAllNA should equal (false)
      utf8vect.noNAs should equal (false)
      utf8vect.isAvailable(0) should equal (false)
      utf8vect.isAvailable(1) should equal (true)
      utf8vect.isAvailable(2) should equal (true)
      utf8vect.numBytes should equal (40)
    }

    it("should be able to calculate min, max # bytes for all elements") {
      val utf8vect = UTF8Vector.appendingVector(5, 1024)
      Seq("apple", "zoe", "bananas").foreach(s => utf8vect.addData(ZeroCopyUTF8String(s)))
      utf8vect.addNA()   // NA or empty string should not affect min/max len
      utf8vect.minMaxStrLen should equal ((3, 7))

      val utf8vect2 = UTF8Vector.appendingVector(5, 1024)
      Seq("apple", "", "bananas").foreach(s => utf8vect2.addData(ZeroCopyUTF8String(s)))
      utf8vect2.noNAs should equal (true)
      utf8vect2.minMaxStrLen should equal ((0, 7))
    }

    it("should be able to freeze and minimize bytes used") {
      val strs = Seq("apple", "zoe", "bananas").map(ZeroCopyUTF8String.apply)
      val utf8vect = UTF8Vector.appendingVector(5, 1024)
      strs.foreach(utf8vect.addData)
      utf8vect.length should equal (3)
      utf8vect.noNAs should equal (true)
      utf8vect.numBytes should equal (4 + 20 + 8 + 4 + 8)

      val frozen = utf8vect.freeze()
      frozen.length should equal (3)
      frozen.toSeq should equal (strs)
      frozen.numBytes should equal (4 + 12 + 8 + 4 + 8)
    }

    it("should be able toFiloBuffer() and parse back with FiloVector") {
      val strs = Seq("apple", "zoe", "bananas").map(ZeroCopyUTF8String.apply)
      val utf8vect = UTF8Vector.appendingVector(5, 1024)
      strs.foreach(utf8vect.addData)
      val buffer = utf8vect.toFiloBuffer()
      val readVect = FiloVector[ZeroCopyUTF8String](buffer)
      readVect.toSeq should equal (strs)
    }
  }
}