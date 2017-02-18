package org.velvia.filo.vectors

import org.scalatest.{FunSpec, Matchers}
import org.velvia.filo.{FiloVector, GrowableVector, ZeroCopyUTF8String}

class UTF8VectorTest extends FunSpec with Matchers {
  describe("UTF8Vector") {
    it("should be able to append all NAs") {
      val utf8vect = UTF8Vector.appendingVector(5, 1024)
      utf8vect.addNA()
      utf8vect.addNA()
      utf8vect.length should equal (2)
      utf8vect.frozenSize should equal (12)
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
      utf8vect.numBytes should equal (36)
      utf8vect.frozenSize should equal (36)
    }

    it("should be able to calculate min, max # bytes for all elements") {
      val utf8vect = UTF8Vector.appendingVector(5, 1024)
      Seq("apple", "zoe", "bananas").foreach(s => utf8vect.addData(ZeroCopyUTF8String(s)))
      utf8vect.addNA()   // NA or empty string should not affect min/max len
      val inner = utf8vect.asInstanceOf[GrowableVector[_]].inner.asInstanceOf[UTF8AppendableVector]
      inner.minMaxStrLen should equal ((3, 7))

      val utf8vect2 = UTF8Vector.appendingVector(5, 1024)
      Seq("apple", "", "bananas").foreach(s => utf8vect2.addData(ZeroCopyUTF8String(s)))
      utf8vect2.noNAs should equal (true)
      val inner2 = utf8vect2.asInstanceOf[GrowableVector[_]].inner.asInstanceOf[UTF8AppendableVector]
      inner2.minMaxStrLen should equal ((0, 7))
    }

    it("should be able to freeze and minimize bytes used") {
      val strs = Seq("apple", "zoe", "bananas").map(ZeroCopyUTF8String.apply)
      val utf8vect = UTF8Vector.appendingVector(10, 1024)
      strs.foreach(utf8vect.addData)
      utf8vect.length should equal (3)
      utf8vect.noNAs should equal (true)
      utf8vect.frozenSize should equal (4 + 12 + 5 + 3 + 7)

      val frozen = utf8vect.freeze()
      frozen.length should equal (3)
      frozen.toSeq should equal (strs)
      frozen.numBytes should equal (4 + 12 + 5 + 3 + 7)
    }

    it("should be able toFiloBuffer() and parse back with FiloVector") {
      val strs = Seq("apple", "zoe", "bananas").map(ZeroCopyUTF8String.apply)
      val utf8vect = UTF8Vector.appendingVector(5, 1024)
      strs.foreach(utf8vect.addData)
      val buffer = utf8vect.toFiloBuffer()
      val readVect = FiloVector[ZeroCopyUTF8String](buffer)
      readVect.toSeq should equal (strs)
    }

    it("should be able to grow the UTF8Vector if run out of initial maxBytes") {
      // Purposefully test when offsets grow beyond 32k
      val strs = (1 to 10000).map(i => ZeroCopyUTF8String("string" + i))
      val utf8vect = UTF8Vector.appendingVector(50, 16384)
      strs.foreach(utf8vect.addData)
      val buffer = utf8vect.toFiloBuffer()
      val readVect = FiloVector[ZeroCopyUTF8String](buffer)
      readVect.toSeq should equal (strs)
    }
  }

  describe("DictUTF8Vector") {
    it("shouldMakeDict when source strings are mostly repeated") {
      val strs = Seq("apple", "zoe", "grape").permutations.flatten.toList.map(ZeroCopyUTF8String.apply)
      val dictInfo = DictUTF8Vector.shouldMakeDict(strs, samplingRate=0.5)
      dictInfo should be ('defined)
      dictInfo.get.codeMap.size should equal (3)
      dictInfo.get.dictStrings.length should equal (4)
    }

    it("should not makeDict when source strings are all unique") {
      val strs = (0 to 9).map(_.toString).map(ZeroCopyUTF8String.apply)
      val dictInfo = DictUTF8Vector.shouldMakeDict(strs)
      dictInfo should be ('empty)
    }

    it("should optimize UTF8Vector to DictVector with NAs and read it back") {
      val strs = ZeroCopyUTF8String.NA +:
                 Seq("apple", "zoe", "grape").permutations.flatten.toList.map(ZeroCopyUTF8String.apply)
      val buffer = UTF8Vector.writeOptimizedBuffer(strs, samplingRate=0.5)
      val reader = FiloVector[ZeroCopyUTF8String](buffer)
      reader shouldBe a [DictUTF8Vector]
      reader.length should equal (strs.length)
      reader.toSeq should equal (strs.drop(1))
      reader.isAvailable(0) should be (false)
      reader(0) should equal (ZeroCopyUTF8String(""))
    }

    // Negative byte values might not get converted to ints properly, leading
    // to an ArrayOutOfBoundsException.
    it("should ensure proper conversion when there are 128-255 unique strings") {
      val orig = (0 to 130).map(_.toString).map(ZeroCopyUTF8String.apply)
      val buffer = UTF8Vector.writeOptimizedBuffer(orig, spaceThreshold = 1.1)
      val binarySeq = FiloVector[ZeroCopyUTF8String](buffer)
      binarySeq shouldBe a [DictUTF8Vector]

      binarySeq.length should equal (orig.length)
      binarySeq.toSeq should equal (orig)
    }
  }
}