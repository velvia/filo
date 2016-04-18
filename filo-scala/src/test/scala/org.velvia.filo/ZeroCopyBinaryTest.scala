package org.velvia.filo

import org.scalatest.{FunSpec, Matchers}

class ZeroCopyBinaryTest extends FunSpec with Matchers {
  describe("ZeroCopyUTF8String") {
    it("should convert back and forth between regular strings") {
      ZeroCopyUTF8String("sheep").asNewString should equal ("sheep")
    }

    import ZeroCopyUTF8String._
    import Ordered._

    it("should compare two strings properly") {
      // Unequal lengths, equal prefix
      ZeroCopyUTF8String("boobeebob") should be > (ZeroCopyUTF8String("boobee"))

      // Equal lengths, different content
      // First comparison fights against int comparisons without proper byte ordering
      ZeroCopyUTF8String("aaab") should be < (ZeroCopyUTF8String("baaa"))
      ZeroCopyUTF8String("bobcat") should equal (ZeroCopyUTF8String("bobcat"))

      // Strings longer than 8 chars (in case comparison uses long compare)
      ZeroCopyUTF8String("dictionary") should be < (ZeroCopyUTF8String("pictionar"))
      ZeroCopyUTF8String("dictionary") should be > (ZeroCopyUTF8String("dictionaries"))
    }

    it("should generate same hashcode for same content") {
      ZeroCopyUTF8String("bobcat").hashCode should equal (ZeroCopyUTF8String("bobcat").hashCode)
      ZeroCopyUTF8String("bobcat").hashCode should not equal (ZeroCopyUTF8String("bob").hashCode)
    }
  }
}