package org.velvia.filo

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.PropertyChecks

class ZeroCopyBinaryTest extends FunSpec with Matchers with PropertyChecks {
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

      // Calling equals to some other type should return false
      ZeroCopyUTF8String("dictionary") should not equal ("dictionary")
    }

    it("should compare random strings properly") {
      import java.lang.Integer.signum
      forAll { (strs: (String, String)) =>
        val nativeCmp = signum(strs._1.compare(strs._2))
        signum(ZeroCopyUTF8String(strs._1).compare(ZeroCopyUTF8String(strs._2))) should equal (nativeCmp)
      }
    }

    it("should get bytes back and convert back to instance, and compare equally") {
      val origUTF8Str = ZeroCopyUTF8String("dictionary")
      ZeroCopyUTF8String(origUTF8Str.bytes) should equal (origUTF8Str)
    }

    it("should generate same hashcode for same content") {
      ZeroCopyUTF8String("bobcat").hashCode should equal (ZeroCopyUTF8String("bobcat").hashCode)
      ZeroCopyUTF8String("bobcat").hashCode should not equal (ZeroCopyUTF8String("bob").hashCode)

      ZeroCopyUTF8String("bobcat").cachedHash64 should equal (ZeroCopyUTF8String("bobcat").cachedHash64)
      ZeroCopyUTF8String("bobcat").cachedHash64 should not equal (ZeroCopyUTF8String("bob").cachedHash64)
    }

    val str1 = ZeroCopyUTF8String("1234")
    val str2 = ZeroCopyUTF8String("一2三4")
    val str3 = ZeroCopyUTF8String("一二34")

    it("should get substring correctly") {
      str1.substring(3, 2) should equal (ZeroCopyUTF8String(""))
      str2.substring(0, 2) should equal (ZeroCopyUTF8String("一2"))
      str2.substring(1, 5) should equal (ZeroCopyUTF8String("2三4"))
      str3.substring(0, 3) should equal (ZeroCopyUTF8String("一二3"))
      str2.substring(1, 3) should equal (ZeroCopyUTF8String("2三"))
    }

    it("should startsWith and endsWith correctly") {
      str2.startsWith(ZeroCopyUTF8String("一2")) should equal (true)
      str2.startsWith(ZeroCopyUTF8String("2三")) should equal (false)
      str2.startsWith(str1) should equal (false)

      str2.endsWith(str3) should equal (false)
      str2.endsWith(ZeroCopyUTF8String("4")) should equal (true)
    }

    it("should check contains correctly") {
      str2.contains(ZeroCopyUTF8String("2三")) should equal (true)
      str2.contains(str1) should equal (false)
    }
  }
}