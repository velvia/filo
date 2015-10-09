package org.velvia.filo.codecs

import scala.language.postfixOps
import scalaxy.loops._

import org.velvia.filo.{FiloVector, FastBufferReader}
import org.velvia.filo.vector._

object DictStringWrapper {
  // Used to represent no string value or NA.  Better than using null.
  val NoString = ""
}

abstract class DictStringWrapper(val dsv: DictStringVector) extends FiloVector[String] {
  import DictStringWrapper._

  private val _len = dsv.len
  val reader = FastBufferReader(dsv.codesAsByteBuffer())

  // To be mixed in depending on type of code vector
  def getCode(index: Int): Int

  // Cache the Strings so we only pay cost of deserializing each unique string once
  val strCache = Array.fill(dsv.dictionaryLength())(NoString)

  final private def dictString(code: Int): String = {
    val cacheValue = strCache(code)
    if (cacheValue == NoString) {
      val strFromDict = dsv.dictionary(code)
      strCache(code) = strFromDict
      strFromDict
    } else {
      cacheValue
    }
  }

  final def isAvailable(index: Int): Boolean = getCode(index) != 0

  final def apply(index: Int): String = dictString(getCode(index))

  final def length: Int = _len

  final def foreach[B](fn: String => B): Unit = {
    for { i <- 0 until length optimized } {
      val code = getCode(i)
      if (code != 0) fn(dictString(code))
    }
  }
}