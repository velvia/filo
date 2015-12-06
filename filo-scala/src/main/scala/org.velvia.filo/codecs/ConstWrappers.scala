package org.velvia.filo.codecs

import scala.language.postfixOps
import scalaxy.loops._

import org.velvia.filo._
import org.velvia.filo.vector._

class ConstStringWrapper(csv: ConstStringVector) extends FiloVector[String] with NaMaskAvailable {
  val naMask = csv.naMask
  val _len = csv.len
  val _str = csv.str

  final def length: Int = _len

  final def apply(i: Int): String = _str

  final def foreach[B](fn: String => B): Unit = {
    for { i <- 0 until length optimized } { if (isAvailable(i)) fn(apply(i)) }
  }
}
