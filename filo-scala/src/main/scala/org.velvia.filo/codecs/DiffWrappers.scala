package org.velvia.filo.codecs

import scala.language.postfixOps
import scalaxy.loops._

import org.velvia.filo._
import org.velvia.filo.vector._

abstract class DiffPrimitiveWrapper[A: TypedReaderProvider](dpv: DiffPrimitiveVector)
    extends FiloVector[A] with NaMaskAvailable {
  val naMask = dpv.naMask
  val info = dpv.info
  val _len = dpv.len
  val dataReader = TypedBufferReader[A](FastBufferReader(dpv.dataAsByteBuffer()),
                                        info.nbits, info.signed)
  val baseReader = TypedBufferReader[A](FastBufferReader(dpv.baseAsByteBuffer()),
                                        dpv.baseInfo.nbits, dpv.baseInfo.signed)
  val base = baseReader.read(0)

  final def length: Int = _len

  final def foreach[B](fn: A => B): Unit = {
    if (maskType == MaskType.AllZeroes) {   // every value available!
      for { i <- 0 until length optimized } { fn(apply(i)) }
    } else {
      for { i <- 0 until length optimized } { if (isAvailable(i)) fn(apply(i)) }
    }
  }
}
