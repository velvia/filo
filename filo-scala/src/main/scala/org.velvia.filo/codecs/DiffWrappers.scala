package org.velvia.filo.codecs

import org.joda.time.{DateTime, DateTimeZone}
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
  val baseReader = FastBufferReader(dpv.base)

  final def length: Int = _len

  final def foreach[B](fn: A => B): Unit = {
    if (maskType == MaskType.AllZeroes) {   // every value available!
      for { i <- 0 until length optimized } { fn(apply(i)) }
    } else {
      for { i <- 0 until length optimized } { if (isAvailable(i)) fn(apply(i)) }
    }
  }
}

/**
 * A FiloVector that represents DateTime's with a fixed single TimeZone
 * and a differentially encoded millis vector
 */
abstract class DiffDateTimeWrapperBase(ddtv: DiffDateTimeVector)
extends FiloVector[DateTime] with NaMaskAvailable {
  import TypedBufferReader._

  val naMask = ddtv.naMask
  val _len = ddtv.vars.len
  val millisBase: Long = ddtv.vars.baseMillis
  val millisReader = TypedBufferReader[Long](FastBufferReader(ddtv.millisAsByteBuffer),
                                             ddtv.millisInfo.nbits, ddtv.millisInfo.signed)

  final def length: Int = _len

  final def foreach[B](fn: DateTime => B): Unit = {
    if (maskType == MaskType.AllZeroes) {   // every value available!
      for { i <- 0 until length optimized } { fn(apply(i)) }
    } else {
      for { i <- 0 until length optimized } { if (isAvailable(i)) fn(apply(i)) }
    }
  }
}

class DiffDateTimeWrapper(ddtv: DiffDateTimeVector) extends DiffDateTimeWrapperBase(ddtv) {
  val zone = DateTimeZone.forOffsetMillis(ddtv.vars.baseTz * VectorBuilder.FifteenMinMillis)

  final def apply(i: Int): DateTime = new DateTime(millisBase + millisReader.read(i), zone)
}

/**
 * A variant of DiffDateTimeWrapper that can create a different time zone for each element
 */
class DiffDateTimeWithTZWrapper(ddtv: DiffDateTimeVector) extends DiffDateTimeWrapperBase(ddtv) {
  import TypedBufferReader._

  val tzBase: Byte = ddtv.vars.baseTz
  val tzReader = TypedBufferReader[Int](FastBufferReader(ddtv.tzAsByteBuffer),
                                        ddtv.tzInfo.nbits, ddtv.tzInfo.signed)

  final def apply(i: Int): DateTime = {
    val zone = DateTimeZone.forOffsetMillis((tzBase + tzReader.read(i)) * VectorBuilder.FifteenMinMillis)
    new DateTime(millisBase + millisReader.read(i), zone)
  }
}
