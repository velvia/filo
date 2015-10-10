package org.velvia.filo

import com.google.flatbuffers.FlatBufferBuilder
import scala.collection.mutable.BitSet
import scala.language.postfixOps
import scalaxy.loops._

/**
 * A trait to build the smallest space fitting data vector possible given numeric
 * inputs.  Generally designed for Integer types; would also work for BigInteger etc.
 */
trait PrimitiveDataVectBuilder[A] {
  // Returns (offset, nbits)
  def build(fbb: FlatBufferBuilder, data: Seq[A], min: A, max: A): (Int, Int)
}

/**
 * Builders for efficient *unsigned* representations. That means for example if an Int
 * column has values between 0 and 255, it could be represented as a byte vector taking
 * up 8 bits per value.  Thus all values assumed to be between 0 and maxValue.
 */
object PrimitiveUnsignedBuilders {
  import Utils._

  implicit object BoolDataVectBuilder extends PrimitiveDataVectBuilder[Boolean] {
    def build(fbb: FlatBufferBuilder, data: Seq[Boolean], min: Boolean, max: Boolean): (Int, Int) = {
      // TODO: handle case where all booleans are true or false
      val bitset = new BitSet
      for { i <- 0 until data.length optimized } {
        if (data(i)) bitset += i
      }
      val mask = makeBitMask(bitset, data.length)
      longVect(fbb, mask.size, mask.reverseIterator)
    }
  }

  implicit object ShortDataVectBuilder extends PrimitiveDataVectBuilder[Short] {
    def build(fbb: FlatBufferBuilder, data: Seq[Short], min: Short, max: Short): (Int, Int) = {
      // TODO: Add support for stuff below byte level
      if (min >= 0 && max < 256) { byteVect(fbb, data.size, data.reverseIterator.map(_.toByte)) }
      else                       { shortVect(fbb, data.size, data.reverseIterator) }
    }
  }

  implicit object IntDataVectBuilder extends PrimitiveDataVectBuilder[Int] {
    def build(fbb: FlatBufferBuilder, data: Seq[Int], min: Int, max: Int): (Int, Int) = {
      // TODO: Add support for stuff below byte level
      if (min >= 0 && max < 256) {
        byteVect(fbb, data.size, data.reverseIterator.map(_.toByte))
      } else if (min >= 0 && max < 65536) {
        shortVect(fbb, data.size, data.reverseIterator.map(_.toShort))
      } else {
        intVect(fbb, data.size, data.reverseIterator)
      }
    }
  }

  implicit object LongDataVectBuilder extends PrimitiveDataVectBuilder[Long] {
    val maxUInt = 65536L * 65536L
    def build(fbb: FlatBufferBuilder, data: Seq[Long], min: Long, max: Long): (Int, Int) = {
      if (min >= 0L && max < 256L) {
        byteVect(fbb, data.size, data.reverseIterator.map(_.toByte))
      } else if (min >= 0L && max < 65536L) {
        shortVect(fbb, data.size, data.reverseIterator.map(_.toShort))
      } else if (min >= 0L && max < maxUInt) {
        intVect(fbb, data.size, data.reverseIterator.map(_.toInt))
      } else {
        longVect(fbb, data.size, data.reverseIterator)
      }
    }
  }
}

object FPBuilders {
  import Utils._

  implicit object DoubleDataVectBuilder extends PrimitiveDataVectBuilder[Double] {
    def build(fbb: FlatBufferBuilder, data: Seq[Double], min: Double, max: Double): (Int, Int) = {
      doubleVect(fbb, data.size, data.reverseIterator)
    }
  }

  implicit object FloatDataVectBuilder extends PrimitiveDataVectBuilder[Float] {
    def build(fbb: FlatBufferBuilder, data: Seq[Float], min: Float, max: Float): (Int, Int) = {
      floatVect(fbb, data.size, data.reverseIterator)
    }
  }
}