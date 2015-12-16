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
  /**
   * Populates the FlatBuffers binary data vector based on the sequence of elements and predetermined
   * mins and maxes.  May determine the smallest representation automatically.
   * @return ((offset, nbits), signed)
   */
  def build(fbb: FlatBufferBuilder, data: Seq[A], min: A, max: A): ((Int, Int), Boolean)

  def shouldBuildDeltas(min: A, max: A): Boolean = false

  /**
   * Like build, but works on deltas of the sequence from the min value (the "base").
   * This often allows for smaller representations.
   */
  def buildDeltas(fbb: FlatBufferBuilder, data: Seq[A], min: A, max: A): ((Int, Int), Boolean) = ???

  // Converts the primitive A to a Long.  If shouldBuildDeltas might be true, this should be defined.
  def toLong(item: A): Long = ???
}

/**
 * Builders for efficient integral data vect representations. Automatically determines the smallest
 * representation possible - for example if all numbers fit in signed 8 bits, then use that.
 */
object AutoIntegralDVBuilders {
  import Utils._

  implicit object BoolDataVectBuilder extends PrimitiveDataVectBuilder[Boolean] {
    def build(fbb: FlatBufferBuilder, data: Seq[Boolean], min: Boolean, max: Boolean):
        ((Int, Int), Boolean) = {
      // TODO: handle case where all booleans are true or false
      val bitset = new BitSet
      for { i <- 0 until data.length optimized } {
        if (data(i)) bitset += i
      }
      val mask = makeBitMask(bitset, data.length)
      (longVect(fbb, mask.size, mask.reverseIterator), false)
    }
  }

  implicit object ShortDataVectBuilder extends PrimitiveDataVectBuilder[Short] {
    def build(fbb: FlatBufferBuilder, data: Seq[Short], min: Short, max: Short): ((Int, Int), Boolean) = {
      // TODO: Add support for stuff below byte level
      if (min >= Byte.MinValue.toShort && max <= Byte.MaxValue.toShort) {
        (byteVect(fbb, data.size, data.reverseIterator.map(_.toByte)), true)
      } else if (min >= 0 && max < 256) {
        (byteVect(fbb, data.size, data.reverseIterator.map(_.toByte)), false)
      } else {
        (shortVect(fbb, data.size, data.reverseIterator), true)
      }
    }

    override def shouldBuildDeltas(min: Short, max: Short): Boolean = {
      val diff = max - min
      diff > 0 &&
      (if (min >= 0) {
        (diff < 256 && max >= 256)
      } else {
        (diff < 256 && (min < Byte.MinValue || max > Byte.MaxValue))
      })
    }

    override def toLong(item: Short): Long = item.toLong

    override def buildDeltas(fbb: FlatBufferBuilder, data: Seq[Short], min: Short, max: Short):
        ((Int, Int), Boolean) = build(fbb, data.map(x => (x - min).toShort), 0, (max - min).toShort)
  }

  implicit object IntDataVectBuilder extends PrimitiveDataVectBuilder[Int] {
    def build(fbb: FlatBufferBuilder, data: Seq[Int], min: Int, max: Int): ((Int, Int), Boolean) = {
      // TODO: Add support for stuff below byte level
      if (min >= Byte.MinValue && max <= Byte.MaxValue) {
        (byteVect(fbb, data.size, data.reverseIterator.map(_.toByte)), true)
      } else if (min >= 0 && max < 256) {
        (byteVect(fbb, data.size, data.reverseIterator.map(_.toByte)), false)
      } else if (min >= Short.MinValue && max <= Short.MaxValue) {
        (shortVect(fbb, data.size, data.reverseIterator.map(_.toShort)), true)
      } else if (min >= 0 && max < 65536) {
        (shortVect(fbb, data.size, data.reverseIterator.map(_.toShort)), false)
      } else {
        (intVect(fbb, data.size, data.reverseIterator), true)
      }
    }

    override def shouldBuildDeltas(min: Int, max: Int): Boolean = {
      val diff = max - min
      diff > 0 &&
      (if (min >= 0) {
        (diff < 256 && max >= 256) ||
        (diff < 65536 && max >= 65536)
      } else {
        (diff < 256 && (min < Byte.MinValue || max > Byte.MaxValue)) ||
        (diff < 65536 && (min < Short.MinValue || max > Short.MaxValue))
      })
    }

    override def toLong(item: Int): Long = item.toLong

    override def buildDeltas(fbb: FlatBufferBuilder, data: Seq[Int], min: Int, max: Int):
        ((Int, Int), Boolean) = build(fbb, data.map(_ - min), 0, max - min)
  }

  implicit object LongDataVectBuilder extends PrimitiveDataVectBuilder[Long] {
    val maxUInt = 65536L * 65536L
    def build(fbb: FlatBufferBuilder, data: Seq[Long], min: Long, max: Long): ((Int, Int), Boolean) = {
      if (min >= Byte.MinValue && max <= Byte.MaxValue) {
        (byteVect(fbb, data.size, data.reverseIterator.map(_.toByte)), true)
      } else if (min >= 0L && max < 256L) {
        (byteVect(fbb, data.size, data.reverseIterator.map(_.toByte)), false)
      } else if (min >= Short.MinValue && max <= Short.MaxValue) {
        (shortVect(fbb, data.size, data.reverseIterator.map(_.toShort)), true)
      } else if (min >= 0L && max < 65536L) {
        (shortVect(fbb, data.size, data.reverseIterator.map(_.toShort)), false)
      } else if (min >= Int.MinValue && max <= Int.MaxValue) {
        (intVect(fbb, data.size, data.reverseIterator.map(_.toInt)), true)
      } else if (min >= 0L && max < maxUInt) {
        (intVect(fbb, data.size, data.reverseIterator.map(_.toInt)), false)
      } else {
        (longVect(fbb, data.size, data.reverseIterator), false)
      }
    }

    override def shouldBuildDeltas(min: Long, max: Long): Boolean = {
      val diff = max - min
      diff > 0 &&
      (if (min >= 0) {
        (diff < 256L && max >= 256L) ||
        (diff < 65536L && max >= 65536L) ||
        (diff < maxUInt && max >= maxUInt)
      } else {
        (diff < 256L && (min < Byte.MinValue || max > Byte.MaxValue)) ||
        (diff < 65536L && (min < Short.MinValue || max > Short.MaxValue)) ||
        (diff < maxUInt && (min < Int.MinValue || max > Int.MaxValue))
      })
    }

    override def toLong(item: Long): Long = item

    override def buildDeltas(fbb: FlatBufferBuilder, data: Seq[Long], min: Long, max: Long):
        ((Int, Int), Boolean) = build(fbb, data.map(_ - min), 0, max - min)
  }
}

object FPBuilders {
  import Utils._

  implicit object DoubleDataVectBuilder extends PrimitiveDataVectBuilder[Double] {
    def build(fbb: FlatBufferBuilder, data: Seq[Double], min: Double, max: Double): ((Int, Int), Boolean) = {
      (doubleVect(fbb, data.size, data.reverseIterator), false)
    }
  }

  implicit object FloatDataVectBuilder extends PrimitiveDataVectBuilder[Float] {
    def build(fbb: FlatBufferBuilder, data: Seq[Float], min: Float, max: Float): ((Int, Int), Boolean) = {
      (floatVect(fbb, data.size, data.reverseIterator), false)
    }
  }
}