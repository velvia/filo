package org.velvia.filo

import java.nio.ByteBuffer
import scala.reflect.ClassTag

import org.velvia.filo.codecs._

/**
 * Type class for encoding a ColumnBuilder to queryable binary Filo format
 */
trait BuilderEncoder[A] {
  def encodeInner(builder: ColumnBuilder[A], hint: BuilderEncoder.EncodingHint): ByteBuffer
  def encode(builder: ColumnBuilderBase, hint: BuilderEncoder.EncodingHint): ByteBuffer = {
    if (builder.isAllNA && builder.length <= WireFormat.MaxEmptyVectorLen) {
      SimpleEncoders.toEmptyVector(builder.length)
    } else {
      encodeInner(builder.asInstanceOf[ColumnBuilder[A]], hint)
    }
  }
}

trait MinMaxEncoder[A] {
  def minMaxZero(builder: ColumnBuilder[A]): (A, A, A) = {
    val minMaxBuilder = builder.asInstanceOf[MinMaxColumnBuilder[A]]
    (minMaxBuilder.min, minMaxBuilder.max, minMaxBuilder.zero)
  }
}

trait IntegralEncoder[A] extends BuilderEncoder[A] with MinMaxEncoder[A] {
  def unsignedBuilder: PrimitiveDataVectBuilder[A]
  def encodeInner(builder: ColumnBuilder[A], hint: BuilderEncoder.EncodingHint): ByteBuffer = {
    val (min, max, zero) = minMaxZero(builder)
    // TODO: use signed typeclasses once they are done
    SimpleEncoders.toPrimitiveVector(builder.data, builder.naMask.result,
                                     min, max, false)(unsignedBuilder)
  }
}

/**
 * Classes to encode a Builder to a queryable binary Filo format.
 * Methods automatically detect the best encoding method to use, but hints are available
 * to pass to the methods.
 *
 * To extend the encoder for additional base types A, implement a type class BuilderEncoder[A].
 */
object BuilderEncoder {
  sealed trait EncodingHint
  case object AutoDetect extends EncodingHint
  case object SimpleEncoding extends EncodingHint
  case object DictionaryEncoding extends EncodingHint

  implicit object BoolEncoder extends IntegralEncoder[Boolean] {
    val unsignedBuilder = PrimitiveUnsignedBuilders.BoolDataVectBuilder
  }

  implicit object IntEncoder extends IntegralEncoder[Int] {
    val unsignedBuilder = PrimitiveUnsignedBuilders.IntDataVectBuilder
  }

  implicit object LongEncoder extends IntegralEncoder[Long] {
    val unsignedBuilder = PrimitiveUnsignedBuilders.LongDataVectBuilder
  }

  implicit object DoubleEncoder extends BuilderEncoder[Double] with MinMaxEncoder[Double] {
    def encodeInner(builder: ColumnBuilder[Double], hint: EncodingHint): ByteBuffer = {
      import FPBuilders._
      val (min, max, _) = minMaxZero(builder)
      SimpleEncoders.toPrimitiveVector(builder.data, builder.naMask.result,
                                       min, max, false)
    }
  }

  implicit object FloatEncoder extends BuilderEncoder[Float] with MinMaxEncoder[Float] {
    def encodeInner(builder: ColumnBuilder[Float], hint: EncodingHint): ByteBuffer = {
      import FPBuilders._
      val (min, max, _) = minMaxZero(builder)
      SimpleEncoders.toPrimitiveVector(builder.data, builder.naMask.result,
                                       min, max, false)
    }
  }

  implicit object StringEncoder extends BuilderEncoder[String] {
    def encodeInner(builder: ColumnBuilder[String], hint: EncodingHint): ByteBuffer = {
      val useDictEncoding = hint match {
        case DictionaryEncoding => true
        case SimpleEncoding     => false
        case x: Any             => builder match {
          case sb: StringColumnBuilder =>
            // If the string cardinality is below say half of # of elements
            // then definitely worth it to do dictionary encoding.
            // Empty/missing elements do not count towards cardinality, so columns with
            // many NA values will get dict encoded, which saves space
            sb.stringSet.size <= (sb.data.size / 2)
          // case x: Any =>  // Someone used something other than our own builder. Oh well. TODO: log
          //   false
          // NOTE: above is commented out for now because ColumnBuilder is sealed. May change in future.
        }
      }
      (useDictEncoding, builder) match {
        case (true, sb: StringColumnBuilder) =>
          DictEncodingEncoders.toStringVector(sb.data, sb.naMask.result, sb.stringSet)
        case x: Any =>
          SimpleEncoders.toStringVector(builder.data, builder.naMask.result)
      }
    }
  }
}

