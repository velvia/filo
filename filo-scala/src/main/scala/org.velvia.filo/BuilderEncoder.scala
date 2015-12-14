package org.velvia.filo

import java.nio.ByteBuffer
import scala.reflect.ClassTag

import org.velvia.filo.codecs._

/**
 * Type class for encoding a VectorBuilder to queryable binary Filo format
 */
trait BuilderEncoder[A] {
  def encodeInner(builder: VectorBuilder[A], hint: BuilderEncoder.EncodingHint): ByteBuffer
  def encode(builder: VectorBuilderBase, hint: BuilderEncoder.EncodingHint): ByteBuffer = {
    if (builder.isAllNA && builder.length <= WireFormat.MaxEmptyVectorLen) {
      SimpleEncoders.toEmptyVector(builder.length)
    } else {
      encodeInner(builder.asInstanceOf[VectorBuilder[A]], hint)
    }
  }
}

trait MinMaxEncoder[A] {
  def minMaxZero(builder: VectorBuilder[A]): (A, A, A) = {
    val minMaxBuilder = builder.asInstanceOf[MinMaxVectorBuilder[A]]
    (minMaxBuilder.min, minMaxBuilder.max, minMaxBuilder.zero)
  }
}

abstract class IntegralEncoder[A: PrimitiveDataVectBuilder] extends BuilderEncoder[A] with MinMaxEncoder[A] {
  val bufBuilder = implicitly[PrimitiveDataVectBuilder[A]]
  def encodeInner(builder: VectorBuilder[A], hint: BuilderEncoder.EncodingHint): ByteBuffer = {
    val (min, max, zero) = minMaxZero(builder)
    if (min == max) {
      ConstEncoders.toPrimitiveVector(builder.data, builder.naMask.result, min, max)
    } else if ((hint == BuilderEncoder.AutoDetect || hint == BuilderEncoder.DiffEncoding) &&
               bufBuilder.shouldBuildDeltas(min, max)) {
      DiffEncoders.toPrimitiveVector(builder.data, builder.naMask.result, min, max)
    } else {
      SimpleEncoders.toPrimitiveVector(builder.data, builder.naMask.result, min, max)
    }
  }
}

abstract class FloatDoubleEncoder[A: PrimitiveDataVectBuilder] extends
BuilderEncoder[A] with MinMaxEncoder[A] {
  def encodeInner(builder: VectorBuilder[A], hint: BuilderEncoder.EncodingHint): ByteBuffer = {
    val (min, max, _) = minMaxZero(builder)
    if (min == max) {
      ConstEncoders.toPrimitiveVector(builder.data, builder.naMask.result, min, max)
    } else {
      SimpleEncoders.toPrimitiveVector(builder.data, builder.naMask.result, min, max)
    }
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
  case object DiffEncoding extends EncodingHint

  import AutoIntegralDVBuilders._
  implicit object BoolEncoder extends IntegralEncoder[Boolean]
  implicit object IntEncoder extends IntegralEncoder[Int]
  implicit object LongEncoder extends IntegralEncoder[Long]

  import FPBuilders._
  implicit object DoubleEncoder extends FloatDoubleEncoder[Double]
  implicit object FloatEncoder extends FloatDoubleEncoder[Float]

  implicit object StringEncoder extends BuilderEncoder[String] {
    def encodeInner(builder: VectorBuilder[String], hint: EncodingHint): ByteBuffer = {
      val useDictEncoding = hint match {
        case DictionaryEncoding => true
        case SimpleEncoding     => false
        case x: Any             => builder match {
          case sb: StringVectorBuilder =>
            // If the string cardinality is below say half of # of elements
            // then definitely worth it to do dictionary encoding.
            // Empty/missing elements do not count towards cardinality, so columns with
            // many NA values will get dict encoded, which saves space
            sb.stringSet.size <= (sb.data.size / 2)
          // case x: Any =>  // Someone used something other than our own builder. Oh well. TODO: log
          //   false
          // NOTE: above is commented out for now because VectorBuilder is sealed. May change in future.
        }
      }
      (useDictEncoding, builder) match {
        case (_, sb: StringVectorBuilder) if sb.stringSet.size == 1 =>
          ConstEncoders.toStringVector(sb.stringSet.head, sb.data.length, sb.naMask.result)
        case (true, sb: StringVectorBuilder) =>
          DictEncodingEncoders.toStringVector(sb.data, sb.naMask.result, sb.stringSet)
        case x: Any =>
          SimpleEncoders.toStringVector(builder.data, builder.naMask.result)
      }
    }
  }
}

