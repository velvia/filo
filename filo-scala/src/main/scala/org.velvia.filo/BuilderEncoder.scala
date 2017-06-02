package org.velvia.filo

import java.nio.ByteBuffer
import java.sql.Timestamp
import org.joda.time.DateTime
import scala.reflect.ClassTag
import scalaxy.loops._

import org.velvia.filo.codecs._

/**
 * Type class for encoding a VectorBuilder to queryable binary Filo format
 */
trait BuilderEncoder[A] {
  def encodeInner(builder: VectorBuilderBase, hint: BuilderEncoder.EncodingHint): ByteBuffer
  def encode(builder: VectorBuilderBase, hint: BuilderEncoder.EncodingHint): ByteBuffer = {
    if (builder.isAllNA && builder.length <= WireFormat.MaxEmptyVectorLen) {
      SimpleEncoders.toEmptyVector(builder.length)
    } else {
      encodeInner(builder.asInstanceOf[VectorBuilderBase], hint)
    }
  }
}

trait MinMaxEncoder[A] {
  def minMaxZero(builder: VectorBuilderBase): (A, A, A) = {
    val minMaxBuilder = builder.asInstanceOf[MinMaxVectorBuilder[A]]
    (minMaxBuilder.min, minMaxBuilder.max, minMaxBuilder.zero)
  }
}

trait SingleMethodEncoder[A] extends BuilderEncoder[A] {
  val encodingPF: BuilderEncoder.EncodingPF
  val default: BuilderEncoder.EncodingPF = {
    case x: Any                   =>
      throw new RuntimeException("Unsupported VectorBuilder")
  }

  def encodeInner(builder: VectorBuilderBase, hint: BuilderEncoder.EncodingHint): ByteBuffer = {
    (encodingPF orElse default)((builder, hint))
  }
}

abstract class IntegralEncoder[A: PrimitiveDataVectBuilder] extends BuilderEncoder[A] with MinMaxEncoder[A] {
  val bufBuilder = implicitly[PrimitiveDataVectBuilder[A]]
  def encodeInner(vBuilder: VectorBuilderBase, hint: BuilderEncoder.EncodingHint): ByteBuffer = {
    val (min, max, zero) = minMaxZero(vBuilder)
    val builder = vBuilder.asInstanceOf[VectorBuilder[A]]
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
  def encodeInner(vBuilder: VectorBuilderBase, hint: BuilderEncoder.EncodingHint): ByteBuffer = {
    val (min, max, _) = minMaxZero(vBuilder)
    val builder = vBuilder.asInstanceOf[VectorBuilder[A]]
    if (min == max) {
      ConstEncoders.toPrimitiveVector(builder.data, builder.naMask.result, min, max)
    } else {
      vBuilder match {
        case d: DoubleVectorBuilder if d.useIntVector =>
          val intVect = vectors.IntBinaryVector.appendingVector(d.length)
          for { i <- 0 until d.length optimized } {
            if (d.naMask.contains(i)) intVect.addNA() else intVect.addData(d.data(i).toInt)
          }
          intVect.optimize().toFiloBuffer
        case o: Any =>
          SimpleEncoders.toPrimitiveVector(builder.data, builder.naMask.result, min, max)
      }
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
  final case class AutoDictString(spaceThreshold: Double = 0.6, samplingRate: Double = 0.3) extends EncodingHint

  type EncodingPF = PartialFunction[(VectorBuilderBase, EncodingHint), ByteBuffer]

  import AutoIntegralDVBuilders._
  implicit object BoolEncoder extends IntegralEncoder[Boolean]
  implicit object IntEncoder extends IntegralEncoder[Int]
  implicit object LongEncoder extends IntegralEncoder[Long]

  import FPBuilders._
  implicit object DoubleEncoder extends FloatDoubleEncoder[Double]
  implicit object FloatEncoder extends FloatDoubleEncoder[Float]

  implicit object StringEncoder extends BuilderEncoder[String] {
    def encodeInner(builder: VectorBuilderBase, hint: EncodingHint): ByteBuffer = {
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
          case x: Any =>  // Someone used something other than our own builder. Oh well. TODO: log
            false
        }
      }
      (useDictEncoding, builder) match {
        case (_, sb: StringVectorBuilder) if sb.stringSet.size == 1 =>
          ConstEncoders.toStringVector(sb.stringSet.head, sb.data.length, sb.naMask.result)
        case (true, sb: StringVectorBuilder) =>
          DictEncodingEncoders.toStringVector(sb.data, sb.naMask.result, sb.stringSet)
        case x: Any =>
          val bldr = builder.asInstanceOf[VectorBuilder[String]]
          SimpleEncoders.toStringVector(bldr.data, bldr.naMask.result)
      }
    }
  }

  implicit object DateTimeEncoder extends SingleMethodEncoder[DateTime] {
    val encodingPF: EncodingPF = {
      case (b: DateTimeVectorBuilder, _) =>
        DiffEncoders.toDateTimeVector(b.millisBuilder, b.tzBuilder, b.millisBuilder.naMask.result)
    }
  }

  implicit object SqlTimestampEncoder extends SingleMethodEncoder[Timestamp] with MinMaxEncoder[Long] {
    val encodingPF: EncodingPF = {
      case (b: SqlTimestampVectorBuilder, _) =>
        val (min, max, zero) = minMaxZero(b.millisBuilder)
        DiffEncoders.toPrimitiveVector(b.millisBuilder.data, b.millisBuilder.naMask.result, min, max)
    }
  }
}

