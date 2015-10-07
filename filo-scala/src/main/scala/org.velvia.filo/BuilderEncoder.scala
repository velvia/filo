package org.velvia.filo

import java.nio.ByteBuffer
import scala.reflect.ClassTag

/**
 * Type class for encoding a ColumnBuilder to queryable binary Filo format
 */
trait BuilderEncoder[A] {
  // Used for automatic conversion of Seq[A] and Seq[Option[A]]
  def getBuilder(): ColumnBuilder[A]
  def encodeInner(builder: ColumnBuilder[A], hint: BuilderEncoder.EncodingHint): ByteBuffer
  def encode(builder: ColumnBuilder[A], hint: BuilderEncoder.EncodingHint): ByteBuffer = {
    if (Utils.isAllNA(builder.naMask, builder.data.length) &&
        builder.data.length <= WireFormat.MaxEmptyVectorLen) {
      SimpleEncoders.toEmptyVector(builder.data.length)
    } else {
      encodeInner(builder, hint)
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

  def apply[T: BuilderEncoder]: BuilderEncoder[T] = implicitly[BuilderEncoder[T]]

  implicit object IntEncoder extends IntegralEncoder[Int] {
    def getBuilder: ColumnBuilder[Int] = new IntColumnBuilder
    val unsignedBuilder = PrimitiveUnsignedBuilders.IntDataVectBuilder
  }

  implicit object LongEncoder extends IntegralEncoder[Long] {
    def getBuilder: ColumnBuilder[Long] = new LongColumnBuilder
    val unsignedBuilder = PrimitiveUnsignedBuilders.LongDataVectBuilder
  }

  implicit object DoubleEncoder extends BuilderEncoder[Double] with MinMaxEncoder[Double] {
    def getBuilder: ColumnBuilder[Double] = new DoubleColumnBuilder
    def encodeInner(builder: ColumnBuilder[Double], hint: EncodingHint): ByteBuffer = {
      import FPBuilders._
      val (min, max, _) = minMaxZero(builder)
      SimpleEncoders.toPrimitiveVector(builder.data, builder.naMask.result,
                                       min, max, false)
    }
  }

  implicit object StringEncoder extends BuilderEncoder[String] {
    def getBuilder: ColumnBuilder[String] = new StringColumnBuilder
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

  /**
   * Encodes a [[org.velvia.filo.ColumnBuilder]] to a Filo format ByteBuffer
   */
  def builderToBuffer[A: BuilderEncoder](builder: ColumnBuilder[A],
                                         hint: EncodingHint = AutoDetect): ByteBuffer = {
    BuilderEncoder[A].encode(builder, hint)
  }

  /**
   * Encodes a sequence of type A to a Filo format ByteBuffer
   * All values will be marked available.
   * I know this may not be the most efficient way to encode, but the benefits is that
   * all of the auto-encoding-detection is available.
   */
  def seqToBuffer[A: BuilderEncoder](vector: collection.Seq[A],
                                     hint: EncodingHint = AutoDetect): ByteBuffer = {
    val builder = BuilderEncoder[A].getBuilder
    vector.foreach(builder.addData)
    builderToBuffer(builder, hint)
  }

  /**
   * Encodes a sequence of type Option[A] to a Filo format ByteBuffer.
   * Elements which are None will get encoded as NA bits.
   */
  def seqOptionToBuffer[A: BuilderEncoder](vector: collection.Seq[Option[A]],
                                           hint: EncodingHint = AutoDetect): ByteBuffer = {
    val builder = BuilderEncoder[A].getBuilder
    vector.foreach(builder.addOption)
    builderToBuffer(builder, hint)
  }
}

