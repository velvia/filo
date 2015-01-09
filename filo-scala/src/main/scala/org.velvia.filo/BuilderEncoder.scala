package org.velvia.filo

import java.nio.ByteBuffer

/**
 * Type class for encoding a ColumnBuilder to queryable binary Filo format
 */
trait BuilderEncoder[A] {
  def encode(builder: ColumnBuilder[A], hint: BuilderEncoder.EncodingHint): ByteBuffer
}

/**
 * Classes to encode a Builder to a queryable binary Filo format.
 * Methods automatically detect the best encoding method to use, but hints are available
 * to pass to the methods.
 */
object BuilderEncoder {
  sealed trait EncodingHint
  case object AutoDetect extends EncodingHint
  case object SimpleConversion extends EncodingHint
  case object DictionaryEncoding extends EncodingHint

  implicit object IntEncoder extends BuilderEncoder[Int] {
    def encode(builder: ColumnBuilder[Int], hint: EncodingHint) = {
      SimpleEncoders.toSimpleColumn(builder.data, builder.naMask.result,
                                      Utils.intVectorBuilder)
    }
  }

  implicit object LongEncoder extends BuilderEncoder[Long] {
    def encode(builder: ColumnBuilder[Long], hint: EncodingHint) = {
      SimpleEncoders.toSimpleColumn(builder.data, builder.naMask.result,
                                      Utils.longVectorBuilder)
    }
  }

  implicit object DoubleEncoder extends BuilderEncoder[Double] {
    def encode(builder: ColumnBuilder[Double], hint: EncodingHint) = {
      SimpleEncoders.toSimpleColumn(builder.data, builder.naMask.result,
                                      Utils.doubleVectorBuilder)
    }
  }

  implicit object StringEncoder extends BuilderEncoder[String] {
    def encode(builder: ColumnBuilder[String], hint: EncodingHint) = {
      val useDictEncoding = hint match {
        case DictionaryEncoding => true
        case SimpleConversion   => false
        case x => builder match {
          case sb: StringColumnBuilder =>
            // If the string cardinality is below say half of # of elements
            // then definitely worth it to do dictionary encoding.
            // Empty/missing elements do not count towards cardinality, so columns with
            // many NA values will get dict encoded, which saves space
            sb.stringSet.size <= (sb.data.size / 2)
          case x =>  // Someone used something other than our own builder. Oh well. TODO: log
            false
        }
      }
      (useDictEncoding, builder) match {
        case (true, sb: StringColumnBuilder) =>
          DictEncodingEncoders.toDictStringColumn(sb.data, sb.naMask.result, sb.stringSet)
        case x =>
          SimpleEncoders.toSimpleColumn(builder.data, builder.naMask.result,
                                          Utils.stringVectorBuilder)
      }
    }
  }

  def encodeToBuffer[A: BuilderEncoder](builder: ColumnBuilder[A],
                                          hint: EncodingHint = AutoDetect): ByteBuffer = {
    implicitly[BuilderEncoder[A]].encode(builder, hint)
  }
}

