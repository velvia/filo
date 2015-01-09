package org.velvia.filo

import java.nio.ByteBuffer

/**
 * Type class for encoding a ColumnBuilder to queryable binary Filo format
 */
trait BuilderEncoder[A] {
  // Used for automatic conversion of Seq[A] and Seq[Option[A]]
  def getBuilder(): ColumnBuilder[A]
  def encode(builder: ColumnBuilder[A], hint: BuilderEncoder.EncodingHint): ByteBuffer
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

  implicit object IntEncoder extends BuilderEncoder[Int] {
    def getBuilder(): ColumnBuilder[Int] = new IntColumnBuilder
    def encode(builder: ColumnBuilder[Int], hint: EncodingHint) = {
      SimpleEncoders.toSimpleColumn(builder.data, builder.naMask.result,
                                      Utils.intVectorBuilder)
    }
  }

  implicit object LongEncoder extends BuilderEncoder[Long] {
    def getBuilder(): ColumnBuilder[Long] = new LongColumnBuilder
    def encode(builder: ColumnBuilder[Long], hint: EncodingHint) = {
      SimpleEncoders.toSimpleColumn(builder.data, builder.naMask.result,
                                      Utils.longVectorBuilder)
    }
  }

  implicit object DoubleEncoder extends BuilderEncoder[Double] {
    def getBuilder(): ColumnBuilder[Double] = new DoubleColumnBuilder
    def encode(builder: ColumnBuilder[Double], hint: EncodingHint) = {
      SimpleEncoders.toSimpleColumn(builder.data, builder.naMask.result,
                                      Utils.doubleVectorBuilder)
    }
  }

  implicit object StringEncoder extends BuilderEncoder[String] {
    def getBuilder(): ColumnBuilder[String] = new StringColumnBuilder
    def encode(builder: ColumnBuilder[String], hint: EncodingHint) = {
      val useDictEncoding = hint match {
        case DictionaryEncoding => true
        case SimpleEncoding   => false
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

  /**
   * Encodes a [[ColumnBuilder[A]]] to a Filo format ByteBuffer
   */
  def builderToBuffer[A: BuilderEncoder](builder: ColumnBuilder[A],
                                         hint: EncodingHint = AutoDetect): ByteBuffer = {
    implicitly[BuilderEncoder[A]].encode(builder, hint)
  }

  /**
   * Encodes a sequence of type A to a Filo format ByteBuffer
   * All values will be marked available.
   * I know this may not be the most efficient way to encode, but the benefits is that
   * all of the auto-encoding-detection is available.
   */
  def seqToBuffer[A: BuilderEncoder](vector: collection.Seq[A],
                                     hint: EncodingHint = AutoDetect): ByteBuffer = {
    val builder = implicitly[BuilderEncoder[A]].getBuilder
    vector.foreach(builder.addData)
    builderToBuffer(builder, hint)
  }
}

