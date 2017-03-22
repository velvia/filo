package org.velvia.filo

import com.google.flatbuffers.Table
import java.nio.ByteBuffer
import java.sql.Timestamp
import org.joda.time.DateTime

import org.velvia.filo.codecs._
import org.velvia.filo.vector._
import org.velvia.filo.vectors.{IntBinaryVector, DoubleVector, UTF8Vector, DictUTF8Vector, LongBinaryVector}

case class UnsupportedFiloType(vectType: Int, subType: Int) extends
  Exception(s"Unsupported Filo vector type $vectType, subType $subType")

/**
 * VectorReader is a type class to help create FiloVector's from the raw Filo binary byte buffers --
 * mostly parsing the header bytes and ensuring the creation of the right FiloVector parsing class.
 *
 * NOTE: I KNOW there is LOTS of repetition here, but apply() method is the inner loop and must be
 * super fast.  Traits would slow it WAY down.  Instead maybe we can use macros.
 */
object VectorReader {
  import WireFormat._
  import TypedBufferReader._

  implicit object BoolVectorReader extends PrimitiveVectorReader[Boolean]

  implicit object IntVectorReader extends PrimitiveVectorReader[Int] {
    override def makeDiffVector(dpv: DiffPrimitiveVector): FiloVector[Int] = {
      new DiffPrimitiveWrapper[Int, Int](dpv) {
        val base = baseReader.readInt(0)
        final def apply(i: Int): Int = base + dataReader.read(i)
      }
    }

    override val otherMaker: PartialFunction[(Int, Int, ByteBuffer), FiloVector[Int]] = {
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_INT, b)        => IntBinaryVector.masked(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_INT_NOMASK, b) => IntBinaryVector(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_REPEATED, b)   => IntBinaryVector.const(b)
    }
  }

  implicit object LongVectorReader extends PrimitiveVectorReader[Long] {
    override def makeDiffVector(dpv: DiffPrimitiveVector): FiloVector[Long] = {
      new DiffPrimitiveWrapper[Long, Long](dpv) {
        val base = baseReader.readLong(0)
        final def apply(i: Int): Long = base + dataReader.read(i)
      }
    }

    override val otherMaker: PartialFunction[(Int, Int, ByteBuffer), FiloVector[Long]] = {
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_INT, b)        => LongBinaryVector.fromMaskedIntBuf(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_INT_NOMASK, b) => LongBinaryVector.fromIntBuf(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_REPEATED, b)   => LongBinaryVector.const(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE, b)  => LongBinaryVector.masked(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE_NOMASK, b) => LongBinaryVector(b)
    }
  }

  implicit object DoubleVectorReader extends PrimitiveVectorReader[Double] {
    override val otherMaker: PartialFunction[(Int, Int, ByteBuffer), FiloVector[Double]] = {
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_INT, b)        => DoubleVector.fromMaskedIntBuf(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_INT_NOMASK, b) => DoubleVector.fromIntBuf(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_REPEATED, b)   => DoubleVector.const(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE, b)  => DoubleVector.masked(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE_NOMASK, b) => DoubleVector(b)
    }
  }

  implicit object FloatVectorReader extends PrimitiveVectorReader[Float]

  implicit object StringVectorReader extends VectorReader[String] {
    def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[String] = {
      (majorVectorType(headerBytes), vectorSubType(headerBytes)) match {
        case (VECTORTYPE_SIMPLE, SUBTYPE_STRING) =>
          val ssv = SimpleStringVector.getRootAsSimpleStringVector(buf)
          new SimpleStringWrapper(ssv)

        case (VECTORTYPE_CONST, SUBTYPE_STRING) =>
          val csv = ConstStringVector.getRootAsConstStringVector(buf)
          new ConstStringWrapper(csv)

        case (VECTORTYPE_DICT, SUBTYPE_STRING) =>
          val dsv = DictStringVector.getRootAsDictStringVector(buf)
          new DictStringWrapper(dsv) {
            val intReader = TypedBufferReader[Int](reader, dsv.info.nbits, dsv.info.signed)
            final def getCode(i: Int): Int = intReader.read(i)
          }

        case (vectType, subType) => throw UnsupportedFiloType(vectType, subType)
      }
    }
  }

  implicit object UTF8VectorReader extends VectorReader[ZeroCopyUTF8String] {
    def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[ZeroCopyUTF8String] = {
      (majorVectorType(headerBytes), vectorSubType(headerBytes)) match {
        case (VECTORTYPE_BINSIMPLE, SUBTYPE_UTF8)     => UTF8Vector(buf)
        case (VECTORTYPE_BINSIMPLE, SUBTYPE_FIXEDMAXUTF8) => UTF8Vector.fixedMax(buf)
        case (VECTORTYPE_BINDICT, SUBTYPE_UTF8)       => DictUTF8Vector(buf)
        case (VECTORTYPE_BINSIMPLE, SUBTYPE_REPEATED) => UTF8Vector.const(buf)
        case (vectType, subType) => throw UnsupportedFiloType(vectType, subType)
      }
    }
  }

  implicit object DateTimeVectorReader extends VectorReader[DateTime] {
    def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[DateTime] = {
      (majorVectorType(headerBytes), vectorSubType(headerBytes)) match {
        case (VECTORTYPE_DIFF, SUBTYPE_DATETIME) =>
          val ddtv = DiffDateTimeVector.getRootAsDiffDateTimeVector(buf)
          if (ddtv.tzLength == 0) {
            new DiffDateTimeWrapper(ddtv)
          } else {
            new DiffDateTimeWithTZWrapper(ddtv)
          }

        case (vectType, subType) => throw UnsupportedFiloType(vectType, subType)
      }
    }
  }

  implicit object TimestampVectorReader extends VectorReader[Timestamp] {
    def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[Timestamp] = {
      (majorVectorType(headerBytes), vectorSubType(headerBytes)) match {
        case (VECTORTYPE_DIFF, SUBTYPE_PRIMITIVE) =>
          val dpv = DiffPrimitiveVector.getRootAsDiffPrimitiveVector(buf)
          new DiffPrimitiveWrapper[Long, Timestamp](dpv) {
            val base = baseReader.readLong(0)
            final def apply(i: Int): Timestamp = new Timestamp(base + dataReader.read(i))
          }

        case (vectType, subType) => throw UnsupportedFiloType(vectType, subType)
      }
    }
  }
}

/**
 * Implemented by specific Filo column/vector types.
 */
trait VectorReader[A] {
  /**
   * Creates a FiloVector based on the remaining bytes.  Needs to decipher
   * what sort of vector it is and make the appropriate choice.
   * @param buf a ByteBuffer of the binary vector, with the position at right after
   *            the 4 header bytes... at the beginning of FlatBuffers or whatever
   * @param the four byte headerBytes
   */
  def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[A]
}

// NOTE: we MUST @specialize here so that the apply method below will not create boxing
class PrimitiveVectorReader[@specialized A: TypedReaderProvider] extends VectorReader[A] {
  import VectorReader._
  import WireFormat._

  def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[A] =
    vectMaker((majorVectorType(headerBytes), vectorSubType(headerBytes), buf))

  val fbbPrimitiveMaker: PartialFunction[(Int, Int, ByteBuffer), FiloVector[A]] = {
    case (VECTORTYPE_SIMPLE, SUBTYPE_PRIMITIVE, buf) =>
      val spv = SimplePrimitiveVector.getRootAsSimplePrimitiveVector(buf)
      new SimplePrimitiveWrapper[A](spv) {
        val typedReader = TypedBufferReader[A](reader, spv.info.nbits, spv.info.signed)
        final def apply(i: Int): A = typedReader.read(i)
      }

    case (VECTORTYPE_CONST, SUBTYPE_PRIMITIVE, buf) =>
      val spv = SimplePrimitiveVector.getRootAsSimplePrimitiveVector(buf)
      new SimplePrimitiveWrapper[A](spv) {
        val typedReader = TypedBufferReader[A](reader, spv.info.nbits, spv.info.signed)
        final def apply(i: Int): A = typedReader.read(0)
      }

    case (VECTORTYPE_DIFF, SUBTYPE_PRIMITIVE, buf) =>
      val dpv = DiffPrimitiveVector.getRootAsDiffPrimitiveVector(buf)
      makeDiffVector(dpv)
  }

  val defaultMaker: PartialFunction[(Int, Int, ByteBuffer), FiloVector[A]] = {
    case (vectType, subType, _) => throw UnsupportedFiloType(vectType, subType)
  }

  def otherMaker: PartialFunction[(Int, Int, ByteBuffer), FiloVector[A]] = Map.empty

  lazy val vectMaker = otherMaker orElse fbbPrimitiveMaker orElse defaultMaker

  def makeDiffVector(dpv: DiffPrimitiveVector): FiloVector[A] = ???
}
