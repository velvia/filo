package org.velvia.filo

import com.google.flatbuffers.Table
import java.nio.ByteBuffer
import java.sql.Timestamp
import org.joda.time.DateTime

import org.velvia.filo.codecs._
import org.velvia.filo.vector._
import org.velvia.filo.vectors.{IntBinaryVector, UTF8Vector}

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
    override def makeMaskedBinaryVector(buf: ByteBuffer): FiloVector[Int] = {
      val (base, off, nBytes) = UnsafeUtils.BOLfromBuffer(buf)
      IntBinaryVector.masked(base, off, nBytes)
    }
    override def makeSimpleBinaryVector(buf: ByteBuffer): FiloVector[Int] = {
      val (base, off, nBytes) = UnsafeUtils.BOLfromBuffer(buf)
      IntBinaryVector(base, off, nBytes)
    }
  }

  implicit object LongVectorReader extends PrimitiveVectorReader[Long] {
    override def makeDiffVector(dpv: DiffPrimitiveVector): FiloVector[Long] = {
      new DiffPrimitiveWrapper[Long, Long](dpv) {
        val base = baseReader.readLong(0)
        final def apply(i: Int): Long = base + dataReader.read(i)
      }
    }
  }

  implicit object DoubleVectorReader extends PrimitiveVectorReader[Double]
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
        case (VECTORTYPE_BINSIMPLE, SUBTYPE_UTF8) =>
          val (base, off, nBytes) = UnsafeUtils.BOLfromBuffer(buf)
          UTF8Vector(base, off, nBytes)
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

  def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[A] = {
    (majorVectorType(headerBytes), vectorSubType(headerBytes)) match {
      case (VECTORTYPE_SIMPLE, SUBTYPE_PRIMITIVE) =>
        val spv = SimplePrimitiveVector.getRootAsSimplePrimitiveVector(buf)
        new SimplePrimitiveWrapper[A](spv) {
          val typedReader = TypedBufferReader[A](reader, spv.info.nbits, spv.info.signed)
          final def apply(i: Int): A = typedReader.read(i)
        }

      case (VECTORTYPE_CONST, SUBTYPE_PRIMITIVE) =>
        val spv = SimplePrimitiveVector.getRootAsSimplePrimitiveVector(buf)
        new SimplePrimitiveWrapper[A](spv) {
          val typedReader = TypedBufferReader[A](reader, spv.info.nbits, spv.info.signed)
          final def apply(i: Int): A = typedReader.read(0)
        }

      case (VECTORTYPE_DIFF, SUBTYPE_PRIMITIVE) =>
        val dpv = DiffPrimitiveVector.getRootAsDiffPrimitiveVector(buf)
        makeDiffVector(dpv)

      case (VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE) =>
        makeMaskedBinaryVector(buf)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE_NOMASK) =>
        makeSimpleBinaryVector(buf)

      case (vectType, subType) => throw UnsupportedFiloType(vectType, subType)
    }
  }

  def makeDiffVector(dpv: DiffPrimitiveVector): FiloVector[A] = ???
  def makeMaskedBinaryVector(buf: ByteBuffer): FiloVector[A] = ???
  def makeSimpleBinaryVector(buf: ByteBuffer): FiloVector[A] = ???
}
