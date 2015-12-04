package org.velvia.filo

import com.google.flatbuffers.Table
import java.nio.ByteBuffer

import org.velvia.filo.codecs._
import org.velvia.filo.vector._

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

  type NBitsToWrapper[A] = PartialFunction[(Int, Boolean), FiloVector[A]]

  def UnsupportedVectPF[A]: NBitsToWrapper[A] = {
    case (nbits, signed) => throw new RuntimeException(s"Unsupported Filo vector nbits=$nbits signed=$signed")
  }

  implicit object BoolVectorReader extends PrimitiveVectorReader[Boolean] {
    def simpleVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Boolean] = {
      case (64, false) =>
        new SimplePrimitiveWrapper[Boolean](spv) {
          final def apply(i: Int): Boolean = ((reader.readByte(i >> 3) >> (i & 0x07)) & 0x01) != 0
        }
    }

    def constVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Boolean] = {
      case (64, false) =>
        new SimplePrimitiveWrapper[Boolean](spv) {
          final def apply(i: Int): Boolean = (reader.readByte(0) & 0x01) != 0
        }
    }
  }

  implicit object IntVectorReader extends PrimitiveVectorReader[Int] {
    def simpleVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Int] = {
      case (32, false) => new SimplePrimitiveWrapper[Int](spv) {
                            final def apply(i: Int): Int = reader.readInt(i)
                          }
      case (16, false) => new SimplePrimitiveWrapper[Int](spv) {
                            final def apply(i: Int): Int = (reader.readShort(i) & 0x0ffff).toInt
                          }
      case (8, false)  => new SimplePrimitiveWrapper[Int](spv) {
                            final def apply(i: Int): Int = (reader.readByte(i) & 0x00ff).toInt
                          }
    }

    def constVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Int] = {
      case (32, false) => new SimplePrimitiveWrapper[Int](spv) {
                            final def apply(i: Int): Int = reader.readInt(0)
                          }
      case (16, false) => new SimplePrimitiveWrapper[Int](spv) {
                            final def apply(i: Int): Int = (reader.readShort(0) & 0x0ffff).toInt
                          }
      case (8, false)  => new SimplePrimitiveWrapper[Int](spv) {
                            final def apply(i: Int): Int = (reader.readByte(0) & 0x00ff).toInt
                          }
    }
 }

  implicit object LongVectorReader extends PrimitiveVectorReader[Long] {
    def simpleVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Long] = {
      case (64, false) => new SimplePrimitiveWrapper[Long](spv) {
                            final def apply(i: Int): Long = reader.readLong(i)
                          }
      case (32, false) => new SimplePrimitiveWrapper[Long](spv) {
                            final def apply(i: Int): Long = (reader.readInt(i) & 0x0ffffffffL).toLong
                          }
      case (16, false) => new SimplePrimitiveWrapper[Long](spv) {
                            final def apply(i: Int): Long = (reader.readShort(i) & 0x0ffff).toLong
                          }
      case (8, false)  => new SimplePrimitiveWrapper[Long](spv) {
                            final def apply(i: Int): Long = (reader.readByte(i) & 0x00ff).toLong
                          }
    }

    def constVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Long] = {
      case (64, false) => new SimplePrimitiveWrapper[Long](spv) {
                            final def apply(i: Int): Long = reader.readLong(0)
                          }
      case (32, false) => new SimplePrimitiveWrapper[Long](spv) {
                            final def apply(i: Int): Long = (reader.readInt(0) & 0x0ffffffffL).toLong
                          }
      case (16, false) => new SimplePrimitiveWrapper[Long](spv) {
                            final def apply(i: Int): Long = (reader.readShort(0) & 0x0ffff).toLong
                          }
      case (8, false)  => new SimplePrimitiveWrapper[Long](spv) {
                            final def apply(i: Int): Long = (reader.readByte(0) & 0x00ff).toLong
                          }
    }
  }

  implicit object DoubleVectorReader extends PrimitiveVectorReader[Double] {
    def simpleVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Double] = {
      case (64, false) => new SimplePrimitiveWrapper[Double](spv) {
                            final def apply(i: Int): Double = reader.readDouble(i)
                          }
    }

    def constVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Double] = {
      case (64, false) => new SimplePrimitiveWrapper[Double](spv) {
                            final def apply(i: Int): Double = reader.readDouble(0)
                          }
    }
  }

  implicit object FloatVectorReader extends PrimitiveVectorReader[Float] {
    def simpleVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Float] = {
      case (32, false) => new SimplePrimitiveWrapper[Float](spv) {
                            final def apply(i: Int): Float = reader.readFloat(i)
                          }
    }

    def constVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Float] = {
      case (32, false) => new SimplePrimitiveWrapper[Float](spv) {
                            final def apply(i: Int): Float = reader.readFloat(0)
                          }
    }
  }

  implicit object StringVectorReader extends VectorReader[String] {
    def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[String] = {
      (majorVectorType(headerBytes), vectorSubType(headerBytes)) match {
        case (VECTORTYPE_SIMPLE, SUBTYPE_STRING) =>
          val ssv = SimpleStringVector.getRootAsSimpleStringVector(buf)
          new SimpleStringWrapper(ssv)
        case (VECTORTYPE_DICT, SUBTYPE_STRING) =>
          val dsv = DictStringVector.getRootAsDictStringVector(buf)
          (dictStringVectPF(dsv) orElse UnsupportedVectPF)((dsv.info.nbits, dsv.info.signed))
        case (vectType, subType) => throw UnsupportedFiloType(vectType, subType)
      }
    }

    def dictStringVectPF(dsv: DictStringVector): NBitsToWrapper[String] = {
      case (32, false) => new DictStringWrapper(dsv) {
                            final def getCode(i: Int): Int = reader.readInt(i)
                          }
      case (16, false) => new DictStringWrapper(dsv) {
                            final def getCode(i: Int): Int = (reader.readShort(i) & 0x0ffff).toInt
                          }
      case (8, false)  => new DictStringWrapper(dsv) {
                            final def getCode(i: Int): Int = (reader.readByte(i) & 0x00ff).toInt
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

// TODO: Move this somewhere else
trait PrimitiveVectorReader[A] extends VectorReader[A] {
  import VectorReader._
  import WireFormat._

  def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[A] = {
    (majorVectorType(headerBytes), vectorSubType(headerBytes)) match {
      case (VECTORTYPE_SIMPLE, SUBTYPE_PRIMITIVE) =>
        val spv = SimplePrimitiveVector.getRootAsSimplePrimitiveVector(buf)
        (simpleVectPF(spv) orElse UnsupportedVectPF[A])((spv.info.nbits, spv.info.signed))

      case (VECTORTYPE_CONST, SUBTYPE_PRIMITIVE) =>
        val spv = SimplePrimitiveVector.getRootAsSimplePrimitiveVector(buf)
        (constVectPF(spv) orElse UnsupportedVectPF[A])((spv.info.nbits, spv.info.signed))

      case (vectType, subType) => throw UnsupportedFiloType(vectType, subType)
    }
  }

  def simpleVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[A]
  def constVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[A]
}
