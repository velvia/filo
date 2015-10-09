package org.velvia.filo

import com.google.flatbuffers.Table
import java.nio.{ByteBuffer, ByteOrder}

import org.velvia.filo.codecs._
import org.velvia.filo.vector._

case class UnsupportedFiloType(vectType: Int, subType: Int) extends
  Exception(s"Unsupported Filo vector type $vectType, subType $subType")

/**
 * The main entry point for parsing a Filo binary vector, returning a ColumnWrapper with which
 * to iterate over and read the data vector.
 */
object ColumnParser {
  import WireFormat._

  type NBitsToWrapper[A] = PartialFunction[(Int, Boolean), ColumnWrapper[A]]

  def UnsupportedVectPF[A]: NBitsToWrapper[A] = {
    case (nbits, signed) => throw new RuntimeException(s"Unsupported Filo vector nbits=$nbits signed=$signed")
  }

  /**
   * Parses a Filo-format ByteBuffer into a ColumnWrapper.  Automatically detects what type of encoding
   * is used underneath.
   *
   * @param buf the ByteBuffer with the columnar chunk at the current position.  After parse returns, the
   *            position will be restored to its original value, but it may change in the meantime.
   */
  def parse[A](buf: ByteBuffer)(implicit cm: ColumnMaker[A]): ColumnWrapper[A] = {
    if (buf == null) return new EmptyColumnWrapper[A](0)
    val origPos = buf.position
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val headerBytes = buf.getInt()
    val wrapper = majorVectorType(headerBytes) match {
      case VECTORTYPE_EMPTY =>
        new EmptyColumnWrapper[A](emptyVectorLen(headerBytes))
      case other =>
        cm.makeColumn(buf, headerBytes)
    }
    buf.position(origPos)
    wrapper
  }

  implicit object BoolColumnMaker extends PrimitiveColumnMaker[Boolean] {
    def simpleVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Boolean] = {
      case (64, false) =>
        new SimplePrimitiveWrapper[Boolean](spv) {
          final def apply(i: Int): Boolean = ((reader.readByte(i >> 3) >> (i & 0x07)) & 0x01) != 0
        }
    }
  }

  implicit object IntColumnMaker extends PrimitiveColumnMaker[Int] {
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
  }

  implicit object LongColumnMaker extends PrimitiveColumnMaker[Long] {
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
  }

  implicit object DoubleColumnMaker extends PrimitiveColumnMaker[Double] {
    def simpleVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Double] = {
      case (64, false) => new SimplePrimitiveWrapper[Double](spv) {
                            final def apply(i: Int): Double = reader.readDouble(i)
                          }
    }
  }

  implicit object FloatColumnMaker extends PrimitiveColumnMaker[Float] {
    def simpleVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[Float] = {
      case (32, false) => new SimplePrimitiveWrapper[Float](spv) {
                            final def apply(i: Int): Float = reader.readFloat(i)
                          }
    }
  }

  implicit object StringColumnMaker extends ColumnMaker[String] {
    def makeColumn(buf: ByteBuffer, headerBytes: Int): ColumnWrapper[String] = {
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
trait ColumnMaker[A] {
  /**
   * Creates a ColumnWrapper based on the remaining bytes.  Needs to decipher
   * what sort of vector it is and make the appropriate choice.
   * @param buf a ByteBuffer of the binary vector, with the position at right after
   *            the 4 header bytes... at the beginning of FlatBuffers or whatever
   * @param the four byte headerBytes
   */
  def makeColumn(buf: ByteBuffer, headerBytes: Int): ColumnWrapper[A]
}

// TODO: Move this somewhere else
trait PrimitiveColumnMaker[A] extends ColumnMaker[A] {
  import ColumnParser._
  import WireFormat._

  def makeColumn(buf: ByteBuffer, headerBytes: Int): ColumnWrapper[A] = {
    (majorVectorType(headerBytes), vectorSubType(headerBytes)) match {
      case (VECTORTYPE_SIMPLE, SUBTYPE_PRIMITIVE) =>
        val spv = SimplePrimitiveVector.getRootAsSimplePrimitiveVector(buf)
        (simpleVectPF(spv) orElse UnsupportedVectPF[A])((spv.info.nbits, spv.info.signed))
      case (vectType, subType) => throw UnsupportedFiloType(vectType, subType)
    }
  }

  def simpleVectPF(spv: SimplePrimitiveVector): NBitsToWrapper[A]
}
