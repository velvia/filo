package org.velvia.filo

import com.google.flatbuffers.Table
import java.nio.ByteBuffer
import org.velvia.filo.column._

/**
 * The main entry point for parsing a Filo binary vector, returning a ColumnWrapper with which
 * to iterate over and read the data vector.
 */
object ColumnParser {
  /**
   * Parses a Filo-format ByteBuffer into a ColumnWrapper.  Automatically detects what type of encoding
   * is used underneath.
   */
  def parse[A](buf: ByteBuffer)(implicit cm: ColumnMaker[A]): ColumnWrapper[A] = {
    cm.makeColumn(buf)
  }

  implicit object IntSimpleColumnMaker extends SimpleColumnMaker[Int] {
    def makeSimpleCol(sc: SimpleColumn, vector: Table): ColumnWrapper[Int] = vector match {
      case v: IntVector =>
        new SimpleColumnWrapper[Int](sc, vector) {
          val reader = FastBufferReader(v.dataAsByteBuffer())
          final def apply(i: Int): Int = reader.readInt(i)
        }
      case v: ShortVector =>
        new SimpleColumnWrapper[Int](sc, vector) {
          val reader = FastBufferReader(v.dataAsByteBuffer())
          final def apply(i: Int): Int = (reader.readShort(i) & 0x0ffff).toInt
        }
      case v: ByteVector if v.dataType == ByteDataType.TByte =>
        new SimpleColumnWrapper[Int](sc, vector) {
          val reader = FastBufferReader(v.dataAsByteBuffer())
          final def apply(i: Int): Int = (reader.readByte(i) & 0x00ff).toInt
        }
    }
  }

  implicit object LongSimpleColumnMaker extends SimpleColumnMaker[Long] {
    def makeSimpleCol(sc: SimpleColumn, vector: Table): ColumnWrapper[Long] = vector match {
      case v: LongVector =>
        new SimpleColumnWrapper[Long](sc, vector) {
          val reader = FastBufferReader(v.dataAsByteBuffer())
          final def apply(i: Int): Long = reader.readLong(i)
        }
    }
  }

  implicit object DoubleSimpleColumnMaker extends SimpleColumnMaker[Double] {
    def makeSimpleCol(sc: SimpleColumn, vector: Table): ColumnWrapper[Double] = vector match {
      case v: DoubleVector =>
        new SimpleColumnWrapper[Double](sc, vector) {
          val reader = FastBufferReader(v.dataAsByteBuffer())
          final def apply(i: Int): Double = reader.readDouble(i)
        }
    }
  }

  implicit object StringColumnMaker extends SimpleColumnMaker[String] {
    def makeSimpleCol(sc: SimpleColumn, vector: Table): ColumnWrapper[String] = vector match {
      case v: StringVector =>
        new SimpleColumnWrapper[String](sc, vector) {
          final def apply(i: Int): String = v.data(i)
        }
    }

    override def makeColumn(buf: ByteBuffer): ColumnWrapper[String] = {
      val column = Column.getRootAsColumn(buf)
      column.colType match {
        case AnyColumn.SimpleColumn => simpleColumnWrap(column)
        case AnyColumn.DictStringColumn =>
          val dsc = new DictStringColumn
          column.col(dsc)
          if (dsc.codesType == 0) return new EmptyColumnWrapper[String]
          val vector = VectorUtils.getVectorFromType(dsc.codesType)
          dsc.codes(vector)
          vector match {
            case v: IntVector =>
              new DictStringColumnWrapper(dsc, vector) {
                val reader = FastBufferReader(v.dataAsByteBuffer())
                final def getCode(i: Int): Int = reader.readInt(i)
              }
            case v: ShortVector =>
              new DictStringColumnWrapper(dsc, vector) {
                val reader = FastBufferReader(v.dataAsByteBuffer())
                final def getCode(i: Int): Int = (reader.readShort(i) & 0x0ffff).toInt
              }
            case v: ByteVector if v.dataType == ByteDataType.TByte =>
              new DictStringColumnWrapper(dsc, vector) {
                val reader = FastBufferReader(v.dataAsByteBuffer())
                final def getCode(i: Int): Int = (reader.readByte(i) & 0x00ff).toInt
              }
          }
      }
    }
  }
}

trait ColumnMaker[A] {
  def makeColumn(buf: ByteBuffer): ColumnWrapper[A]
}

trait SimpleColumnMaker[A] extends ColumnMaker[A] {
  def makeSimpleCol(sc: SimpleColumn, vector: Table): ColumnWrapper[A]

  def simpleColumnWrap(c: Column): ColumnWrapper[A] = {
    val sc = new SimpleColumn
    c.col(sc)
    if (sc.naMask.maskType == MaskType.AllOnes) {
      new EmptyColumnWrapper[A]
    } else {
      val vector = VectorUtils.getVectorFromType(sc.vectorType)
      sc.vector(vector)
      makeSimpleCol(sc, vector)
    }
  }

  def makeColumn(buf: ByteBuffer): ColumnWrapper[A] = {
    if (buf == null) return new EmptyColumnWrapper[A]
    val column = Column.getRootAsColumn(buf)
    require(column.colType == AnyColumn.SimpleColumn,
            "Not a SimpleColumn, but a " + AnyColumn.name(column.colType))
    simpleColumnWrap(column)
  }
}
