package org.velvia.filo

import com.google.flatbuffers.{FlatBufferBuilder, Table}
import framian.column.Mask
import java.nio.ByteBuffer
import org.velvia.filo.column._

/**
 * Common utilities for creating FlatBuffers, including mask and data vector building
 */
object Utils {
  // default initial size of bytebuffer to allocate.  flatbufferbuilder will expand the buffer if needed.
  // don't make this too big, because one buffer needs to be allocated per column, and writing many columns
  // at once will use up a ton of memory otherwise.
  val BufferSize = 64 * 1024
  val SizeOfInt = 4

  // (offset of mask table, true if all NAs / bitmask full / empty data
  def populateNaMask(fbb: FlatBufferBuilder, mask: Mask): (Int, Boolean) = {
    val empty = mask.size == 0
    val full = mask.size > 0 && mask.size == (mask.max.get + 1)
    var bitMaskOffset = 0

    // Simple bit mask, 1 bit per row
    // One option is to use JavaEWAH compressed bitmaps, requires no deserialization now
    // RoaringBitmap is really cool, but very space inefficient when you have less than 4096 integers
    //    it's much better when you have 100000 or more rows
    // NOTE: we cannot nest structure creation, so have to create bitmask vector first :(
    if (!empty && !full) bitMaskOffset = NaMask.createBitMaskVector(fbb, mask.toBitSet.toBitMask)

    NaMask.startNaMask(fbb)
    NaMask.addMaskType(fbb, if (empty)     { MaskType.AllZeroes }
                            else if (full) { MaskType.AllOnes }
                            else           { MaskType.SimpleBitMask })

    if (!empty && !full) NaMask.addBitMask(fbb, bitMaskOffset)
    (NaMask.endNaMask(fbb), full)
  }

  type DataVectorBuilder[A] = (FlatBufferBuilder, Seq[A]) => (Int, Byte)

  def byteVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Byte]): (Int, Byte) = {
    val vectOffset = ByteVector.createDataVector(fbb, data.toArray)
    (ByteVector.createByteVector(fbb, ByteDataType.TByte, vectOffset), AnyVector.ByteVector)
  }

  def shortVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Short]): (Int, Byte) = {
    val vectOffset = ShortVector.createDataVector(fbb, data.toArray)
    (ShortVector.createShortVector(fbb, vectOffset), AnyVector.ShortVector)
  }

  def intVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Int]): (Int, Byte) = {
    val vectOffset = IntVector.createDataVector(fbb, data.toArray)
    (IntVector.createIntVector(fbb, vectOffset), AnyVector.IntVector)
  }

  def longVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Long]): (Int, Byte) = {
    val vectOffset = LongVector.createDataVector(fbb, data.toArray)
    (LongVector.createLongVector(fbb, vectOffset), AnyVector.LongVector)
  }

  def doubleVectorBuilder(fbb: FlatBufferBuilder, data: Seq[Double]): (Int, Byte) = {
    val vectOffset = DoubleVector.createDataVector(fbb, data.toArray)
    (DoubleVector.createDoubleVector(fbb, vectOffset), AnyVector.DoubleVector)
  }

  def stringVectorBuilder(fbb: FlatBufferBuilder, data: Seq[String]): (Int, Byte) = {
    val vectOffset = makeStringVect(fbb, data)
    (StringVector.createStringVector(fbb, vectOffset), AnyVector.StringVector)
  }

  def makeStringVect(fbb: FlatBufferBuilder, data: Seq[String]): Int = {
    val offsets = data.map { str => fbb.createString(str) }.toArray
    StringVector.createDataVector(fbb, offsets)
  }

  // Just finishes the Column and returns the ByteBuffer.
  // It would be nice to wrap the lifecycle, but too many intricacies with building a FB now.
  def finishColumn(fbb: FlatBufferBuilder, colType: Byte): ByteBuffer = {
    // We want to at least throw an error here if colType is not in the valid range.
    // Better than writing out a random type byte and failing upon read.
    AnyColumn.name(colType)
    val colOffset = Column.createColumn(fbb, colType, fbb.endObject())
    Column.finishColumnBuffer(fbb, colOffset)
    fbb.dataBuffer()
  }
}

// Yuck.  I think we really need to generate native Scala FlatBuffers code, because the Java
// code it generates is pretty pretty yucky.
object VectorUtils {
  final def getLength(t: Table): Int = t match {
    case i: IntVector    => i.dataLength
    case s: StringVector => s.dataLength
    case l: LongVector   => l.dataLength
    case d: DoubleVector => d.dataLength
    case s: ShortVector  => s.dataLength
    case f: FloatVector  => f.dataLength
  }

  final def getVectorFromType(vectorType: Byte): Table = vectorType match {
    case AnyVector.IntVector    => new IntVector
    case AnyVector.StringVector => new StringVector
    case AnyVector.LongVector   => new LongVector
    case AnyVector.DoubleVector => new DoubleVector
    case AnyVector.ShortVector  => new ShortVector
    case AnyVector.FloatVector  => new FloatVector
  }
}

trait VectorExtractor[A] {
  def getExtractor(t: Table): (Int => A)
}

/**
 * Type classes to extract values of type A from any underlying vector.
 * For instance, a ByteVector, ShortVector, or IntVector may extract to Int.
 */
object VectorExtractor {
  private def unsupportedVector[T](t: Table): (Int => T) =
    throw new RuntimeException("Unsupported vector table type " + t)

  implicit object IntVectorExtractor extends VectorExtractor[Int] {
    def getExtractor(t: Table): (Int => Int) = t match {
      case v: IntVector    =>
        val intReader = new FastIntBufferReader(v.dataAsByteBuffer())
        (i: Int) => intReader.read(i)
      case v: ShortVector  =>
        val shortReader = new FastShortBufferReader(v.dataAsByteBuffer())
        (i: Int) => shortReader.read(i).toInt
      case v: ByteVector if v.dataType == ByteDataType.TByte =>
        val byteReader = new FastByteBufferReader(v.dataAsByteBuffer())
        (i: Int) => byteReader.read(i).toInt
      case x: Any          => unsupportedVector(x)
    }
  }

  implicit object StringVectorExtractor extends VectorExtractor[String] {
    def getExtractor(t: Table): (Int => String) = t match {
      case v: StringVector => (i: Int) => v.data(i)
      case x: Any          => unsupportedVector(x)
    }
  }

  implicit object LongVectorExtractor extends VectorExtractor[Long] {
    def getExtractor(t: Table): (Int => Long) = t match {
      case v: LongVector   => (i: Int) => v.data(i)
      case x: Any          => unsupportedVector(x)
    }
  }

  implicit object DoubleVectorExtractor extends VectorExtractor[Double] {
    def getExtractor(t: Table): (Int => Double) = t match {
      case v: DoubleVector => (i: Int) => v.data(i)
      case x: Any          => unsupportedVector(x)
    }
  }
}

