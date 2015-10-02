package org.velvia.filo

import com.google.flatbuffers.{FlatBufferBuilder, Table}
import java.nio.ByteBuffer
import org.velvia.filo.column._
import scala.collection.mutable.BitSet

/**
 * Common utilities for creating FlatBuffers, including mask and data vector building
 */
object Utils {
  // default initial size of bytebuffer to allocate.  flatbufferbuilder will expand the buffer if needed.
  // don't make this too big, because one buffer needs to be allocated per column, and writing many columns
  // at once will use up a ton of memory otherwise.
  val BufferSize = 64 * 1024
  val SizeOfInt = 4

  // Returns true if every element in the data to be encoded is marked as NA (mask is set)
  def isDataEmpty(mask: BitSet, dataLength: Int): Boolean = mask.size == dataLength

  // (offset of mask table, true if all NAs / bitmask full / empty data
  def populateNaMask(fbb: FlatBufferBuilder, mask: BitSet, dataLen: Int): (Int, Boolean) = {
    val empty = mask.size == 0
    val full = isDataEmpty(mask, dataLen)
    var bitMaskOffset = 0

    // Simple bit mask, 1 bit per row
    // One option is to use JavaEWAH compressed bitmaps, requires no deserialization now
    // RoaringBitmap is really cool, but very space inefficient when you have less than 4096 integers
    //    it's much better when you have 100000 or more rows
    // NOTE: we cannot nest structure creation, so have to create bitmask vector first :(
    if (!empty && !full) bitMaskOffset = NaMask.createBitMaskVector(fbb, mask.toBitMask)

    NaMask.startNaMask(fbb)
    NaMask.addMaskType(fbb, if (full)       { MaskType.AllOnes }
                            else if (empty) { MaskType.AllZeroes }
                            else            { MaskType.SimpleBitMask })

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
  def finishColumn(fbb: FlatBufferBuilder, colType: Byte, dataLen: Int): ByteBuffer = {
    // We want to at least throw an error here if colType is not in the valid range.
    // Better than writing out a random type byte and failing upon read.
    AnyColumn.name(colType)
    val colOffset = Column.createColumn(fbb, colType, fbb.endObject(), dataLen)
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
    case b: ByteVector   => b.dataLength
    case f: FloatVector  => f.dataLength
  }

  final def getVectorFromType(vectorType: Byte): Table = vectorType match {
    case AnyVector.IntVector    => new IntVector
    case AnyVector.StringVector => new StringVector
    case AnyVector.LongVector   => new LongVector
    case AnyVector.DoubleVector => new DoubleVector
    case AnyVector.ShortVector  => new ShortVector
    case AnyVector.ByteVector   => new ByteVector
    case AnyVector.FloatVector  => new FloatVector
  }
}
