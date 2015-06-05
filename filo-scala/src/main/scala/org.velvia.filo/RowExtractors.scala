package org.velvia.filo

import java.nio.ByteBuffer

/**
 * [[ColumnRowExtractor]] helps extracts column values to some user defined Row, especially
 * where you have a heterogeneous and dynamic mix of column types.  It defines an extractToRow()
 * API which is the same regardless of column type, and also handles missing/NA values.
 */
object RowExtractors {
  import ColumnParser._

  abstract class ColumnRowExtractor[CT: ColumnMaker, R] {
    val bytes: ByteBuffer
    val setter: RowSetter[R]
    val colIndex: Int
    val wrapper: ColumnWrapper[CT] = ColumnParser.parse[CT](bytes)(implicitly[ColumnMaker[CT]])
    final def extractToRow(rowNo: Int, row: R): Unit =
      if (wrapper.isAvailable(rowNo)) extract(rowNo, row) else setter.setNA(row, colIndex)

    // Basically, set the row R from column rowNo assuming value is available
    def extract(rowNo: Int, row: R): Unit
  }

  // TODO: replace this with macros !!
  class IntColumnRowExtractor[R](val bytes: ByteBuffer, val colIndex: Int, val setter: RowSetter[R])
  extends ColumnRowExtractor[Int, R] {
    final def extract(rowNo: Int, row: R): Unit = setter.setInt(row, colIndex, wrapper(rowNo))
  }

  class DoubleColumnRowExtractor[R](val bytes: ByteBuffer, val colIndex: Int, val setter: RowSetter[R])
  extends ColumnRowExtractor[Double, R] {
    final def extract(rowNo: Int, row: R): Unit = setter.setDouble(row, colIndex, wrapper(rowNo))
  }

  class LongColumnRowExtractor[R](val bytes: ByteBuffer, val colIndex: Int, val setter: RowSetter[R])
  extends ColumnRowExtractor[Long, R] {
    final def extract(rowNo: Int, row: R): Unit = setter.setLong(row, colIndex, wrapper(rowNo))
  }

  class StringColumnRowExtractor[R](val bytes: ByteBuffer, val colIndex: Int, val setter: RowSetter[R])
  extends ColumnRowExtractor[String, R] {
    final def extract(rowNo: Int, row: R): Unit = setter.setString(row, colIndex, wrapper(rowNo))
  }

  /**
   * Convenience function to create an array of extractors given a list of column types,
   * the raw bytes, and a RowSetter.   One can then iterate through and generate rows one by one.
   * @param chunks the raw ByteBuffers to be parsed
   * @param columns the column types corresponding to the chunks
   * @param setter a [[RowSetter]]
   * @return an Array of Extractors, which can then be used to step through the rows of the Filo columns
   */
  def getRowExtractors[R](chunks: Array[ByteBuffer], colTypes: Seq[Class[_]], setter: RowSetter[R]):
      Array[ColumnRowExtractor[_, R]] = {
    require(chunks.size == colTypes.size, "chunks and colTypes must be same length")
    val extractors = for { i <- 0 until chunks.length } yield {
      colTypes(i) match {
        case Classes.Int    => new IntColumnRowExtractor(chunks(i), i, setter)
        case Classes.Long   => new LongColumnRowExtractor(chunks(i), i, setter)
        case Classes.Double => new DoubleColumnRowExtractor(chunks(i), i, setter)
        case Classes.String => new StringColumnRowExtractor(chunks(i), i, setter)
      }
    }
    extractors.toArray
  }
}