package org.velvia.filo

import java.nio.ByteBuffer
import scala.language.existentials
import scala.language.postfixOps
import scalaxy.loops._

import BuilderEncoder.{EncodingHint, AutoDetect}

case class IngestColumn(name: String, dataType: Class[_])

// To help matching against the ClassTag in the ColumnBuilder
private object Classes {
  val Byte = java.lang.Byte.TYPE
  val Short = java.lang.Short.TYPE
  val Int = java.lang.Integer.TYPE
  val Long = java.lang.Long.TYPE
  val Float = java.lang.Float.TYPE
  val Double = java.lang.Double.TYPE
  val String = classOf[String]
}

object RowToColumnBuilder {
  /**
   * A convenience method to turn a bunch of rows R to Filo serialized columnar chunks.
   * @param rows the rows to convert to columnar chunks
   * @param schema a Seq of IngestColumn describing the [[ColumnBuilder]] used for each column
   * @param hint an EncodingHint for the encoder
   * @return a Map of column name to the byte chunks
   */
  def buildFromRows(rows: Iterator[RowReader],
                    schema: Seq[IngestColumn],
                    hint: EncodingHint = AutoDetect): Map[String, ByteBuffer] = {
    val builder = new RowToColumnBuilder(schema)
    rows.foreach(builder.addRow)
    builder.convertToBytes(hint)
  }
}

/**
 * Class to help transpose a set of rows to Filo binary columns.
 * @param schema a Seq of IngestColumn describing the data type used for each column
 *
 * TODO: Add stats about # of rows, chunks/buffers encoded, bytes encoded, # NA's etc.
 */
class RowToColumnBuilder(schema: Seq[IngestColumn]) {
  val builders = schema.map { case IngestColumn(_, dataType) => ColumnBuilder(dataType) }
  val numColumns = schema.length

  // Extract out a value assuming it is available and add to builder
  val ingestFuncs: Array[RowReader => Unit] = builders.zipWithIndex.map {
    case (b: IntColumnBuilder,    c) => (r: RowReader) => b.addData(r.getInt(c))
    case (b: LongColumnBuilder,   c) => (r: RowReader) => b.addData(r.getLong(c))
    case (b: DoubleColumnBuilder, c) => (r: RowReader) => b.addData(r.getDouble(c))
    case (b: StringColumnBuilder, c) => (r: RowReader) => b.addData(r.getString(c))
  }.toArray

  /**
   * Resets the ColumnBuilders.  Call this before the next batch of rows to transpose.
   * @return {[type]} [description]
   */
  def reset(): Unit = {
    builders.foreach(_.reset())
  }

  /**
   * Adds a single row of data to each of the ColumnBuilders.
   * @param row the row of data to transpose.  Each column will be added to the right Builders.
   */
  def addRow(row: RowReader): Unit = {
    for { i <- 0 until numColumns optimized } {
      if (row.notNull(i)) { ingestFuncs(i)(row) }
      else                { builders(i).addNA }
    }
  }

  /**
   * Adds a single blank NA value to all builders
   */
  def addEmptyRow(): Unit = {
    builders.foreach(_.addNA())
  }

  /**
   * Converts the contents of the [[ColumnBuilder]]s to ByteBuffers for writing or transmission.
   * @param hint an EncodingHint for the encoder
   */
  def convertToBytes(hint: EncodingHint = AutoDetect): Map[String, ByteBuffer] = {
    val chunks = builders.map {
      case b: IntColumnBuilder    => BuilderEncoder.builderToBuffer(b, hint)
      case b: LongColumnBuilder   => BuilderEncoder.builderToBuffer(b, hint)
      case b: DoubleColumnBuilder => BuilderEncoder.builderToBuffer(b, hint)
      case b: StringColumnBuilder => BuilderEncoder.builderToBuffer(b, hint)
    }
    schema.zip(chunks).map { case (IngestColumn(colName, _), bytes) => (colName, bytes) }.toMap
  }

  private def unsupportedInput(typ: Any) =
    throw new RuntimeException("Unsupported input type " + typ)
}

