package org.velvia.filo

import java.nio.ByteBuffer

import scala.language.existentials

case class IngestColumn(name: String, builder: ColumnBuilder[_])

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

/**
 * Class to help transpose a set of rows of type R to ByteBuffer-backed columns.
 * @param schema a Seq of IngestColumn describing the [[ColumnBuilder]] used for each column
 * @param ingestSupport something to convert from a row R to specific types
 *
 * TODO: Add stats about # of rows, chunks/buffers encoded, bytes encoded, # NA's etc.
 */
class RowToColumnBuilder[R](schema: Seq[IngestColumn], ingestSupport: RowIngestSupport[R]) {
  val ingestFuncs: Seq[(R, Int) => Unit] = schema.map { case IngestColumn(_, builder) =>
    builder.classTagA.runtimeClass match {
      case Classes.Int    =>
        (r: R, c: Int) => builder.asInstanceOf[IntColumnBuilder].addOption(ingestSupport.getInt(r, c))
      case Classes.Long   =>
        (r: R, c: Int) => builder.asInstanceOf[LongColumnBuilder].addOption(ingestSupport.getLong(r, c))
      case Classes.Double =>
        (r: R, c: Int) => builder.asInstanceOf[DoubleColumnBuilder].addOption(ingestSupport.getDouble(r, c))
      case Classes.String =>
        (r: R, c: Int) => builder.asInstanceOf[StringColumnBuilder].addOption(ingestSupport.getString(r, c))
      case x: Any         => unsupportedInput(x)
    }
  }

  /**
   * Resets the ColumnBuilders.  Call this before the next batch of rows to transpose.
   * @return {[type]} [description]
   */
  def reset(): Unit = {
    schema foreach { _.builder.reset() }
  }

  /**
   * Adds a single row of data to each of the ColumnBuilders.
   * @param row the row of data to transpose.  Each column will be added to the right Builders.
   */
  def addRow(row: R): Unit = {
    ingestFuncs.zipWithIndex.foreach { case (func, i) =>
      func(row, i)
    }
  }

  /**
   * Converts the contents of the [[ColumnBuilder]]s to ByteBuffers for writing or transmission.
   */
  def convertToBytes(): Map[String, ByteBuffer] = {
    schema.map { case IngestColumn(columnName, builder) =>
      val bytes = builder.classTagA.runtimeClass match {
        case Classes.Int    =>
          BuilderEncoder.builderToBuffer(builder.asInstanceOf[IntColumnBuilder])
        case Classes.Long   =>
          BuilderEncoder.builderToBuffer(builder.asInstanceOf[LongColumnBuilder])
        case Classes.Double =>
          BuilderEncoder.builderToBuffer(builder.asInstanceOf[DoubleColumnBuilder])
        case Classes.String =>
          BuilderEncoder.builderToBuffer(builder.asInstanceOf[StringColumnBuilder])
        case x: Any         => unsupportedInput(x)
      }
      (columnName, bytes)
    }.toMap
  }

  private def unsupportedInput(typ: Any) =
    throw new RuntimeException("Unsupported input type " + typ)
}

