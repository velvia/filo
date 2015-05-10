package org.velvia.filo

import com.google.flatbuffers.FlatBufferBuilder
import framian.column.{Mask, MaskBuilder}
import java.nio.ByteBuffer
import org.velvia.filo.column._
import scala.reflect.ClassTag

/**
 * A bunch of builders for row-oriented ingestion to create columns in parallel
 * Use these for support of missing/NA values
 * @param empty The empty value to insert for an NA or missing value
 *
 * TODO: Either fully embrace Framian and use their builders, or have no dependency on them
 */
sealed abstract class ColumnBuilder[A](empty: A)(implicit val classTagA: ClassTag[A]) {
  // True for a row number (or bit is part of the set) if data for that row is not available
  val naMask = new MaskBuilder
  val data = new collection.mutable.ArrayBuffer[A]

  def addNA(): Unit = {
    naMask += data.length
    data += empty
  }

  def addData(value: A): Unit = { data += value }

  def addOption(value: Option[A]): Unit = {
    value.foreach { v => addData(v) }
    value.orElse  { addNA(); None }
  }

  def reset(): Unit = {
    naMask.clear
    data.clear
  }
}

// Please add your builder here when you add a type
object ColumnBuilder {
  def apply(dataType: Class[_]): ColumnBuilder[_] = dataType match {
    case Classes.Int => new IntColumnBuilder
    case Classes.Long => new LongColumnBuilder
    case Classes.Double => new DoubleColumnBuilder
    case Classes.String => new StringColumnBuilder
  }
}

class IntColumnBuilder extends ColumnBuilder(0)
class LongColumnBuilder extends ColumnBuilder(0L)
class DoubleColumnBuilder extends ColumnBuilder(0.0)
class StringColumnBuilder extends ColumnBuilder("") {
  // For dictionary encoding. NOTE: this set does NOT include empty value
  val stringSet = new collection.mutable.HashSet[String]

  override def addData(value: String): Unit = {
    stringSet += value
    super.addData(value)
  }

  override def reset(): Unit = {
    stringSet.clear
    super.reset()
  }
}
