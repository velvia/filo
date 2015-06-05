package org.velvia.filo

/**
 * Sets the value in row of type R at position index (0=first column)
 * NOTE: This is definitely designed for performance/mutation rather than
 * functional purity.  In fact it's designed with Spark's Row trait in mind.
 */
trait RowSetter[R] {
  def setInt(row: R, index: Int, data: Int): Unit
  def setLong(row: R, index: Int, data: Long): Unit
  def setDouble(row: R, index: Int, data: Double): Unit
  def setString(row: R, index: Int, data: String): Unit

  def setNA(row: R, index: Int): Unit
}

/**
 * An example RowSetter good for creating string output such as for opencsv.
 */
object ArrayStringRowSetter extends RowSetter[Array[String]] {
  def setInt(row: Array[String], index: Int, data: Int): Unit = {
    row(index) = data.toString
  }

  def setLong(row: Array[String], index: Int, data: Long): Unit = {
    row(index) = data.toString
  }

  def setDouble(row: Array[String], index: Int, data: Double): Unit = {
    // If we really need performance here, use grisu.scala
    row(index) = data.toString
  }

  def setString(row: Array[String], index: Int, data: String): Unit = {
    row(index) = data
  }

  def setNA(row: Array[String], index: Int): Unit = { row(index) = "" }
}