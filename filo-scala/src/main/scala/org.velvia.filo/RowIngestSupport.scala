package org.velvia.filo

import scala.util.Try

/**
 * Must be implemented for proper building of columns from rows
 */
trait RowIngestSupport[R] {
  def getString(row: R, columnNo: Int): Option[String]
  def getInt(row: R, columnNo: Int): Option[Int]
  def getLong(row: R, columnNo: Int): Option[Long]
  def getDouble(row: R, columnNo: Int): Option[Double]
}

/**
 * Just an example of extracting from a Tuple or Case Class where every element is an Option[T].
 * Used mostly for tests.
 */
object TupleRowIngestSupport extends RowIngestSupport[Product] {
  type R = Product
  def getString(row: R, columnNo: Int): Option[String] =
    row.productElement(columnNo).asInstanceOf[Option[String]]
  def getInt(row: R, columnNo: Int): Option[Int] =
    row.productElement(columnNo).asInstanceOf[Option[Int]]
  def getLong(row: R, columnNo: Int): Option[Long] =
    row.productElement(columnNo).asInstanceOf[Option[Long]]
  def getDouble(row: R, columnNo: Int): Option[Double] =
    row.productElement(columnNo).asInstanceOf[Option[Double]]
}

/**
 * Define a RowIngestSupport for a String[], such as that parsed by OpenCSV
 * Throws exceptions if value cannot parse - better to fail fast than silently create an NA value
 */
object ArrayStringRowSupport extends RowIngestSupport[Array[String]] {
  private def maybeString(row: Array[String], index: Int): Option[String] =
    Try(row(index)).toOption.filter(_.nonEmpty)

  def getString(row: Array[String], columnNo: Int): Option[String] =
    maybeString(row, columnNo)
  def getInt(row: Array[String], columnNo: Int): Option[Int] =
    maybeString(row, columnNo).map(_.toInt)
  def getLong(row: Array[String], columnNo: Int): Option[Long] =
    maybeString(row, columnNo).map(_.toLong)
  def getDouble(row: Array[String], columnNo: Int): Option[Double] =
    maybeString(row, columnNo).map(_.toDouble)
}