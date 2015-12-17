package org.velvia.filo

/**
 * Filo wire format definitions - especially for the header bytes.
 * See [wire_format.md] for details.
 */
object WireFormat {
  val VECTORTYPE_EMPTY = 0x01
  val VECTORTYPE_SIMPLE = 0x02
  val VECTORTYPE_DICT = 0x03
  val VECTORTYPE_CONST = 0x04
  val VECTORTYPE_DIFF = 0x05

  def majorVectorType(headerBytes: Int): Int = headerBytes & 0x00ff
  def emptyVectorLen(headerBytes: Int): Int = {
    require(majorVectorType(headerBytes) == VECTORTYPE_EMPTY)
    java.lang.Integer.rotateRight(headerBytes & 0xffffff00, 8)
  }

  val SUBTYPE_PRIMITIVE = 0x00
  val SUBTYPE_STRING = 0x01
  val SUBTYPE_BINARY = 0x02
  val SUBTYPE_FIXEDSTRING = 0x03
  val SUBTYPE_DATETIME = 0x04

  def vectorSubType(headerBytes: Int): Int = (headerBytes & 0x00ff00) >> 8

  val MaxEmptyVectorLen = 0x00ffffff

  def emptyVector(len: Int): Int = {
    require(len <= MaxEmptyVectorLen, "Vector len too long")
    (len << 8) | VECTORTYPE_EMPTY
  }

  def apply(majorVectorType: Int, subType: Int): Int =
    ((subType & 0x00ff) << 8) | (majorVectorType & 0x00ff)
}