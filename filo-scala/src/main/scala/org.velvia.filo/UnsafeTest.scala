package org.velvia.filo

import sun.misc.Unsafe
import java.nio.ByteBuffer

object UnsafeTest extends App {
  val buf = ByteBuffer.wrap(Array[Byte](0, 1, 2, 3, 4, 5))
  val aray = buf.array()

  val field = classOf[Unsafe].getDeclaredField("theUnsafe")
  field.setAccessible(true)
  val unsafe = field.get(null).asInstanceOf[Unsafe]

  val arayOffset = unsafe.arrayBaseOffset(classOf[Array[Byte]])

  //scalastyle:off
  for (i <- 0 to 3) {
    println(unsafe.getByte(aray, arayOffset + i.toLong))
  }
  //scalastyle:on
}