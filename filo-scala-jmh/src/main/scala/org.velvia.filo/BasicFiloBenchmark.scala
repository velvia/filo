package org.velvia.filo

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.{Mode, State, Scope}
import org.openjdk.jmh.annotations.OutputTimeUnit
import scalaxy.loops._
import scala.language.postfixOps

import java.util.concurrent.TimeUnit

/**
 * Measures basic read benchmark with no NAs for an IntColumn.
 * Just raw read speed basically.
 * Measures read speed of different encodings (int, byte, diff) as well as
 * different read methods.
 *
 * For a description of the JMH measurement modes, see
 * https://github.com/ktoso/sbt-jmh/blob/master/src/sbt-test/sbt-jmh/jmh-run/src/main/scala/org/openjdk/jmh/samples/JMHSample_02_BenchmarkModes.scala
 */
@State(Scope.Thread)
class BasicFiloBenchmark {
  import VectorReader._

  // Ok, create an IntColumn and benchmark it.
  val numValues = 10000

  val randomInts = (0 until numValues).map(i => util.Random.nextInt)
  val randomIntsAray = randomInts.toArray
  val filoBuffer = VectorBuilder(randomInts).toFiloBuffer
  val sc = FiloVector[Int](filoBuffer)

  val byteFiloBuf = VectorBuilder(randomInts.map(_ % 128)).toFiloBuffer
  val byteVect = FiloVector[Int](byteFiloBuf)

  val diffFiloBuf = VectorBuilder(randomInts.map(10000 + _ % 128)).toFiloBuffer
  val diffVect = FiloVector[Int](diffFiloBuf)

  // According to @ktosopl, be sure to return some value if possible so that JVM won't
  // optimize out the method body.  However JMH is apparently very good at avoiding this.
  // fastest loop possible using FiloVectorApply method
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllIntsFiloApply(): Int = {
    var total = 0
    for { i <- 0 until numValues optimized } {
      total += sc(i)
    }
    total
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllIntsFiloByteApply(): Int = {
    var total = 0
    for { i <- 0 until numValues optimized } {
      total += byteVect(i)
    }
    total
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllIntsFiloDiffApply(): Int = {
    var total = 0
    for { i <- 0 until numValues optimized } {
      total += diffVect(i)
    }
    total
  }

  // sum which uses foreach from FiloVector
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllIntsFiloForeachFoldLeft(): Int = {
    sc.foldLeft(0)(_ + _)
  }
}
