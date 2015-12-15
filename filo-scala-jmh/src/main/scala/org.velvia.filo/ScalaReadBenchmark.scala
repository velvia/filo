package org.velvia.filo

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.{Mode, State, Scope}
import org.openjdk.jmh.annotations.OutputTimeUnit
import scalaxy.loops._
import scala.language.postfixOps

import java.util.concurrent.TimeUnit

/**
 * Measures read speeds for Scala collections and Arrays for comparison
 * to Filo vectors.
 */
@State(Scope.Thread)
class ScalaReadBenchmark {
  // Ok, create an IntColumn and benchmark it.
  val numValues = 10000

  val randomInts = (0 until numValues).map(i => util.Random.nextInt)
  val randomIntsAray = randomInts.toArray

  // Scala Seq sum
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllIntsScalaSeqFoldLeft(): Int = {
    randomInts.foldLeft(0)(_ + _)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllIntsScalaArrayFoldLeft(): Int = {
    randomIntsAray.foldLeft(0)(_ + _)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllIntsScalaArrayWhileLoop(): Int = {
    var total = 0
    for { i <- 0 until numValues optimized } {
      total += randomIntsAray(i)
    }
    total
  }
}