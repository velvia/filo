package org.velvia.filo

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.{Mode, State, Scope}
import org.openjdk.jmh.annotations.OutputTimeUnit
import scalaxy.loops._
import scala.language.postfixOps

import java.util.concurrent.TimeUnit

/**
 * Measures read speed for a dictionary-encoded string Filo column.
 * Has tests for both no-NA and some-NA read speed.
 * Since real world datasets tend to contain lots of string data, this is
 * probably a much more realistic speed benchmark than Ints.
 * Simulate a somewhat-realistic by varying string length and using alphanum chars
 *
 * TODO: compare against Seq[String] encoding in MessagePack, etc.
 */
@State(Scope.Thread)
class DictStringBenchmark {
  import scala.util.Random.{alphanumeric, nextInt, nextFloat}
  import VectorReader._

  val numValues = 10000
  // NOTE: results show that time spent is heavily influenced by ratio of unique strings...
  val numUniqueStrings = 500
  val maxStringLength = 15
  val minStringLength = 5
  val naChance = 0.05    //5% of values will be NA

  def randString(len: Int): String = alphanumeric.take(len).mkString

  val uniqueStrings = (0 until numUniqueStrings).map { i =>
    randString(minStringLength + nextInt(maxStringLength - minStringLength))
  }
  val randomStrings = (0 until numValues).map(i => uniqueStrings(nextInt(numUniqueStrings)))
  val filoBufferNoNA = VectorBuilder(randomStrings).toFiloBuffer
  val scNoNA = FiloVector[String](filoBufferNoNA)

  def shouldNA: Boolean = nextFloat < naChance

  val filoBufferNA = VectorBuilder.fromOptions(
                       randomStrings.map(str => if (shouldNA) None else Some(str))
                     ).toFiloBuffer
  val scNA = FiloVector[String](filoBufferNA)

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def rawStringLengthTotal(): Int = {
    var totalLen = 0
    for { i <- 0 until numValues optimized } {
      totalLen += scNoNA(i).length
    }
    totalLen
  }

  // TODO: also a benchmark for the foreach/fold of a column with no NA's?

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  // Measures foreach and NA read speed
  def withNAlengthTotal(): Unit = {
    var totalLen = 0
    scNA.foreach { str => totalLen += str.length }
    totalLen
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  // Measures encoding speed
  def encoding(): Unit = {
    VectorBuilder(randomStrings).toFiloBuffer
  }
}