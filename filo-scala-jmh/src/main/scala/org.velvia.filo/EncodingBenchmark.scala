package org.velvia.filo

import java.sql.Timestamp
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.{Mode, State, Scope}
import org.openjdk.jmh.annotations.OutputTimeUnit
import scalaxy.loops._
import scala.language.postfixOps

import java.util.concurrent.TimeUnit

/**
 * Measures the speed of encoding different types of data,
 * including just Filo vector encoding and encoding from RowReaders.
 *
 * For a description of the JMH measurement modes, see
 * https://github.com/ktoso/sbt-jmh/blob/master/src/sbt-test/sbt-jmh/jmh-run/src/main/scala/org/openjdk/jmh/samples/JMHSample_02_BenchmarkModes.scala
 */
@State(Scope.Thread)
class EncodingBenchmark {
  import scala.util.Random.{alphanumeric, nextInt, nextFloat}
  import VectorReader._

  // Ok, create an IntColumn and benchmark it.
  val numValues = 10000

  val randomInts = (0 until numValues).map(i => util.Random.nextInt)
  val randomLongs = randomInts.map(_.toLong)

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

  val chunks = Array(VectorBuilder(randomInts).toFiloBuffer,
                     VectorBuilder(randomLongs).toFiloBuffer,
                     VectorBuilder(randomStrings).toFiloBuffer)
  val clazzes = Array[Class[_]](classOf[Int], classOf[Long], classOf[String])

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  // Measures encoding speed of strings that are often repeated
  def dictStringEncoding(): Unit = {
    VectorBuilder(randomStrings).toFiloBuffer
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def intVectorEncoding(): Unit = {
    VectorBuilder(randomInts).toFiloBuffer
  }

  // TODO: RowReader based vector building
}