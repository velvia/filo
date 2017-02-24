package org.velvia.filo

import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.{Mode, State, Scope}
import scala.language.postfixOps
import scalaxy.loops._

import org.velvia.filo.vectors._

/**
 * Measures the speed of encoding different types of data,
 * including just Filo vector encoding and encoding from RowReaders.
 *
 * For a description of the JMH measurement modes, see
 * https://github.com/ktoso/sbt-jmh/blob/master/src/sbt-test/sbt-jmh/jmh-run/src/main/scala/org/openjdk/jmh/samples/JMHSample_02_BenchmarkModes.scala
 */
@State(Scope.Thread)
class EncodingBenchmark {
  import BuilderEncoder.SimpleEncoding
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
  def simpleStringEncoding(): Unit = {
    VectorBuilder(randomStrings).toFiloBuffer(SimpleEncoding)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def intVectorEncoding(): Unit = {
    VectorBuilder(randomInts).toFiloBuffer
  }

  val intArray = randomInts.toArray

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def newIntVectorEncoding(): Unit = {
    val cb = IntBinaryVector.appendingVector(numValues)
    for { i <- 0 until numValues optimized } {
      cb.addData(intArray(i))
    }
    IntBinaryVector.optimize(cb).toFiloBuffer
  }

  val utf8strings = randomStrings.map(ZeroCopyUTF8String.apply).toArray

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def newUtf8VectorEncoding(): Unit = {
    val cb = UTF8Vector.appendingVector(numValues, 16 + numValues * 20)
    for { i <- 0 until numValues optimized } {
      cb.addData(utf8strings(i))
    }
    cb.toFiloBuffer()
  }
  // TODO: RowReader based vector building

  val utf8cb = UTF8Vector.appendingVector(numValues, 16 + numValues * 20)
  for { i <- 0 until numValues optimized } {
    utf8cb.addData(utf8strings(i))
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def newUtf8AddVector(): Unit = {
    val cb = UTF8Vector.appendingVector(numValues, 16 + numValues * 20)
    cb.addVector(utf8cb)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def newDictUtf8VectorEncoding(): Unit = {
    UTF8Vector.writeOptimizedBuffer(utf8strings, samplingRate=0.5)
  }
}