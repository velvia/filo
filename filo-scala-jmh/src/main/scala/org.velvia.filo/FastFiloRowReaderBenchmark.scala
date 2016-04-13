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
 * Measures the speed of creating a FastFiloRowReader,
 * parsing the chunks into FiloVectors, and iterating through rows and reading values.
 *
 * For a description of the JMH measurement modes, see
 * https://github.com/ktoso/sbt-jmh/blob/master/src/sbt-test/sbt-jmh/jmh-run/src/main/scala/org/openjdk/jmh/samples/JMHSample_02_BenchmarkModes.scala
 */
@State(Scope.Thread)
class FastFiloRowReaderBenchmark {
  import VectorReader._

  // Ok, create an IntColumn and benchmark it.
  val numValues = 10000

  val randomInts = (0 until numValues).map(i => util.Random.nextInt)
  val randomLongs = randomInts.map(_.toLong)
  val randomTs = randomLongs.map(l => new Timestamp(l))

  val chunks = Array(VectorBuilder(randomInts).toFiloBuffer,
                     VectorBuilder(randomLongs).toFiloBuffer,
                     VectorBuilder(randomTs).toFiloBuffer)
  val clazzes = Array[Class[_]](classOf[Int], classOf[Long], classOf[Timestamp])

  // According to @ktosopl, be sure to return some value if possible so that JVM won't
  // optimize out the method body.  However JMH is apparently very good at avoiding this.
  // fastest loop possible using FiloVectorApply method
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def createFastFiloRowReader(): RowReader = {
    new FastFiloRowReader(chunks, clazzes)
  }

  val fastReader = new FastFiloRowReader(chunks, clazzes)

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def fastFiloRowReaderReadOne(): Int = {
    fastReader.setRowNo(0)
    if (fastReader.notNull(0)) fastReader.getInt(0) + 1 else 0
  }
}