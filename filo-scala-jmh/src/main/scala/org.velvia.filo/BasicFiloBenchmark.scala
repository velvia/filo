package org.velvia.filo

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.{Mode, State, Scope}
import org.openjdk.jmh.annotations.OutputTimeUnit

import java.util.concurrent.TimeUnit

/**
 * Measures basic read benchmark with no NAs for an IntColumn.
 * Just raw read speed basically.
 *
 * For a description of the JMH measurement modes, see
 * https://github.com/ktoso/sbt-jmh/blob/master/src/sbt-test/sbt-jmh/jmh-run/src/main/scala/org/openjdk/jmh/samples/JMHSample_02_BenchmarkModes.scala
 */
@State(Scope.Thread)
class BasicFiloBenchmark {
  import ColumnParser._

  // Ok, create an IntColumn and benchmark it.
  val numValues = 10000

  val randomInts = (0 until numValues).map(i => util.Random.nextInt)
  val filoBuffer = BuilderEncoder.seqToBuffer(randomInts)
  val sc = ColumnParser.parse[Int](filoBuffer)

  // According to @ktosopl, be sure to return some value if possible so that JVM won't
  // optimize out the method body.  However JMH is apparently very good at avoiding this.
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllInts(): Int = {
    var i = 0
    var total = 0
    while (i < numValues) {
      total += sc(i)
      i += 1
    }
    total
  }
}
