# filo
![filo](Filo.jpg)

A thin layer of dough for baking fast, memory-efficient, minimal-deserialization, binary data vectors into your app.

Think of it as the good parts of Parquet without the HDFS and file format cruft -- just the serdes and fast columnar storage.

For Scala, you get `Seq[A]` or `Traversable[A]` APIs directly on top of binary vectors, with minimal/lazy deserialization.

The Scala implementation IntColumns have been clocked at 2 billion integer reads per second per thread using JMH on my laptop.

## Properties

* A [wire format](wire_format.md) for efficient data vectors for reading with zero or minimal/lazy deserialization
    - Very compact and fast string vectors using cached dictionary encoding
    - Numeric vectors stored using smaller number of bits if possible
* Random or linear access, no need to deserialize everything for random access
* Support for missing / Not Available values, even for primitive vectors
* Trade off between read speed and compactness -- Dictionary encoding, delta encoding, other techniques
* Designed for long term persistence - based on Google [FlatBuffers](https://github.com/google/flatbuffers) which has schema evolution
* Potentially cross-platform - once FlatBuffers and codecs are written

Perfect for efficiently representing your data for storing in files, mmap, NoSQL or key-value stores, etc. etc.

## Current Status

Wire format is stable; header bytes enable future expansion into even non-FlatBuffer based binary formats.

## Getting Started

1. Clone the Google Flatbuffers [repo](https://github.com/google/flatbuffers).
1. Install cmake - on OSX: `brew install cmake`
1. `cmake -G "Unix Makefiles"`
2. Run `make` at the root of the flatbuffers dir
3. Put the `flatc` compiler binary in your path

## Filo-Scala

Get it here:

    resolvers += "Velvia Bintray" at "https://dl.bintray.com/velvia/maven"

    libraryDependencies += "org.velvia.filo" % "filo-scala" % "0.1.1"

Using a `ColumnBuilder` to progressively build a column:

```scala
scala> import org.velvia.filo._
import org.velvia.filo._

scala> val cb = new IntColumnBuilder
cb: org.velvia.filo.IntColumnBuilder = org.velvia.filo.IntColumnBuilder@48cbb760

scala> cb.addNA

scala> cb.addData(101)

scala> cb.addData(102)

scala> cb.addData(103)

scala> cb.addNA
```

Encoding it to a `ByteBuffer`:

```scala
scala> BuilderEncoder.builderToBuffer(cb)
res6: java.nio.ByteBuffer = java.nio.HeapByteBuffer[pos=65408 lim=65536 cap=65536]
```

Parsing and iterating through the ByteBuffer as a collection:

```scala
scala> ColumnParser.parse[Int](res6).foreach(println)
101
102
103
```

All `ColumnWrappers` implement `scala.collection.Traversable` for transforming
and iterating over the non-missing elements of a Filo binary vector.  There are
also methods for accessing and iterating over all elements.

### Converting rows to Filo columnar chunks

Please see `RowToColumnBuilder` and the `RowToColumnBuilderTest` for an example.
There is a convenience function to convert a whole bunch of rows at once.

Also see `FiloRowReader` for extracting rows out of a bunch of heterogeneous Filo vectors.  Both this and the `RowToColumnBuilder` works with `RowReader`s, to facilitate composing rows to and from Filo vectors.

### Support for Seq[A] and Seq[Option[A]]

You can also encode a `Seq[A]` to a buffer easily:

```scala
scala> import org.velvia.filo._
import org.velvia.filo._

scala> val orig = Seq(1, 2, -5, 101)
orig: Seq[Int] = List(1, 2, -5, 101)

scala> val buf = BuilderEncoder.seqToBuffer(orig)
buf: java.nio.ByteBuffer = java.nio.HeapByteBuffer[pos=65432 lim=65536 cap=65536]

scala> val binarySeq = ColumnParser.parse[Int](buf)
binarySeq: org.velvia.filo.ColumnWrapper[Int] = SimpleColumnWrapper(1, 2, -5, 101)

scala> binarySeq.sum == orig.sum
res2: Boolean = true
```

Note that even though a `ColumnWrapper` implements `Traversable`, it only
traverses over defined elements that are not NA.  To work with collections of
potentially missing elements, start with a `Seq[Option[A]]`, then use
`BuilderEncoder.seqOptionToBuffer`.  You can extract out an
`Iterator[Option[A]]` with the `optionIterator` method.

### Performance Benchmarking

To just get overall run times:

    sbt filoScalaJmh/run

To also get profiling of top methods:

    sbt filoScalaJmh/run -prof stack -jvmArgsAppend -Djmh.stack.lines=3

See this [gist](https://gist.github.com/velvia/213b837c6e02c4982a9a) for how I improved the `ColumnWrapper.apply()` method performance by 50x.
 
## Future directions

Cross-platform support - Go, C/C++, etc.

### Additional Encodings

Still random:
* A much more compact encoding for sparse values
* Delta encoding - but to allow for random, set central value to average or first value
* Combo delta + pack into float for double vector compression
* Use [JavaEWAH](https://github.com/lemire/javaewah) `ImmutableBitSet` for efficient compressed bit vectors / NA masks
* Encode a set or a hash, perhaps using Murmur3 hash for keys with an open hash design
* Encode other data structures in [Open Data Structures](http://opendatastructures.org/)... a BTree would be fun

No longer zero serialization:
* Use the super fast byte packing algorithm from Cap'n Proto for much smaller wire representation
* [Jsmaz](https://github.com/RyanAD/jsmaz) and [Shoco](http://ed-von-schleck.github.io/shoco/) for small string compression
* [JavaFastPFor](https://github.com/lemire/JavaFastPFOR) for integer array compression

### General Compression

My feeling is that we don't need general compression algorithms like LZ4,
Snappy, etc.  (An interesting new one is
[Z-STD](http://fastcompression.blogspot.fr/2015/01/zstd-stronger-compression-
algorithm.html?m=1)).  The whole goal of this project is to be able to read from
disk or database with minimal or no deserialization / decompression step.  Many
databases, such as Cassandra, already default to some kind of on-disk
compression as well.
