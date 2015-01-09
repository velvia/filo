# filo
![filo](Filo.jpg)

A thin layer of dough for baking fast, memory-efficient, zero-serialization, binary data vectors into your app.

## Properties

* A wire format for efficient data vectors for reading with zero or minimal/lazy serialization
    - Native JVM strings require deserialization from UTF8 to JVM UTF16 native format.  This can be avoided if you read the raw bytes.
    - JVM Objects still get allocated for non-primitive types, but their memory cost can be reduced by pointing back at the raw bytes
* Random or linear access
* Support for missing / Not Available values, even for primitive vectors
* Trade off between read speed and compactness -- Dictionary encoding, delta encoding, other techniques
* Designed for long term persistence - based on Google [FlatBuffers](https://github.com/google/flatbuffers) which has ProtoBuf-like wire schema compatibility
* Potentially cross-platform - once FlatBuffers and codecs are written

Perfect for efficiently representing your data for storing in files, mmap, NoSQL or key-value stores, etc. etc.

## Current Status

Just started.  Consider the wire format not stable.  First priority is to stabilize wire format, then stabilize Scala APIs.

I considered Cap'n Proto, but that has some issues in the Java client that needs to be worked out first.

## Filo-Scala

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
scala> BuilderEncoder.encodeToBuffer(cb)
res6: java.nio.ByteBuffer = java.nio.HeapByteBuffer[pos=65408 lim=65536 cap=65536]
```

Parsing and iterating through the ByteBuffer as a collection:

```scala
scala> ColumnParser.parseAsSimpleColumn(res6).foreach(println)
101
102
103
```

All `ColumnWrappers` implement `scala.collection.Traversable` for transforming
and iterating over the non-missing elements of a Filo binary vector.  There are
also methods for accessing and iterating over all elements.

## Future directions

### Additional Encodings

Still random:
* A much more compact encoding for sparse values
* Delta encoding - but to allow for random, set central value to average or first value
* Combo delta + pack into float for double vector compression
* Use [JavaEWAH](https://github.com/lemire/javaewah) `ImmutableBitSet` for efficient compressed bit vectors / NA masks
* Encode a set or a hash, perhaps using Murmur3 hash for keys with an open hash design

No longer zero serialization:
* Use the super fast byte packing algorithm from Cap'n Proto for much smaller wire representation
* [Jsmaz](https://github.com/RyanAD/jsmaz) and [Shoco](http://ed-von-schleck.github.io/shoco/) for small string compression
* [JavaFastPFor](https://github.com/lemire/JavaFastPFOR) for integer array compression