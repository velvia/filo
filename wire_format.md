# Filo Wire Format

The Filo binary vector wire format is mostly based on FlatBuffer chunks, with some extra stuff around it.

## Header bytes

The first four bytes of any Filo chunk indicates the type of binary vector and structure following it.  This allows for future evolution.

| Offset | Type  | Description     |
|--------|-------|-----------------|
| +0     | u32   | Vector type     |

The valid values of the vector type:

| Value       | Vector Type    |  Description   |
|-------------|----------------|----------------|
| 0xnnnnnn01  | EmptyVector    | Every value in vector of length nnnnnn is NA |
| 0x00000002  | SimplePrimitiveVector | FlatBuffer Vector with fixed bit size per element |
| 0x00000102  | SimpleStringVector | FlatBuffer Vector with variable-size string elements |
| 0x00000103  | DictStringVector   | FlatBuffer Vector with dictionary-encoded strings |
| 0x00000004  | ConstPrimitiveVector  | A SimplePrimitiveVector for representing primitives holding the same value for entire vector
| 0x00000104  | ConstStringVector     | Same string vector for entire vector |
| 0x00000005  | DiffPrimitiveVector   | Stores base + deltas for primitives for more compact representation |
| 0x00000405  | DiffDateTimeVector   | Delta-encoded timestamp with TZ info for joda.time.DateTime |
| 0x00000006  | Primitive bitmap-mask vector | New-style BinaryVector with fixed bit size (primitives) per element and bitmask for NA |
| 0x00000206  | UTF8Vector | New-style BinaryVector with variable sized UTF8 strings or binary blobs |
| 0x00000306  | FixedMaxUTF8Vector | Actually a PrimitiveVector for UTF8/blob elements with a fixed max size and a size byte |
| 0x00000506  | PrimitiveAppendableVector | New-style BinaryVector with fixed bit size (primitives) per element and no NA mask |
| 0x00000207  | DictUTF8Vector  | New-style BinaryVector with dictionary-encoded UTF8/blobs |

See `WireFormat.scala` for code definitions.

## Data vector type

Many of the vectors consists of at least one fixed-size element arrays.  These data vectors are, for flexibility and compactness, not represented with the native FlatBuffer arrays, but rather with two elements:

* info - a struct with the following elements:
    * nbits - a u8 representing the number of bits per element
    * signed - a bool, true if the vector contains signed integers
* data - a u8 array with each element aligned to every nbits, stored in little endian order.
    - For example, if nbits was 4, and the array was [1, 2, 3, 4], then in file byte order, the bytes would be 0x21 0x43

The following types and standard nbits sizes should be supported by all Filo implementations:

| nbits |  Types supported   |
|-------|--------------------|
| 1     | bit vector         |
| 8     | byte / u8          |
| 16    | short int          |
| 32    | int, uint, IEEE Float |
| 64    | long, IEEE Double  |

## EmptyVector

Nothing follows the 4 header bytes since the length is already captured there.

## Simple*Vector

The FlatBuffers buffer for each type follows the header bytes.  See the *.fbs files for exact definition.

SimplePrimitiveVector contains a data vector as described above with nbits and signed struct, with a separate len field, while SimpleStringVector simply contains a [string].

Note that when implementing custom types, if the binary blobs are all fixed size, it is probably more efficient to use SimplePrimitiveVector - assuming the blobs are less than 256 bytes long each.

## FixedMaxUTF8Vector

`SimpleStringVector` is actually inefficient for strings less than about 16 chars long, or string columns where the strings are all the same size.  This is because a `[string]` vector has 8 bytes of overhead compared to a primitive fixed array: a string vector is an array of int offsets to a `[byte]` vector, which is itself a 4-byte length element plus the bytes.

`UTF8Vector` is much more space efficient than `SimpleStringVector` -- it stores 20 bits for offset and 11 bits for string length, and the strings are packed together and not word-aligned -- thus when strings fit into 1MB and are less than 2^11 bytes long, the overhead is just 4 bytes per string.  For fixed length strings though we can do better.

The `FixedMaxUTF8Vector` uses a `PrimitiveAppendableVector` for short strings and stores them inline in the data vector itself.  The first byte is a length field, followed by the UTF8 bytes.  `nbits` is set to `(longest string length + 1) * 8`.  Another benefit is better cache efficiency.

The actual equation for determining when simpleFixedString works well is this:

    (maxStringLen + 1) < (4 + 4 + avgStringLen)

## SimpleConstVector

This is really a SimplePrimitiveVector, in which the data vector contains one element only.  This vector logically represents this single element repeated *len* times.  The NA bitmask is still available.

## DictStringVector

Here, each unique string is given a number, or code, starting from 1.
There are two inner vectors within DictStringVector:

* codes - represents the UTF8 string for each dictionary code.  The code 0 is reserved for null or NA, and represented with the empty string, so the code vector looks like this: `["", "firstUnique", ...]`
* data - represents the code for each string in the vector

## Diff*Vector

Stores the base value as a separate single element vector, and all other elements as a vector of deltas from the base value.

DiffDateTimeVector is similar, storing the millis since Epoch as the data vector with a base value (since many timestamp vectors contains values relatively close together), with an optional TZ vector if the DateTimes differ in timezone.  Timezones are encoded as integer increments of 15 minute offsets from UTC (eg, UTC+1hr = 4).

## Future Formats

* Diff-based for double values, if there is a tight range between min and max.
* Sparse element storage.  Store only the index and value, not every element.
