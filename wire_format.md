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
| 0x00000202  | SimpleBinaryVector | FlatBuffer Vector with variable-size binary elements |
| 0x00000302  | SimpleFixedStringVector | Actually a SimplePrimitiveVector for string vectors where max string len < 32 bytes |
| 0x00000103  | DictStringVector   | FlatBuffer Vector with dictionary-encoded strings |
| 0x00000004  | ConstPrimitiveVector  | A SimplePrimitiveVector for representing primitives holding the same value for entire vector
| 0x00000104  | ConstStringVector     | Same string vector for entire vector |
| 0x00000005  | DiffPrimitiveVector   | Stores base + deltas for primitives for more compact representation |
| 0x00000405  | DiffDateTimeVector   | Delta-encoded timestamp with TZ info for joda.time.DateTime |

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

## SimpleFixedStringVector

`SimpleStringVector` is actually inefficient for strings less than about 16 chars long, or string columns where the strings are all the same size.  This is because a `[string]` vector has 8 bytes of overhead compared to a primitive fixed array: a string vector is an array of int offsets to a `[byte]` vector, which is itself a 4-byte length element plus the bytes.

The `SimpleFixedStringVector` uses a `SimplePrimitiveVector` for short strings and stores them inline in the data vector itself.  The first byte is a length field, followed by the UTF8 bytes.  `nbits` is set to `(longest string length + 1) * 8`.  Another benefit is better cache efficiency.

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
