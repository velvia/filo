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
| 0x00000103  | DictStringVector   | FlatBuffer Vector with dictionary-encoded strings |
| 0x00000004  | RLEPrimitiveVector | FlatBuffer Run-Length-Encoded vector for primitives |

## Data vector type

Many of the vectors consists of fixed-size element arrays.  These data vectors are, for flexibility and compactness, not represented with the native FlatBuffer arrays, but rather with two elements:

* nbits - a u8 representing the number of bits per element
* data - a u8 array with each element aligned to every nbits, stored in little endian order.
    - For example, if nbits was 4, and the array was [1, 2, 3, 4], then in network / file byte order, the bytes would be 0x21 0x43

## EmptyVector

Nothing follows the 4 header bytes since the length is already captured there.

## Simple*Vector

The FlatBuffers buffer for each type follows the header bytes.  See the *.fbs files for exact definition.

## DictStringVector

Here, each unique string is given a number, or code, starting from 1.
There are two inner vectors within DictStringVector:

* codes - represents the UTF8 string for each dictionary code.  The code 0 is reserved for null or NA, and represented with the empty string, so the code vector looks like this: `["", "firstUnique", ...]`
* data - represents the code for each string in the vector

## Future Formats

* Diff-based for integer and double values, if there is a tight range between min and max.
* Sparse element storage.  Store only the index and value, not every element.
