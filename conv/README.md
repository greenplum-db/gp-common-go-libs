### Conv functions

This package provides utility functions for converting golang data types with high performance.
These functions pursuit maximum performance for building data-intensive applications, at the cost of versatility
compared with golang native functions.

For example, golang native function like `fmt.Sprintf` or `strconv.Itoa` do typecast and may alloc slices inside,
which is costly and not cache-line friendly.

In this package, you can find some specialized functions crafted with care. Although a little bit annoying when
using because you need to prepare a byte array outside as buffer, when your program processing thousands
of millions of tuples to an io.Writer, it enables to reuse that pre-allocated buffer, pressing the throughput to times
over golang native functions.

You can find benchmarks at the bottom of each *_test.go file.
To run the benchmark
```
go test -bench=. -benchmem -cpu=1
```

If your program only does conversion on a small number of samples, you probably don't need this package.
Use golang native way is good enough. This package is beneficial for a program
(1) processing a large number of data
(2) performance is a priority

#### Example

```
var buff = [4]byte{}
for i := 0; i < 10000000; i++ {
    s := conv.Int8ToBytes(input[i], &buff)
    writer.Write(s)
}
```