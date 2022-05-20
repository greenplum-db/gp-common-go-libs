package conv_test

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	. "github.com/greenplum-db/gp-common-go-libs/conv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Integer Conversion functions", func() {
	Context("Int8ToBytes", func() {
		var b4 = [4]byte{}
		It("should convert int8", func() {
			Expect(Int8ToBytes(89, &b4)).To(Equal([]byte("89")))
		})
		It("should convert int8 edge", func() {
			Expect(Int8ToBytes(127, &b4)).To(Equal([]byte("127")))
		})
		It("should convert zero int8", func() {
			Expect(Int8ToBytes(0, &b4)).To(Equal([]byte("0")))
		})
		It("should convert negative zero int8", func() {
			Expect(Int8ToBytes(-0, &b4)).To(Equal([]byte("0")))
		})
		It("should convert negative int8", func() {
			Expect(Int8ToBytes(-89, &b4)).To(Equal([]byte("-89")))
		})
		It("should convert negative int8 edge", func() {
			Expect(Int8ToBytes(-128, &b4)).To(Equal([]byte("-128")))
		})
		It("should get same result with strconv.Itoa", func() {
			var i int8 = math.MinInt8
			for {
				Expect(Int8ToBytes(i, &b4)).To(Equal([]byte(strconv.Itoa(int(i)))))
				if i == math.MaxInt8 {
					break
				}
				i++
			}
		})
	})
	Context("Int16ToBytes", func() {
		var b6 = [6]byte{}
		It("should convert int16", func() {
			Expect(Int16ToBytes(8756, &b6)).To(Equal([]byte("8756")))
		})
		It("should convert int16 edge", func() {
			Expect(Int16ToBytes(32767, &b6)).To(Equal([]byte("32767")))
		})
		It("should convert zero int16", func() {
			Expect(Int16ToBytes(0, &b6)).To(Equal([]byte("0")))
		})
		It("should convert negative zero int16", func() {
			Expect(Int16ToBytes(-0, &b6)).To(Equal([]byte("0")))
		})
		It("should convert negative int16", func() {
			Expect(Int16ToBytes(-8756, &b6)).To(Equal([]byte("-8756")))
		})
		It("should convert negative int16 edge", func() {
			Expect(Int16ToBytes(-32768, &b6)).To(Equal([]byte("-32768")))
		})
		It("should get same result with strconv.Itoa", func() {
			var i int16 = math.MinInt16
			for {
				Expect(Int16ToBytes(i, &b6)).To(Equal([]byte(strconv.Itoa(int(i)))))
				if i == math.MaxInt16 {
					break
				}
				i++
			}
		})
	})
	Context("Int32ToBytes", func() {
		var b11 = [11]byte{}
		It("should convert int32", func() {
			Expect(Int32ToBytes(8756, &b11)).To(Equal([]byte("8756")))
		})
		It("should convert int32 edge", func() {
			Expect(Int32ToBytes(2147483647, &b11)).To(Equal([]byte("2147483647")))
		})
		It("should convert zero int32", func() {
			Expect(Int32ToBytes(0, &b11)).To(Equal([]byte("0")))
		})
		It("should convert negative zero int32", func() {
			Expect(Int32ToBytes(-0, &b11)).To(Equal([]byte("0")))
		})
		It("should convert negative int32", func() {
			Expect(Int32ToBytes(-8756, &b11)).To(Equal([]byte("-8756")))
		})
		It("should convert negative int32 edge", func() {
			Expect(Int32ToBytes(-2147483648, &b11)).To(Equal([]byte("-2147483648")))
		})
		It("random test int32", func() {
			var n int32
			var max int32 = math.MinInt32
			var min int32 = math.MaxInt32
			rand.Seed(time.Now().UnixNano())
			for i := 0; i < 100000; i++ {
				n = int32(rand.Intn(math.MaxInt32-math.MinInt32) + math.MinInt32)
				Expect(Int32ToBytes(n, &b11)).To(Equal([]byte(strconv.Itoa(int(n)))))
				if max < n {
					rand.Seed(time.Now().UnixNano())
					max = n
				}
				if min > n {
					rand.Seed(time.Now().UnixNano())
					min = n
				}
			}
			fmt.Println("Random test for Int32ToBytes,", min, "to", max)
		})
	})
	Context("Int64ToBytes", func() {
		var b20 = [20]byte{}
		It("should convert int64", func() {
			Expect(Int64ToBytes(1234567890, &b20)).To(Equal([]byte("1234567890")))
		})
		It("should convert int64 edge", func() {
			Expect(Int64ToBytes(9223372036854775807, &b20)).To(Equal([]byte("9223372036854775807")))
		})
		It("should convert zero int64", func() {
			Expect(Int64ToBytes(0, &b20)).To(Equal([]byte("0")))
		})
		It("should convert negative zero int64", func() {
			Expect(Int64ToBytes(-0, &b20)).To(Equal([]byte("0")))
		})
		It("should convert negative int64", func() {
			Expect(Int64ToBytes(-1234567890, &b20)).To(Equal([]byte("-1234567890")))
		})
		It("should convert negative int64 edge", func() {
			Expect(Int64ToBytes(-9223372036854775808, &b20)).To(Equal([]byte("-9223372036854775808")))
		})
		It("random test int64", func() {
			var n int64
			var r uint64
			var max int64 = math.MinInt64
			var min int64 = math.MaxInt64
			rand.Seed(time.Now().UnixNano())
			for i := 0; i < 1000000; i++ {
				r = rand.Uint64()
				if r > math.MaxInt64 {
					n = int64(r - uint64(-math.MinInt64))
				} else {
					n = int64(r) + math.MinInt64
				}
				Expect(Int64ToBytes(n, &b20)).To(Equal([]byte(fmt.Sprintf("%d", n))))
				if max < n {
					rand.Seed(time.Now().UnixNano())
					max = n
				}
				if min > n {
					rand.Seed(time.Now().UnixNano())
					min = n
				}
			}
			fmt.Println("Random test for Int64ToBytes,", min, "to", max)
		})
	})
})

/*
 * Int8 conversion benchmark
 * BenchmarkInt8ToByte            355891632         3.33 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt8Itoa              46987203         25.7  ns/op       4 B/op       1 allocs/op
 * BenchmarkInt8AppendInt         31453852         40.1  ns/op       8 B/op       1 allocs/op
 */
func BenchmarkInt8ToByte(b *testing.B) {
	var buff = [4]byte{}
	var a int8 = -124
	for n := 0; n < b.N; n++ {
		Int8ToBytes(a, &buff)
	}
}
func BenchmarkInt8Itoa(b *testing.B) {
	var a int8 = -124
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkInt8AppendInt(b *testing.B) {
	var buff []byte
	var a int8 = -124
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}

/*
 * Int8 conversion benchmark for zero value
 * BenchmarkInt8ToByte_Zero       1000000000        0.266 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt8Itoa_Zero         494303570         2.42  ns/op       0 B/op       0 allocs/op
 * BenchmarkInt8AppendInt_Zero    37836842         33.0   ns/op       8 B/op       1 allocs/op
 */
func BenchmarkInt8ToByte_Zero(b *testing.B) {
	var buff = [4]byte{}
	var a int8 = 0
	for n := 0; n < b.N; n++ {
		Int8ToBytes(a, &buff)
	}
}
func BenchmarkInt8Itoa_Zero(b *testing.B) {
	var a int8 = 0
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkInt8AppendInt_Zero(b *testing.B) {
	var buff []byte
	var a int8 = 0
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}

/*
 * Int8 conversion benchmark for short value
 * BenchmarkInt8ToByte_Short      471791479         2.51 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt8Itoa_Short        376539334         3.16 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt8AppendInt_Short   39066796         31.7  ns/op       8 B/op       1 allocs/op
 */
func BenchmarkInt8ToByte_Short(b *testing.B) {
	var buff = [4]byte{}
	var a int8 = 74
	for n := 0; n < b.N; n++ {
		Int8ToBytes(a, &buff)
	}
}
func BenchmarkInt8Itoa_Short(b *testing.B) {
	var a = 74
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkInt8AppendInt_Short(b *testing.B) {
	var buff []byte
	var a int8 = 74
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}

/*
 * Int16 conversion benchmark
 * BenchmarkInt16ToByte           148595379         8.05 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt16Itoa             37582310         32.0  ns/op       8 B/op       1 allocs/op
 * BenchmarkInt16AppendInt        30225768         40.7  ns/op       8 B/op       1 allocs/op
 */
func BenchmarkInt16ToByte(b *testing.B) {
	var buff = [6]byte{}
	var a int16 = -32749
	for n := 0; n < b.N; n++ {
		Int16ToBytes(a, &buff)
	}
}
func BenchmarkInt16Itoa(b *testing.B) {
	var a int16 = -32749
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkInt16AppendInt(b *testing.B) {
	var buff []byte
	var a int16 = -32749
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}

/*
 * Int16 conversion benchmark for zero value
 * BenchmarkInt16ToByte_Zero      1000000000         0.264 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt16Itoa_Zero        490720638          2.43  ns/op       0 B/op       0 allocs/op
 * BenchmarkInt16AppendInt_Zero   38828906          30.8   ns/op       8 B/op       1 allocs/op
 */
func BenchmarkInt16ToByte_Zero(b *testing.B) {
	var buff = [6]byte{}
	var a int16 = 0
	for n := 0; n < b.N; n++ {
		Int16ToBytes(a, &buff)
	}
}
func BenchmarkInt16Itoa_Zero(b *testing.B) {
	var a int16 = 0
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkInt16AppendInt_Zero(b *testing.B) {
	var buff []byte
	var a int16 = 0
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}

/*
 * Int16 conversion benchmark for short value
 * BenchmarkInt16ToByte_Short     452417127         2.66 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt16Itoa_Short       393517578         2.91 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt16AppendInt_Short  35915983         31.8  ns/op       8 B/op       1 allocs/op
 */
func BenchmarkInt16ToByte_Short(b *testing.B) {
	var buff = [6]byte{}
	var a int16 = 37
	for n := 0; n < b.N; n++ {
		Int16ToBytes(a, &buff)
	}
}
func BenchmarkInt16Itoa_Short(b *testing.B) {
	var a = 37
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkInt16AppendInt_Short(b *testing.B) {
	var buff []byte
	var a int16 = 37
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}

/*
 * Int32 conversion benchmark
 * BenchmarkInt32ToByte           83562061        14.2 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt32Itoa             27075024        45.1 ns/op      16 B/op       1 allocs/op
 * BenchmarkInt32AppendInt        21696037        54.8 ns/op      16 B/op       1 allocs/op
 */
func BenchmarkInt32ToByte(b *testing.B) {
	var buff = [11]byte{}
	var a int32 = -2147483174
	for n := 0; n < b.N; n++ {
		Int32ToBytes(a, &buff)
	}
}
func BenchmarkInt32Itoa(b *testing.B) {
	var a int32 = -2147483174
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkInt32AppendInt(b *testing.B) {
	var buff []byte
	var a int32 = -2147483174
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}

/*
 * Int32 conversion benchmark for zero value
 * BenchmarkInt32ToByte_Zero      1000000000         0.262 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt32Itoa_Zero        486177613          2.43  ns/op       0 B/op       0 allocs/op
 * BenchmarkInt32AppendInt_Zero   39167974          30.9   ns/op       8 B/op       1 allocs/op
 */
func BenchmarkInt32ToByte_Zero(b *testing.B) {
	var buff = [11]byte{}
	var a int32 = 0
	for n := 0; n < b.N; n++ {
		Int32ToBytes(a, &buff)
	}
}
func BenchmarkInt32Itoa_Zero(b *testing.B) {
	var a int32 = 0
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkInt32AppendInt_Zero(b *testing.B) {
	var buff []byte
	var a int32 = 0
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}

/*
 * Int32 conversion benchmark for short value
 * BenchmarkInt32ToByte_Short     459720351         2.59 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt32Itoa_Short       366198333         3.20 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt32AppendInt_Short  35955322         32.6  ns/op       8 B/op       1 allocs/op
 */
func BenchmarkInt32ToByte_Short(b *testing.B) {
	var buff = [11]byte{}
	var a int32 = 47
	for n := 0; n < b.N; n++ {
		Int32ToBytes(a, &buff)
	}
}
func BenchmarkInt32Itoa_Short(b *testing.B) {
	var a = 47
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkInt32AppendInt_Short(b *testing.B) {
	var buff []byte
	var a int32 = 47
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}

/*
 * Int64 conversion benchmark
 * BenchmarkInt64ToByte           50467356        23.6 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt64Itoa             19652546        65.0 ns/op      32 B/op       1 allocs/op
 * BenchmarkInt64AppendInt        17173722        78.2 ns/op      32 B/op       1 allocs/op
 */
func BenchmarkInt64ToByte(b *testing.B) {
	var buff = [20]byte{}
	var a int64 = -9223372036854772739
	for n := 0; n < b.N; n++ {
		Int64ToBytes(a, &buff)
	}
}
func BenchmarkInt64Itoa(b *testing.B) {
	var a int64 = -9223372036854772739
	for n := 0; n < b.N; n++ {
		strconv.FormatInt(a, 10)
	}
}
func BenchmarkInt64AppendInt(b *testing.B) {
	var buff []byte
	var a int64 = -9223372036854772739
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}

/*
 * Int64 conversion benchmark for zero value
 * BenchmarkInt64ToByte_Zero      1000000000         0.273 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt64Itoa_Zero        479749556          2.51  ns/op       0 B/op       0 allocs/op
 * BenchmarkInt64AppendInt_Zero   37306914          35.6   ns/op       8 B/op       1 allocs/op
 */
func BenchmarkInt64ToByte_Zero(b *testing.B) {
	var buff = [20]byte{}
	var a int64 = 0
	for n := 0; n < b.N; n++ {
		Int64ToBytes(a, &buff)
	}
}
func BenchmarkInt64Itoa_Zero(b *testing.B) {
	var a int64 = 0
	for n := 0; n < b.N; n++ {
		strconv.FormatInt(a, 10)
	}
}
func BenchmarkInt64AppendInt_Zero(b *testing.B) {
	var buff []byte
	var a int64 = 0
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}

/*
 * Int64 conversion benchmark for short value
 * BenchmarkInt64ToByte_Short     434567925         2.76 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt64Itoa_Short       392553559         3.22 ns/op       0 B/op       0 allocs/op
 * BenchmarkInt64AppendInt_Short  36172772         36.3  ns/op       8 B/op       1 allocs/op
 */
func BenchmarkInt64ToByte_Short(b *testing.B) {
	var buff = [20]byte{}
	var a int64 = 47
	for n := 0; n < b.N; n++ {
		Int64ToBytes(a, &buff)
	}
}
func BenchmarkInt64Itoa_Short(b *testing.B) {
	var a int64 = 47
	for n := 0; n < b.N; n++ {
		strconv.FormatInt(a, 10)
	}
}
func BenchmarkInt64AppendInt_Short(b *testing.B) {
	var buff []byte
	var a int64 = 47
	for n := 0; n < b.N; n++ {
		strconv.AppendInt(buff, int64(a), 10)
	}
}
