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

var _ = Describe("Unsigned Integer Conversion functions", func() {
	Context("UInt8ToBytes", func() {
		var b3 = [3]byte{}
		It("should convert uint8", func() {
			Expect(UInt8ToBytes(63, &b3)).To(Equal([]byte("63")))
		})
		It("should convert uint8 edge", func() {
			Expect(UInt8ToBytes(255, &b3)).To(Equal([]byte("255")))
		})
		It("should convert zero uint8", func() {
			Expect(UInt8ToBytes(0, &b3)).To(Equal([]byte("0")))
		})
		It("should convert zero uint8", func() {
			Expect(UInt8ToBytes(-0, &b3)).To(Equal([]byte("0")))
		})
		It("should get same result with strconv.Itoa", func() {
			var i uint8
			for {
				Expect(UInt8ToBytes(i, &b3)).To(Equal([]byte(strconv.Itoa(int(i)))))
				if i == math.MaxUint8 {
					break
				}
				i++
			}
		})
	})
	Context("UInt16ToBytes", func() {
		var b5 = [5]byte{}
		It("should convert uint16", func() {
			Expect(UInt16ToBytes(1234, &b5)).To(Equal([]byte("1234")))
		})
		It("should convert uint16 edge", func() {
			Expect(UInt16ToBytes(65535, &b5)).To(Equal([]byte("65535")))
		})
		It("should convert zero uint16", func() {
			Expect(UInt16ToBytes(0, &b5)).To(Equal([]byte("0")))
		})
		It("should convert zero uint16", func() {
			Expect(UInt16ToBytes(-0, &b5)).To(Equal([]byte("0")))
		})
		It("should get same result with strconv.Itoa", func() {
			var i uint16
			for {
				Expect(UInt16ToBytes(i, &b5)).To(Equal([]byte(strconv.Itoa(int(i)))))
				if i == math.MaxUint16 {
					break
				}
				i++
			}
		})
	})
	Context("UInt32ToBytes", func() {
		var b10 = [10]byte{}
		It("should convert uint32", func() {
			Expect(UInt32ToBytes(12345678, &b10)).To(Equal([]byte("12345678")))
		})
		It("should convert uint32 edge", func() {
			Expect(UInt32ToBytes(4294967295, &b10)).To(Equal([]byte("4294967295")))
		})
		It("should convert zero uint32", func() {
			Expect(UInt32ToBytes(0, &b10)).To(Equal([]byte("0")))
		})
		It("should convert zero uint32", func() {
			Expect(UInt32ToBytes(-0, &b10)).To(Equal([]byte("0")))
		})
		It("random test uint32", func() {
			var n uint32
			var max uint32 = 0
			var min uint32 = math.MaxUint32
			rand.Seed(time.Now().UnixNano())
			for i := 0; i < 100000; i++ {
				n = rand.Uint32()
				Expect(UInt32ToBytes(n, &b10)).To(Equal([]byte(strconv.Itoa(int(n)))))
				if max < n {
					rand.Seed(time.Now().UnixNano())
					max = n
				}
				if min > n {
					rand.Seed(time.Now().UnixNano())
					min = n
				}
			}
			fmt.Println("Random test for UInt32ToBytes,", min, "to", max)
		})
	})
	Context("UInt64ToBytes", func() {
		var b20 = [20]byte{}
		It("should convert uint64", func() {
			Expect(UInt64ToBytes(1234567890, &b20)).To(Equal([]byte("1234567890")))
		})
		It("should convert uint64 edge", func() {
			Expect(UInt64ToBytes(18446744073709551615, &b20)).To(Equal([]byte("18446744073709551615")))
		})
		It("should convert zero uint64", func() {
			Expect(UInt64ToBytes(0, &b20)).To(Equal([]byte("0")))
		})
		It("should convert zero uint64", func() {
			Expect(UInt64ToBytes(-0, &b20)).To(Equal([]byte("0")))
		})
		It("random test", func() {
			var n uint64
			var max uint64 = 0
			var min uint64 = math.MaxUint64
			rand.Seed(time.Now().UnixNano())
			for i := 0; i < 1000000; i++ {
				n = rand.Uint64()
				Expect(UInt64ToBytes(n, &b20)).To(Equal([]byte(fmt.Sprintf("%d", n))))
				if max < n {
					rand.Seed(time.Now().UnixNano())
					max = n
				}
				if min > n {
					rand.Seed(time.Now().UnixNano())
					min = n
				}
			}
			fmt.Println("Random test for UInt64ToBytes,", min, "to", max)
		})
	})
})

/*
 * UInt8 conversion benchmark
 * BenchmarkUInt8ToByte           1000000000         0.377 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt8Itoa             43034558          27.7   ns/op       3 B/op       1 allocs/op
 * BenchmarkUInt8AppendInt        32037789          41.9   ns/op       8 B/op       1 allocs/op
 */
func BenchmarkUInt8ToByte(b *testing.B) {
	var buff = [3]byte{}
	var a uint8 = 255
	for n := 0; n < b.N; n++ {
		UInt8ToBytes(a, &buff)
	}
}
func BenchmarkUInt8Itoa(b *testing.B) {
	var a uint8 = 255
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkUInt8AppendInt(b *testing.B) {
	var buff []byte
	var a uint8 = 255
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, uint64(a), 10)
	}
}

/*
 * UInt8 conversion benchmark for zero value
 * BenchmarkUInt8ToByte_Zero      1000000000         0.277 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt8Itoa_Zero        439623384          2.53  ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt8AppendInt_Zero   36406144          33.2   ns/op       8 B/op       1 allocs/op
 */
func BenchmarkUInt8ToByte_Zero(b *testing.B) {
	var buff = [3]byte{}
	var a uint8 = 0
	for n := 0; n < b.N; n++ {
		UInt8ToBytes(a, &buff)
	}
}
func BenchmarkUInt8Itoa_Zero(b *testing.B) {
	var a uint8 = 0
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkUInt8AppendInt_Zero(b *testing.B) {
	var buff []byte
	var a uint8 = 0
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, uint64(a), 10)
	}
}

/*
 * UInt8 conversion benchmark for short value
 * BenchmarkUInt8ToByte_Short     1000000000         0.269 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt8Itoa_Short       402061838          3.01  ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt8AppendInt_Short  37338842          33.3   ns/op       8 B/op       1 allocs/op
 */
func BenchmarkUInt8ToByte_Short(b *testing.B) {
	var buff = [3]byte{}
	var a uint8 = 47
	for n := 0; n < b.N; n++ {
		UInt8ToBytes(a, &buff)
	}
}
func BenchmarkUInt8Itoa_Short(b *testing.B) {
	var a = 47
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkUInt8AppendInt_Short(b *testing.B) {
	var buff []byte
	var a uint8 = 47
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, uint64(a), 10)
	}
}

/*
 * UInt16 conversion benchmark
 * BenchmarkUInt16ToByte          147289009         8.19 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt16Itoa            39393541         31.6  ns/op       5 B/op       1 allocs/op
 * BenchmarkUInt16AppendInt       29030041         43.6  ns/op       8 B/op       1 allocs/op
 */
func BenchmarkUInt16ToByte(b *testing.B) {
	var buff = [5]byte{}
	var a uint16 = math.MaxUint16
	for n := 0; n < b.N; n++ {
		UInt16ToBytes(a, &buff)
	}
}
func BenchmarkUInt16Itoa(b *testing.B) {
	var a uint16 = math.MaxUint16
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkUInt16AppendInt(b *testing.B) {
	var buff []byte
	var a uint16 = math.MaxUint16
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, uint64(a), 10)
	}
}

/*
 * UInt16 conversion benchmark for zero value
 * BenchmarkUInt16ToByte_Zero     1000000000         0.276 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt16Itoa_Zero       479231823          2.54  ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt16AppendInt_Zero  40428386          33.9   ns/op       8 B/op       1 allocs/op
 */
func BenchmarkUInt16ToByte_Zero(b *testing.B) {
	var buff = [5]byte{}
	var a uint16 = 0
	for n := 0; n < b.N; n++ {
		UInt16ToBytes(a, &buff)
	}
}
func BenchmarkUInt16Itoa_Zero(b *testing.B) {
	var a uint16 = 0
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkUInt16AppendInt_Zero(b *testing.B) {
	var buff []byte
	var a uint16 = 0
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, uint64(a), 10)
	}
}

/*
 * UInt16 conversion benchmark for short value
 * BenchmarkUInt16ToByte_Short    483674848         2.51 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt16Itoa_Short      360645939         3.49 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt16AppendInt_Short 37796120         33.9  ns/op       8 B/op       1 allocs/op
 */
func BenchmarkUInt16ToByte_Short(b *testing.B) {
	var buff = [5]byte{}
	var a uint16 = 99
	for n := 0; n < b.N; n++ {
		UInt16ToBytes(a, &buff)
	}
}
func BenchmarkUInt16Itoa_Short(b *testing.B) {
	var a = 99
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkUInt16AppendInt_Short(b *testing.B) {
	var buff []byte
	var a uint16 = 99
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, uint64(a), 10)
	}
}

/*
 * UInt32 conversion benchmark
 * BenchmarkUInt32ToByte          90118614        13.8 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt32Itoa            25844562        52.2 ns/op      16 B/op       1 allocs/op
 * BenchmarkUInt32AppendInt       20538741        60.1 ns/op      16 B/op       1 allocs/op
 */
func BenchmarkUInt32ToByte(b *testing.B) {
	var buff = [10]byte{}
	var a uint32 = math.MaxUint32
	for n := 0; n < b.N; n++ {
		UInt32ToBytes(a, &buff)
	}
}
func BenchmarkUInt32Itoa(b *testing.B) {
	var a uint32 = math.MaxUint32
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkUInt32AppendInt(b *testing.B) {
	var buff []byte
	var a uint32 = math.MaxUint32
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, uint64(a), 10)
	}
}

/*
 * UInt32 conversion benchmark for zero value
 * BenchmarkUInt32ToByte_Zero     1000000000         0.289 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt32Itoa_Zero       449177590          2.55  ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt32AppendInt_Zero  36355106          32.9   ns/op       8 B/op       1 allocs/op
 */
func BenchmarkUInt32ToByte_Zero(b *testing.B) {
	var buff = [10]byte{}
	var a uint32 = 0
	for n := 0; n < b.N; n++ {
		UInt32ToBytes(a, &buff)
	}
}
func BenchmarkUInt32Itoa_Zero(b *testing.B) {
	var a uint32 = 0
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkUInt32AppendInt_Zero(b *testing.B) {
	var buff []byte
	var a uint32 = 0
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, uint64(a), 10)
	}
}

/*
 * UInt32 conversion benchmark for short value
 * BenchmarkUInt32ToByte_Short    433739966         2.79 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt32Itoa_Short      391095169         3.06 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt32AppendInt_Short 33872798         34.3  ns/op       8 B/op       1 allocs/op
 */
func BenchmarkUInt32ToByte_Short(b *testing.B) {
	var buff = [10]byte{}
	var a uint32 = 99
	for n := 0; n < b.N; n++ {
		UInt32ToBytes(a, &buff)
	}
}
func BenchmarkUInt32Itoa_Short(b *testing.B) {
	var a = 99
	for n := 0; n < b.N; n++ {
		strconv.Itoa(int(a))
	}
}
func BenchmarkUInt32AppendInt_Short(b *testing.B) {
	var buff []byte
	var a uint32 = 99
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, uint64(a), 10)
	}
}

/*
 * UInt64 conversion benchmark
 * BenchmarkUInt64ToByte          43346986        24.2 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt64Itoa            19735735        61.9 ns/op      32 B/op       1 allocs/op
 * BenchmarkUInt64AppendInt       16643550        71.6 ns/op      32 B/op       1 allocs/op
 */
func BenchmarkUInt64ToByte(b *testing.B) {
	var buff = [20]byte{}
	var a uint64 = math.MaxUint64
	for n := 0; n < b.N; n++ {
		UInt64ToBytes(a, &buff)
	}
}
func BenchmarkUInt64Itoa(b *testing.B) {
	var a uint64 = math.MaxUint64
	for n := 0; n < b.N; n++ {
		strconv.FormatUint(a, 10)
	}
}
func BenchmarkUInt64AppendInt(b *testing.B) {
	var buff []byte
	var a uint64 = math.MaxUint64
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, a, 10)
	}
}

/*
 * UInt64 conversion benchmark for zero value
 * BenchmarkUInt64ToByte_Zero     1000000000         0.271 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt64Itoa_Zero       471060156          2.47  ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt64AppendInt_Zero  38528214          31.7   ns/op       8 B/op       1 allocs/op
 */
func BenchmarkUInt64ToByte_Zero(b *testing.B) {
	var buff = [20]byte{}
	var a uint64 = 0
	for n := 0; n < b.N; n++ {
		UInt64ToBytes(a, &buff)
	}
}
func BenchmarkUInt64Itoa_Zero(b *testing.B) {
	var a uint64 = 0
	for n := 0; n < b.N; n++ {
		strconv.FormatUint(a, 10)
	}
}
func BenchmarkUInt64AppendInt_Zero(b *testing.B) {
	var buff []byte
	var a uint64 = 0
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, a, 10)
	}
}

/*
 * UInt64 conversion benchmark for short value
 * BenchmarkUInt64ToByte_Short    475915591         2.50 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt64Itoa_Short      362572149         3.33 ns/op       0 B/op       0 allocs/op
 * BenchmarkUInt64AppendInt_Short 39199520         31.6  ns/op       8 B/op       1 allocs/op
 */
func BenchmarkUInt64ToByte_Short(b *testing.B) {
	var buff = [20]byte{}
	var a uint64 = 99
	for n := 0; n < b.N; n++ {
		UInt64ToBytes(a, &buff)
	}
}
func BenchmarkUInt64Itoa_Short(b *testing.B) {
	var a uint64 = 99
	for n := 0; n < b.N; n++ {
		strconv.FormatUint(a, 10)
	}
}
func BenchmarkUInt64AppendInt_Short(b *testing.B) {
	var buff []byte
	var a uint64 = 99
	for n := 0; n < b.N; n++ {
		strconv.AppendUint(buff, a, 10)
	}
}
