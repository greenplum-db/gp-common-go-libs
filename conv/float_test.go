package conv_test

import (
	"math"
	"strconv"
	"testing"

	. "github.com/greenplum-db/gp-common-go-libs/conv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Float Conversion functions", func() {
	Context("Float64ToString", func() {
		var b39 = [39]byte{}
		It("should convert float64(2) int", func() {
			Expect(Float64ToString(1.2, 2, &b39)).To(Equal(strconv.FormatFloat(1.2, 'f', 2, 64)))
		})
		It("should convert float64(2) int+negative", func() {
			Expect(Float64ToString(-1.2, 2, &b39)).To(Equal(strconv.FormatFloat(-1.2, 'f', 2, 64)))
		})
		It("should convert float64(2) int", func() {
			Expect(Float64ToString(1234567890, 2, &b39)).To(Equal(strconv.FormatFloat(1234567890, 'f', 2, 64)))
		})
		It("should convert float64(2) int+negative", func() {
			Expect(Float64ToString(-1234567890, 2, &b39)).To(Equal(strconv.FormatFloat(-1234567890, 'f', 2, 64)))
		})
		It("should convert float64(2) neg_prec int", func() {
			Expect(Float64ToString(1234567890, -2, &b39)).To(Equal(strconv.FormatFloat(1234567890, 'f', -2, 64)))
		})
		It("should convert float64(2) neg_prec int+negative", func() {
			Expect(Float64ToString(-1234567890, -2, &b39)).To(Equal(strconv.FormatFloat(-1234567890, 'f', -2, 64)))
		})
		It("should convert float64(2) int+long", func() {
			Expect(Float64ToString(8178654234748649, 2, &b39)).To(Equal(strconv.FormatFloat(8178654234748649, 'f', 2, 64)))
		})
		It("should convert float64(2) int+long+negative", func() {
			Expect(Float64ToString(-8178654234748649, 2, &b39)).To(Equal(strconv.FormatFloat(-8178654234748649, 'f', 2, 64)))
		})
		It("should convert float64(2) neg_prec int+long", func() {
			Expect(Float64ToString(8178654234748649, -2, &b39)).To(Equal(strconv.FormatFloat(8178654234748649, 'f', -2, 64)))
		})
		It("should convert float64(2) neg_prec int+long+negative", func() {
			Expect(Float64ToString(-8178654234748649, -2, &b39)).To(Equal(strconv.FormatFloat(-8178654234748649, 'f', -2, 64)))
		})
		It("should convert float64(2) zero", func() {
			Expect(Float64ToString(0, 2, &b39)).To(Equal(strconv.FormatFloat(0, 'f', 2, 64)))
		})
		It("should convert float64(2) zero+negative", func() {
			Expect(Float64ToString(-0, 2, &b39)).To(Equal(strconv.FormatFloat(-0, 'f', 2, 64)))
		})
		It("should convert float64(2) edge", func() {
			Expect(Float64ToString(0.01, 2, &b39)).To(Equal(strconv.FormatFloat(0.01, 'f', 2, 64)))
		})
		It("should convert float64(2) edge+negative", func() {
			Expect(Float64ToString(-0.01, 2, &b39)).To(Equal(strconv.FormatFloat(-0.01, 'f', 2, 64)))
		})
		It("should convert float64(2) long", func() {
			Expect(Float64ToString(78654234748649.33, 2, &b39)).To(Equal(strconv.FormatFloat(78654234748649.33, 'f', 2, 64)))
		})
		It("should convert float64(2) long+negative", func() {
			Expect(Float64ToString(-78654234748649.33, 2, &b39)).To(Equal(strconv.FormatFloat(-78654234748649.33, 'f', 2, 64)))
		})
		It("should convert float64(2) long+carry", func() {
			Expect(Float64ToString(78654234748649.99, 2, &b39)).To(Equal(strconv.FormatFloat(78654234748649.99, 'f', 2, 64)))
		})
		It("should convert float64(2) long+carry+negative", func() {
			Expect(Float64ToString(-78654234748649.99, 2, &b39)).To(Equal(strconv.FormatFloat(-78654234748649.99, 'f', 2, 64)))
		})
		It("should convert float64(2) long+precise", func() {
			Expect(Float64ToString(654234748649.3333, 2, &b39)).To(Equal(strconv.FormatFloat(654234748649.3333, 'f', 2, 64)))
		})
		It("should convert float64(2) long+precise+negative", func() {
			Expect(Float64ToString(-654234748649.3333, 2, &b39)).To(Equal(strconv.FormatFloat(-654234748649.3333, 'f', 2, 64)))
		})
		It("should convert float64(2) long+precise+carry", func() {
			Expect(Float64ToString(654234748649.9999, 2, &b39)).To(Equal(strconv.FormatFloat(654234748649.9999, 'f', 2, 64)))
		})
		It("should convert float64(2) long+precise+carry+negative", func() {
			Expect(Float64ToString(-654234748649.9999, 2, &b39)).To(Equal(strconv.FormatFloat(-654234748649.9999, 'f', 2, 64)))
		})
		It("should convert float64(2) exceed", func() {
			Expect(Float64ToString(8178654234748649.3333333, 2, &b39)).To(Equal(strconv.FormatFloat(8178654234748649.3333333, 'f', 2, 64)))
		})
		It("should convert float64(2) exceed+negative", func() {
			Expect(Float64ToString(-8178654234748649.3333333, 2, &b39)).To(Equal(strconv.FormatFloat(-8178654234748649.3333333, 'f', 2, 64)))
		})
		It("should convert float64(2) exceed+carry", func() {
			Expect(Float64ToString(8178654234748649.9999999, 2, &b39)).To(Equal(strconv.FormatFloat(8178654234748649.9999999, 'f', 2, 64)))
		})
		It("should convert float64(2) exceed+carry+negative", func() {
			Expect(Float64ToString(-8178654234748649.9999999, 2, &b39)).To(Equal(strconv.FormatFloat(-8178654234748649.9999999, 'f', 2, 64)))
		})
		It("should convert float64(2) exceed+carry+negative", func() {
			Expect(Float64ToString(float64(math.MaxInt64-511.001), 2, &b39)).To(Equal(strconv.FormatFloat(9223372036854774784.00, 'f', 2, 64)))
		})
		It("should convert float64(2) exceed+carry+negative", func() {
			Expect(Float64ToString(float64(math.MinInt64+512.001), 2, &b39)).To(Equal(strconv.FormatFloat(-9223372036854774784.00, 'f', 2, 64)))
		})
		It("should convert float64(9) long+precise+carry", func() {
			Expect(Float64ToString(654234748649.9999999999, 9, &b39)).To(Equal(strconv.FormatFloat(654234748649.9999999999, 'f', 9, 64)))
		})
		It("should convert float64(9) long+precise+carry+negative", func() {
			Expect(Float64ToString(-654234748649.9999999999, 9, &b39)).To(Equal(strconv.FormatFloat(-654234748649.9999999999, 'f', 9, 64)))
		})
		It("should convert NaN", func() {
			Expect(Float64ToString(math.Log(-1.0), 2, &b39)).To(Equal(strconv.FormatFloat(math.Log(-1.0), 'f', 2, 64)))
		})
	})
})

/*
 * Float64ToBytes conversion benchmark
 * BenchmarkFFloat64ToBytes         67491673        16.4 ns/op       0 B/op       0 allocs/op
 * BenchmarkFFormatFloat             4375734       277   ns/op      37 B/op       2 allocs/op
 */
func BenchmarkFFloat64ToBytes(b *testing.B) {
	var buf = [39]byte{}
	for n := 0; n < b.N; n++ {
		_ = Float64ToBytes(10.01, 2, &buf)
	}
}
func BenchmarkFFormatFloat(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_ = strconv.FormatFloat(10.01, 'f', 2, 64)
	}
}

/*
 * Float64ToBytes conversion benchmark for zero value
 * BenchmarkFFloat64ToBytes_Zero   358033568         3.29 ns/op       0 B/op       0 allocs/op
 * BenchmarkFFormatFloat_Zero       11411266       106    ns/op      36 B/op       2 allocs/op
 */
func BenchmarkFFloat64ToBytes_Zero(b *testing.B) {
	var buf = [39]byte{}
	var a float64
	for n := 0; n < b.N; n++ {
		_ = Float64ToBytes(a, 2, &buf)
	}
}
func BenchmarkFFormatFloat_Zero(b *testing.B) {
	var a float64
	for n := 0; n < b.N; n++ {
		_ = strconv.FormatFloat(a, 'f', 2, 64)
	}
}

/*
 * Float64ToBytes conversion benchmark for NaN
 * BenchmarkFFloat64ToBytes_NaN    396928503         2.93 ns/op       0 B/op       0 allocs/op
 * BenchmarkFFormatFloat_NaN        20311473        58.2  ns/op      35 B/op       2 allocs/op
 */
func BenchmarkFFloat64ToBytes_NaN(b *testing.B) {
	var buf = [39]byte{}
	var a = math.Log(-1.0)
	for n := 0; n < b.N; n++ {
		_ = Float64ToBytes(a, 2, &buf)
	}
}
func BenchmarkFFormatFloat_NaN(b *testing.B) {
	var a = math.Log(-1.0)
	for n := 0; n < b.N; n++ {
		_ = strconv.FormatFloat(a, 'f', 2, 64)
	}
}

/*
 * Float64ToBytes conversion benchmark for long value
 * BenchmarkFFloat64ToBytes_Long    33029286        35.7 ns/op       0 B/op       0 allocs/op
 * BenchmarkFFormatFloat_Long        5930419       202   ns/op      64 B/op       2 allocs/op
 */
func BenchmarkFFloat64ToBytes_Long(b *testing.B) {
	var buf = [39]byte{}
	var a float64 = -654234748649.9999999999
	for n := 0; n < b.N; n++ {
		_ = Float64ToBytes(a, 9, &buf)
	}
}
func BenchmarkFFormatFloat_Long(b *testing.B) {
	var a float64 = -654234748649.9999999999
	for n := 0; n < b.N; n++ {
		_ = strconv.FormatFloat(a, 'f', 9, 64)
	}
}

/*
 * Float64ToBytes conversion benchmark for value not eligible for optimized algorithm
 * BenchmarkFFloat64ToBytes_Exceed   4489482       279 ns/op      96 B/op       3 allocs/op
 * BenchmarkFFormatFloat_Exceed      5457046       231 ns/op      64 B/op       2 allocs/op
 */
func BenchmarkFFloat64ToBytes_Exceed(b *testing.B) {
	var buf = [39]byte{}
	var a float64 = math.MinInt64 - 123456.78
	for n := 0; n < b.N; n++ {
		_ = Float64ToBytes(a, 2, &buf)
	}
}
func BenchmarkFFormatFloat_Exceed(b *testing.B) {
	var a float64 = math.MinInt64 - 123456.78
	for n := 0; n < b.N; n++ {
		_ = strconv.FormatFloat(a, 'f', 2, 64)
	}
}
