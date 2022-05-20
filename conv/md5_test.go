package conv_test

import (
	"crypto/md5"
	"fmt"
	"math/rand"
	"testing"
	"time"

	. "github.com/greenplum-db/gp-common-go-libs/conv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("MD5 Conversion functions", func() {
	Context("FormatMD5", func() {
		var b32 = [32]byte{}
		It("should convert to string", func() {
			sum := md5.Sum([]byte("select 1;"))
			FormatMD5(sum, &b32)
			Expect(string(b32[:])).To(Equal(fmt.Sprintf("%x", sum)))
		})
		It("should convert to string + multi-byte", func() {
			sum := md5.Sum([]byte("tgvdoi``f:jd;asdfnk\\//\"\"''lbkng[istf9 世界 jvcl;4nr"))
			FormatMD5(sum, &b32)
			Expect(string(b32[:])).To(Equal(fmt.Sprintf("%x", sum)))
		})
		It("should convert to string + random", func() {
			for k := 0; k < 300; k++ {
				var randomBytes = make([]byte, k, k)
				rand.Seed(time.Now().UnixNano())
				for i := 0; i < k; i++ {
					randomBytes[i] = '!' + byte(rand.Intn(136))
				}
				sum := md5.Sum(randomBytes[:])
				FormatMD5(sum, &b32)
				Expect(string(b32[:])).To(Equal(fmt.Sprintf("%x", sum)))
			}
		})
	})
})

/*
 * FormatMD5 conversion benchmark
 * BenchmarkFormatMD5   45765602        25.2 ns/op       0 B/op       0 allocs/op
 * BenchmarkSprintfMD5   3388221       361   ns/op      64 B/op       3 allocs/op
 */
func BenchmarkFormatMD5(b *testing.B) {
	sum := md5.Sum([]byte("reofjljuhtwe43589sdfsd"))
	var b32 = [32]byte{}
	for n := 0; n < b.N; n++ {
		FormatMD5(sum, &b32)
		_ = string(b32[:])
	}
}
func BenchmarkSprintfMD5(b *testing.B) {
	sum := md5.Sum([]byte("reofjljuhtwe43589sdfsd"))
	for n := 0; n < b.N; n++ {
		_ = fmt.Sprintf("%x", sum)
	}
}
