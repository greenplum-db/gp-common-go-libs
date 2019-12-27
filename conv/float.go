package conv

import (
	"math"
	"strconv"
)

const maxEligbleInt64 = math.MaxInt64 - 512
const minEligbleInt64 = math.MinInt64 + 513

// Float64ToBytes is an optimized implementation for converting float64 to byte array
func Float64ToBytes(n float64, prec int, buf *[39]byte) []byte {
	if n != n {
		return NaNb
	}
	if n == 0 {
		if prec > 0 {
			if prec < 18 {
				return float0[prec]
			}
		} else {
			return float0[0]
		}
	} else {
		if n <= maxEligbleInt64 && n >= minEligbleInt64 && prec < 18 && prec >= 0 {
			return f64Dig(n, prec, buf)
		}
	}
	return []byte(strconv.FormatFloat(n, 'f', prec, 64))
}

// Float64ToString is an optimized implementation for converting float64 to string
func Float64ToString(n float64, prec int, buf *[39]byte) string {
	if n != n {
		return NaN
	}
	if n == 0 {
		if prec > 0 {
			if prec < 18 {
				return float0s[prec]
			}
		} else {
			return float0s[0]
		}
	} else {
		if n <= maxEligbleInt64 && n >= minEligbleInt64 && prec < 18 && prec >= 0 {
			return string(f64Dig(n, prec, buf))
		}
	}
	return strconv.FormatFloat(n, 'f', prec, 64)
}

func f64Dig(n float64, prec int, buf *[39]byte) []byte {
	neg := n < 0
	if neg {
		n = -n
	}
	u := int64(n)
	l := n - float64(u)
	var ul uint64
	if l+math.Pow10(-prec)/2 >= 1 {
		u++
		ul = 0
		l = 0
	} else {
		ul = uint64(math.Round((n - float64(u)) * math.Pow10(prec)))
	}

	pos := 20
	for u >= 100 {
		pos -= 2

		is := u % 100
		u /= 100

		buf[pos+1], buf[pos] = digits2[is][1], digits2[is][0]
	}

	if u < 10 {
		pos--
		buf[pos] = digits[u]
	} else {
		pos -= 2
		buf[pos+1], buf[pos] = digits2[u][1], digits2[u][0]
	}

	if neg {
		pos--
		buf[pos] = '-'
	}

	if prec == 0 {
		return buf[pos:20]
	}

	ed := 20 + prec
	buf[20] = '.'

	for ed > 20 {
		is := ul % 100
		ul /= 100

		if ed == 21 {
			buf[ed] = digits[is]
			break
		} else {
			buf[ed], buf[ed-1] = digits2[is][1], digits2[is][0]
			ed -= 2
		}
	}

	return buf[pos : 21+prec]
}
