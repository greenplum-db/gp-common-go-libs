package conv

// Int8ToBytes is the fastest way to convert int8 into byte slice
func Int8ToBytes(n int8, buf *[4]byte) []byte {
	if 0 == n {
		return digits1[0]
	}
	return i8Dig(n, buf)
}

// Int16ToBytes is the fastest way to convert int16 into byte slice
func Int16ToBytes(n int16, buf *[6]byte) []byte {
	if 0 == n {
		return digits1[0]
	}
	return i16Dig(n, buf)
}

// Int32ToBytes is the fastest way to convert int32 into byte slice
func Int32ToBytes(n int32, buf *[11]byte) []byte {
	if 0 == n {
		return digits1[0]
	}
	return i32Dig(n, buf)
}

// Int64ToBytes is the fastest way to convert int64 into byte slice
func Int64ToBytes(n int64, buf *[20]byte) []byte {
	if 0 == n {
		return digits1[0]
	}
	return i64Dig(n, buf)
}

func i8Dig(n int8, buf *[4]byte) []byte {
	if 0 < n {
		if n < 10 {
			return digits1[n]
		} else if n < 100 {
			return digits2[n]
		} else {
			n = n - 100
			buf[1], buf[2], buf[3] = '1', digits2[n][0], digits2[n][1]
			return buf[1:]
		}
	}

	if n > -10 {
		buf[2], buf[3] = '-', digits[-n]
		return buf[2:]
	} else if n > -100 {
		buf[1], buf[2], buf[3] = '-', digits2[-n][0], digits2[-n][1]
		return buf[1:]
	}

	n = -100 - n
	buf[0], buf[1], buf[2], buf[3] = '-', '1', digits2[n][0], digits2[n][1]
	return buf[:]
}

func i16Dig(n int16, buf *[6]byte) []byte {
	var neg bool
	var u uint16
	if n > 0 {
		if n < 10 {
			return digits1[n]
		} else if n < 100 {
			return digits2[n]
		}
		u = uint16(n)
	} else {
		neg = true
		u = uint16(n)
		u = -u
	}

	pos := 6
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

	return buf[pos:]
}

func i32Dig(n int32, buf *[11]byte) []byte {
	var neg bool
	var u uint32
	if n > 0 {
		if n < 10 {
			return digits1[n]
		} else if n < 100 {
			return digits2[n]
		}
		u = uint32(n)
	} else {
		neg = true
		u = uint32(n)
		u = -u
	}

	pos := 11
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

	return buf[pos:]
}

func i64Dig(n int64, buf *[20]byte) []byte {
	var neg bool
	var u uint64
	if n > 0 {
		if n < 10 {
			return digits1[n]
		} else if n < 100 {
			return digits2[n]
		}
		u = uint64(n)
	} else {
		neg = true
		u = uint64(n)
		u = -u
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

	return buf[pos:]
}
