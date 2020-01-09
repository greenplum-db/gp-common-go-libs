package conv

// UInt8ToBytes is the fastest way to convert uint8 into byte slice
func UInt8ToBytes(n uint8, buf *[3]byte) []byte {
	if n == 0 {
		return digits1[0]
	} else if n < 10 {
		return digits1[n]
	} else if n < 100 {
		return digits2[n]
	}
	n = n - 100
	if n < 100 {
		buf[0] = '1'
	} else {
		n = n - 100
		buf[0] = '2'
	}
	buf[1], buf[2] = digits2[n][0], digits2[n][1]
	return buf[0:]
}

// UInt16ToBytes is the fastest way to convert uint16 into byte slice
func UInt16ToBytes(n uint16, buf *[5]byte) []byte {
	if n == 0 {
		return digits1[0]
	}
	return ui16Dig(n, buf)
}

// UInt32ToBytes is the fastest way to convert uint32 into byte slice
func UInt32ToBytes(n uint32, buf *[10]byte) []byte {
	if n == 0 {
		return digits1[0]
	}
	return ui32Dig(n, buf)
}

// UInt64ToBytes is the fastest way to convert uint64 into byte slice
func UInt64ToBytes(n uint64, buf *[20]byte) []byte {
	if n == 0 {
		return digits1[0]
	}
	return ui64Dig(n, buf)
}

func ui16Dig(u uint16, buf *[5]byte) []byte {
	if u < 10 {
		return digits1[u]
	} else if u < 100 {
		return digits2[u]
	}

	pos := 5
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

	return buf[pos:]
}

func ui32Dig(u uint32, buf *[10]byte) []byte {
	if u < 10 {
		return digits1[u]
	} else if u < 100 {
		return digits2[u]
	}

	pos := 10
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

	return buf[pos:]
}

func ui64Dig(u uint64, buf *[20]byte) []byte {
	if u < 10 {
		return digits1[u]
	} else if u < 100 {
		return digits2[u]
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

	return buf[pos:]
}
