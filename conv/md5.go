package conv

// FormatMD5 converts md5.Sum() result to byte slice
func FormatMD5(hash [16]byte, buf *[32]byte) {
	var i int
	for _, r := range hash {
		buf[i] = digits[r>>4]
		buf[i+1] = digits[r&0xF]
		i += 2
	}
}
