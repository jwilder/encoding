package bitops

// msb32 returns the number of bits required to store the value x
func msb32(x uint32) int {
	pos := 32
	temp := x >> 16
	if temp != 0 {
		pos -= 16
		x = temp
	}
	temp = x >> 8
	if temp != 0 {
		pos -= 8
		x = temp
	}
	temp = x >> 4
	if temp != 0 {
		pos -= 4
		x = temp
	}
	temp = x >> 2
	if temp != 0 {
		pos -= 2
		x = temp
	}
	temp = x >> 1
	if temp != 0 {
		return pos - 2
	}

	return int(uint32(pos) - x)
}

// msb64 returns the number of bits required to store the value x
func msb64(n uint64) int {
	if n <= 0 {
		return -1
	}
	var r, v uint
	if n >= 1<<32 {
		r += 32
		v = uint(n >> 32)
	} else {
		v = uint(n)
	}
	if v >= 1<<16 {
		r += 16
		v >>= 16
	}
	if v >= 1<<8 {
		r += 8
		v >>= 8
	}
	if v >= 1<<4 {
		r += 4
		v >>= 4
	}
	if v >= 1<<2 {
		r += 2
		v >>= 2
	}
	r += v >> 1
	return int(r)
}

// CanPack returs true if n elements from in can be stored using bits per element
func CanPack(src []uint64, n, bits int) bool {
	if len(src) < n {
		return false
	}

	end := len(src)
	if n < end {
		end = n
	}

	max := uint64((2 << uint64(bits-1)) - 1)

	for i := 0; i < end; i++ {
		if src[i] > max {
			return false
		}
	}

	return true
}
