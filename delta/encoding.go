package delta

// Delta modifies src by replacing each sucessive value by the difference
// between itself and the prior value.
func Delta(dst, src []int64) {
	if len(src) == 0 || len(dst) == 0 {
		return
	}
	dst[0] = src[0]
	for i := len(src) - 1; i > 0; i-- {
		dst[i] = src[i] - src[i-1]
	}
}

// Delta modifies src by converting it to the inverse of Delta.
func InverseDelta(dst, src []int64) {
	if len(src) == 0 || len(dst) == 0 {
		return
	}

	dst[0] = src[0]
	for i := 1; i < len(src); i++ {
		dst[i] = dst[i-1] + src[i]
	}
}

// FORDelta10 returns the minimum value, divisor and the deltas between
// succesive values in src using a frame of reference from the minimum
// and scaling each value by the largest factor of 10.  The resulting deltas
// are all positive integers
func FORDelta10(src []int64) (min, max, mod int64, rle bool, dst []int64) {
	if len(src) == 0 {
		return 0, 0, 0, false, src
	}

	// The output size
	dst = make([]int64, len(src))
	Delta(dst, src)

	min, max, mod = reference10(dst)

	// Are the deltas able to be run-length encoded?
	rle = true
	for i := 1; i < len(dst); i++ {
		dst[i] = (dst[i] - min) / mod
		// Skip the first value || see if prev = curr
		rle = i == 1 || rle && (dst[i-1] == dst[i])
	}

	rle = rle && len(dst) > 1
	return min, max, mod, rle, dst
}

func InverseFORDelta10(min, mod int64, src []int64) []int64 {
	if len(src) == 0 {
		return src
	}

	for i := 1; i < len(src); i++ {
		src[i] = (src[i] * mod) + min
	}

	dst := make([]int64, len(src))

	InverseDelta(dst, src)

	return dst
}

// reference returns the minimum and the largest common divisor of
// in that is also a factor of 10.
func reference10(src []int64) (min, max, divisor int64) {
	min = src[0]
	max = 0
	divisor = int64(1e12)
	for i, v := range src {
		if i == 0 {
			continue
		}
		if v < min {
			min = v
		}

		if v > max {
			max = v
		}

		for {
			if v%divisor == 0 {
				break
			}
			divisor /= 10
		}
	}
	return
}
