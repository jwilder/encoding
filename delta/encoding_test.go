package delta_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/jwilder/encoding/delta"
)

func Test_FORDelta10(t *testing.T) {
	x := []int64{}
	now := time.Now()
	x = append(x, now.UnixNano())
	for i := 1; i < 25; i++ {
		x = append(x, now.Add(
			time.Duration(rand.Intn(1e2))*time.Second).UnixNano())
	}
	y := make([]int64, len(x))
	copy(y, x)

	min, mod, d2 := delta.FORDelta10(x)
	d3 := delta.InverseFORDelta10(min, mod, d2)

	for i, v := range d3 {
		if v != y[i] {
			t.Fatalf("Item %d mismatch, got %v, exp %v", i, v, y[i])
		}
	}
}

func Test_FORDelta10_NoValues(t *testing.T) {
	min, mod, x := delta.FORDelta10([]int64{})
	y := delta.InverseFORDelta10(min, mod, x)

	if len(x) != len(y) {
		t.Fatalf("FORDelta10 len mismatch: got %v, exp %v", len(y), 0)
	}
}
