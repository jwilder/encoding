package simple8b_test

import (
	"testing"

	"github.com/jwilder/encoding/simple8b"
)

func Test_Encode_NoValues(t *testing.T) {
	var in []uint64
	encoded, _ := simple8b.Encode(in)

	decoded := make([]uint64, len(in))
	n, _ := simple8b.Decode(decoded, encoded)

	if len(in) != len(decoded[:n]) {
		t.Fatalf("Len mismatch: got %v, exp %v", len(decoded), len(in))
	}
}

func Test_TooBig(t *testing.T) {
	values := 1
	in := make([]uint64, values)
	for i := 0; i < values; i++ {
		in[i] = 2<<61 - 1
	}
	_, err := simple8b.Encode(in)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func Test_FewValues(t *testing.T) {
	testEncode(t, 20, 2)
}

func Test_Encode_240Ones(t *testing.T) {
	testEncode(t, 240, 1)
}

func Test_Encode_120Zeros(t *testing.T) {
	testEncode(t, 120, 1)
}

func Test_Encode_60(t *testing.T) {
	testEncode(t, 60, 1)
}

func Test_Encode_30(t *testing.T) {
	testEncode(t, 30, 3)
}

func Test_Encode_20(t *testing.T) {
	testEncode(t, 20, 7)
}

func Test_Encode_15(t *testing.T) {
	testEncode(t, 15, 15)
}

func Test_Encode_12(t *testing.T) {
	testEncode(t, 12, 31)
}

func Test_Encode_10(t *testing.T) {
	testEncode(t, 10, 63)
}

func Test_Encode_8(t *testing.T) {
	testEncode(t, 8, 127)
}

func Test_Encode_7(t *testing.T) {
	testEncode(t, 7, 255)
}

func Test_Encode_6(t *testing.T) {
	testEncode(t, 6, 1023)
}

func Test_Encode_5(t *testing.T) {
	testEncode(t, 5, 4095)
}

func Test_Encode_4(t *testing.T) {
	testEncode(t, 4, 32767)
}

func Test_Encode_3(t *testing.T) {
	testEncode(t, 3, 1048575)
}

func Test_Encode_2(t *testing.T) {
	testEncode(t, 2, 1073741823)
}

func Test_Encode_1(t *testing.T) {
	testEncode(t, 1, 1152921504606846975)
}

func testEncode(t *testing.T, n int, val uint64) {
	in := make([]uint64, n)
	for i := 0; i < n; i++ {
		in[i] = val
	}
	encoded, _ := simple8b.Encode(in)
	if exp, got := len(in)/n, len(encoded); got != exp {
		t.Fatalf("Encode len mismatch: exp %v, got %v", exp, got)
	}

	decoded := make([]uint64, len(in))
	nv, _ := simple8b.Decode(decoded, encoded)
	if exp, got := n, len(decoded[:nv]); got != exp {
		t.Fatalf("Decode len mismatch: exp %v, got %v", exp, got)
	}

	for i := 0; i < n; i++ {
		if exp := uint64(val); decoded[i] != exp {
			t.Fatalf("Decoded[%d] != %v, got %v", i, exp, decoded[i])
		}
	}
}

func Test_Bytes(t *testing.T) {
	enc := simple8b.NewEncoder()
	for i := 0; i < 30; i++ {
		enc.Write(uint64(i))
	}
	b, _ := enc.Bytes()

	dec := simple8b.NewDecoder(b)
	x := uint64(0)
	for dec.Next() {
		if x != dec.Read() {
			t.Fatalf("mismatch: got %v, exp %v", dec.Read(), x)
		}
		x += 1
	}
}

func Test_Encode_ValueTooLarge(t *testing.T) {
	enc := simple8b.NewEncoder()

	values := []uint64{
		1442369134000000000, 0,
	}

	for _, v := range values {
		enc.Write(v)
	}

	_, err := enc.Bytes()
	if err == nil {
		t.Fatalf("Expected error, got nil")

	}
}

func BenchmarkEncode(b *testing.B) {
	total := 0
	x := make([]uint64, 1024)
	for i := 0; i < len(x); i++ {
		x[i] = uint64(15)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		simple8b.Encode(x)
		b.SetBytes(int64(len(x) * 8))
		total += len(x)
	}
}

func BenchmarkDecode(b *testing.B) {
	total := 0

	x := make([]uint64, 1024)
	for i := 0; i < len(x); i++ {
		x[i] = uint64(10)
	}
	y, _ := simple8b.Encode(x)

	decoded := make([]uint64, len(x))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = simple8b.Decode(decoded, y)
		b.SetBytes(int64(len(decoded) * 8))
		total += len(decoded)
	}
}
