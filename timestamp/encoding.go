// Package timestamp provides structs and functions for converting streams of timestamps
// to byte slices.  The encoded format is
package timestamp

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/jwilder/encoding/delta"
	"github.com/jwilder/encoding/simple8b"
)

type Encoder interface {
	Write(t time.Time)
	Bytes() ([]byte, error)
}

type Decoder interface {
	Next() bool
	Read() time.Time
}

type encoder struct {
	ts []int64
}

func NewEncoder() Encoder {
	return &encoder{}
}

func (e *encoder) Write(t time.Time) {
	e.ts = append(e.ts, t.UnixNano())
}

func (e *encoder) Bytes() ([]byte, error) {
	if len(e.ts) == 0 {
		return []byte{}, nil
	}

	min, mod, dts := delta.FORDelta10(e.ts)

	enc := simple8b.NewEncoder()
	for _, v := range dts[1:] {
		enc.Write(uint64(v))
	}

	b := make([]byte, 8*2+1)
	binary.BigEndian.PutUint64(b[0:8], uint64(min))
	b[8] = byte(math.Log10(float64(mod)))
	binary.BigEndian.PutUint64(b[9:17], uint64(dts[0]))

	deltas, err := enc.Bytes()
	if err != nil {
		return nil, err
	}

	return append(b, deltas...), nil
}

type decoder struct {
	v  time.Time
	ts []int64
}

func NewDecoder(b []byte) Decoder {
	d := &decoder{}
	d.decode(b)
	return d
}

func (d *decoder) Next() bool {
	if len(d.ts) == 0 {
		return false
	}
	d.v = time.Unix(0, d.ts[0])
	d.ts = d.ts[1:]
	return true
}

func (d *decoder) Read() time.Time {
	return d.v
}

func (d *decoder) decode(b []byte) {
	if len(b) == 0 {
		return
	}

	min := int64(binary.BigEndian.Uint64(b[0:8]))
	mod := int64(math.Pow10(int(b[8])))
	first := int64(binary.BigEndian.Uint64(b[9:17]))

	enc := simple8b.NewDecoder(b[17:])

	deltas := []int64{first}
	for enc.Next() {
		deltas = append(deltas, int64(enc.Read()))
	}

	d.ts = delta.InverseFORDelta10(min, mod, deltas)
}
