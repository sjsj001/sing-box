package snellv5

import (
	"bytes"
	"math/bits"
	"testing"
)

// The shuffle sets exactly the bits it was asked for, and two paddings with
// the same count differ: the seeded stream is per padding, not a constant.
func TestFillPaddingWithBitCount(t *testing.T) {
	t.Parallel()
	for _, ones := range []int{0, 1, 1000, 2000, 3999, 4000} {
		var paddings [2][]byte
		for i := range paddings {
			random, err := newPaddingRandom()
			if err != nil {
				t.Fatal(err)
			}
			paddings[i] = make([]byte, 500)
			if err := fillPaddingWithBitCount(random, paddings[i], ones); err != nil {
				t.Fatal(err)
			}
			got := 0
			for _, b := range paddings[i] {
				got += bits.OnesCount8(b)
			}
			if got != ones {
				t.Fatalf("padding with %d ones asked has %d", ones, got)
			}
		}
		if ones > 0 && ones < 4000 && bytes.Equal(paddings[0], paddings[1]) {
			t.Fatalf("two paddings with %d ones are identical", ones)
		}
	}
}
