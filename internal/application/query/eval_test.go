package query

import (
	"math/big"
	"testing"
)

func TestUnsignedComparisonDoesNotWrap(t *testing.T) {
	value := ^uint(0)
	actual, ok := toRat(value)
	expected := new(big.Rat).SetInt(new(big.Int).SetUint64(uint64(value)))
	if !ok || actual.Cmp(expected) != 0 {
		t.Fatal("unsigned value wrapped into a negative number")
	}
	if compareValues(value, int64(0)) <= 0 {
		t.Fatal("unsigned maximum must sort after zero")
	}
}
