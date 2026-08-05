package column

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func FuzzLowCardinalityBorrowedMatchesOwned(f *testing.F) {
	f.Add([]byte(nil), false)
	f.Add([]byte("\x00"), true)
	f.Add([]byte("\x17first\x1bsecond\x17first\x00"), false)
	f.Add(bytes.Repeat([]byte("low-cardinality"), 512), true)
	f.Add(fuzzLowCardinalityUniqueSeed(254), false)
	f.Add(fuzzLowCardinalityUniqueSeed(255), false)
	f.Add(fuzzLowCardinalityUniqueSeed(254), true)
	f.Add(fuzzLowCardinalityUniqueSeed(255), true)

	f.Fuzz(func(t *testing.T, input []byte, nullable bool) {
		if len(input) > 1<<16 {
			input = input[:1<<16]
		}
		values := fuzzLowCardinalityValues(input)
		typeOf := Type("LowCardinality(String)")
		if nullable {
			typeOf = "LowCardinality(Nullable(String))"
		}

		owned := fuzzLowCardinalityColumn(t, typeOf, values, false)
		borrowed := fuzzLowCardinalityColumn(t, typeOf, values, true)
		streamed := fuzzLowCardinalityColumn(t, typeOf, values, true)
		mixed := fuzzLowCardinalityMixedColumn(t, typeOf, values)

		var expected proto.Buffer
		owned.Encode(&expected)
		var actual proto.Buffer
		borrowed.Encode(&actual)
		fuzzRequireLowCardinalityRows(t, typeOf, nullable, values, expected.Buf)
		fuzzRequireLowCardinalityRows(t, typeOf, nullable, values, actual.Buf)
		var mixedEncoded proto.Buffer
		mixed.Encode(&mixedEncoded)
		fuzzRequireLowCardinalityRows(t, typeOf, nullable, values, mixedEncoded.Buf)

		var streamedBytes bytes.Buffer
		writer := proto.NewStreamingWriter(&streamedBytes, new(proto.Buffer))
		streamed.write(writer)
		if _, err := writer.Flush(); err != nil {
			t.Fatalf("flush LowCardinality stream: %v", err)
		}
		if !bytes.Equal(actual.Buf, streamedBytes.Bytes()) {
			t.Fatal("contiguous and streaming LowCardinality encodings differ")
		}

		borrowed.Reset()
		if borrowed.Rows() != 0 || len(borrowed.append.index) != 0 || len(borrowed.append.borrowedIndex) != 0 {
			t.Fatal("reset retained borrowed LowCardinality state")
		}
		stringColumn := borrowed.index
		if nullableColumn, ok := stringColumn.(*Nullable); ok {
			stringColumn = nullableColumn.Base()
		}
		if len(stringColumn.(*String).borrowed.Values) != 0 {
			t.Fatal("reset retained borrowed LowCardinality values")
		}
	})
}

func fuzzRequireLowCardinalityRows(t *testing.T, typeOf Type, nullable bool, values []fuzzLowCardinalityValue, encoded []byte) {
	t.Helper()
	column, err := typeOf.Column("value", nil)
	if err != nil {
		t.Fatalf("create decoded LowCardinality column: %v", err)
	}
	decoded := column.(*LowCardinality)
	if err := decoded.Decode(proto.NewReader(bytes.NewReader(encoded)), len(values)); err != nil {
		t.Fatalf("decode LowCardinality column: %v", err)
	}
	for i, value := range values {
		got := decoded.Row(i, false)
		if nullable && value.isNull {
			if got != nil {
				t.Fatalf("row %d: got %v, want NULL", i, got)
			}
			continue
		}
		want := string(value.value)
		var gotString string
		switch got := got.(type) {
		case string:
			gotString = got
		case *string:
			gotString = *got
		default:
			t.Fatalf("row %d: got type %T, want String", i, got)
		}
		if gotString != want {
			t.Fatalf("row %d: got %q, want %q", i, gotString, want)
		}
	}
}

func fuzzLowCardinalityMixedColumn(t *testing.T, typeOf Type, values []fuzzLowCardinalityValue) *LowCardinality {
	t.Helper()
	column, err := typeOf.Column("value", nil)
	if err != nil {
		t.Fatalf("create mixed LowCardinality column: %v", err)
	}
	lowCardinality := column.(*LowCardinality)
	for i, value := range values {
		var row any
		switch {
		case value.isNull:
			row = nil
		case i%2 == 0:
			row = BorrowedBytes(value.value)
		default:
			row = string(value.value)
		}
		if err := lowCardinality.AppendRow(row); err != nil {
			t.Fatalf("append mixed LowCardinality row: %v", err)
		}
	}
	return lowCardinality
}

type fuzzLowCardinalityValue struct {
	isNull bool
	value  []byte
}

func fuzzLowCardinalityValues(input []byte) []fuzzLowCardinalityValue {
	values := make([]fuzzLowCardinalityValue, 0, len(input)/8+1)
	for len(input) > 0 {
		control := input[0]
		input = input[1:]
		if control&3 == 0 {
			values = append(values, fuzzLowCardinalityValue{isNull: true})
			continue
		}
		size := int(control >> 2)
		if size == 63 && len(input) >= 2 {
			size = int(input[0]) | int(input[1])<<8
			input = input[2:]
		}
		if size > len(input) {
			size = len(input)
		}
		values = append(values, fuzzLowCardinalityValue{value: input[:size]})
		input = input[size:]
	}
	return values
}

func fuzzLowCardinalityUniqueSeed(count int) []byte {
	seed := make([]byte, 0, count*3)
	for i := range count {
		seed = append(seed, 2<<2|1, byte(i), byte(i>>8))
	}
	return seed
}

func fuzzLowCardinalityColumn(t *testing.T, typeOf Type, values []fuzzLowCardinalityValue, borrowed bool) *LowCardinality {
	t.Helper()
	column, err := typeOf.Column("value", nil)
	if err != nil {
		t.Fatalf("create LowCardinality column: %v", err)
	}
	lowCardinality := column.(*LowCardinality)
	for _, value := range values {
		var row any
		switch {
		case value.isNull:
			row = nil
		case borrowed:
			row = BorrowedBytes(value.value)
		default:
			row = string(value.value)
		}
		if err := lowCardinality.AppendRow(row); err != nil {
			t.Fatalf("append LowCardinality row: %v", err)
		}
	}
	return lowCardinality
}
