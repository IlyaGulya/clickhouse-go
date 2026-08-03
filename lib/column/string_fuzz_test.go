package column

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func FuzzStringBorrowedMatchesOwned(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("\x00"))
	f.Add([]byte("\x00\x03abc\x01\x05hello"))
	f.Add(bytes.Repeat([]byte("payload"), 1024))
	f.Add(fuzzStringLengthSeed(127))
	f.Add(fuzzStringLengthSeed(128))
	f.Add(fuzzStringLengthSeed(16_383))
	f.Add(fuzzStringLengthSeed(16_384))

	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 1<<16 {
			input = input[:1<<16]
		}
		var owned String
		var borrowed String
		var mixed String
		for len(input) > 0 {
			control := input[0]
			input = input[1:]
			if control&3 == 0 {
				if err := owned.AppendRow(nil); err != nil {
					t.Fatalf("append owned nil: %v", err)
				}
				if err := borrowed.AppendRow(nil); err != nil {
					t.Fatalf("append borrowed nil: %v", err)
				}
				if err := mixed.AppendRow(nil); err != nil {
					t.Fatalf("append mixed nil: %v", err)
				}
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
			value := input[:size]
			input = input[size:]
			if err := owned.AppendRow([]byte(value)); err != nil {
				t.Fatalf("append owned value: %v", err)
			}
			if err := borrowed.AppendRow(BorrowedBytes(value)); err != nil {
				t.Fatalf("append borrowed value: %v", err)
			}
			var mixedValue any = []byte(value)
			if mixed.Rows()%2 == 0 {
				mixedValue = BorrowedBytes(value)
			}
			if err := mixed.AppendRow(mixedValue); err != nil {
				t.Fatalf("append mixed value: %v", err)
			}
		}

		if owned.Rows() != borrowed.Rows() || owned.Rows() != mixed.Rows() {
			t.Fatalf("row count differs: owned=%d borrowed=%d mixed=%d", owned.Rows(), borrowed.Rows(), mixed.Rows())
		}
		for i := 0; i < owned.Rows(); i++ {
			if owned.Row(i, false) != borrowed.Row(i, false) {
				t.Fatalf("row %d differs: owned=%q borrowed=%q", i, owned.Row(i, false), borrowed.Row(i, false))
			}
			if owned.Row(i, false) != mixed.Row(i, false) {
				t.Fatalf("row %d differs: owned=%q mixed=%q", i, owned.Row(i, false), mixed.Row(i, false))
			}
		}

		var expected proto.Buffer
		owned.Encode(&expected)
		var actual proto.Buffer
		borrowed.Encode(&actual)
		if !bytes.Equal(expected.Buf, actual.Buf) {
			t.Fatal("contiguous encoding differs")
		}
		var mixedEncoded proto.Buffer
		mixed.Encode(&mixedEncoded)
		if !bytes.Equal(expected.Buf, mixedEncoded.Buf) {
			t.Fatal("mixed contiguous encoding differs")
		}

		var streamed bytes.Buffer
		writer := proto.NewStreamingWriter(&streamed, new(proto.Buffer))
		borrowed.Write(writer)
		if _, err := writer.Flush(); err != nil {
			t.Fatalf("flush borrowed String: %v", err)
		}
		if !bytes.Equal(expected.Buf, streamed.Bytes()) {
			t.Fatal("streaming encoding differs")
		}

		borrowed.Reset()
		if borrowed.Rows() != 0 || len(borrowed.borrowed.Values) != 0 || borrowed.pendingEmpties != 0 {
			t.Fatalf("reset retained borrowed state")
		}
	})
}

func fuzzStringLengthSeed(size int) []byte {
	seed := []byte{0xff, byte(size), byte(size >> 8)}
	return append(seed, bytes.Repeat([]byte{'x'}, size)...)
}
