package column

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

func TestStringAppendRowBorrowedBytes(t *testing.T) {
	payload := []byte("payload")
	col := new(String)

	require.NoError(t, col.AppendRow(BorrowedBytes(payload)))
	require.Empty(t, col.col.Buf)
	require.Same(t, &payload[0], &col.col.RowBytes(0)[0])

	payload[0] = 'P'
	var encoded proto.Buffer
	col.Encode(&encoded)

	var decoded proto.ColStr
	require.NoError(t, decoded.DecodeColumn(proto.NewReader(bytes.NewReader(encoded.Buf)), 1))
	require.Equal(t, "Payload", decoded.Row(0))
}

func TestStringAppendBorrowedBytesColumn(t *testing.T) {
	first := BorrowedBytes("first")
	second := BorrowedBytes("second")
	col := new(String)

	nulls, err := col.Append([]BorrowedBytes{first, second})
	require.NoError(t, err)
	require.Equal(t, []uint8{0, 0}, nulls)
	require.Equal(t, "first", col.col.Row(0))
	require.Equal(t, "second", col.col.Row(1))
}

func BenchmarkStringAppendLargeBytes(b *testing.B) {
	payload := make([]byte, 1<<20)

	b.Run("Copied", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			col := new(String)
			if err := col.AppendRow(payload); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Borrowed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			col := new(String)
			if err := col.AppendRow(BorrowedBytes(payload)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkStringEncodeLargeBytes(b *testing.B) {
	payload := make([]byte, 1<<20)

	b.Run("Copied", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			col := new(String)
			if err := col.AppendRow(payload); err != nil {
				b.Fatal(err)
			}
			var encoded proto.Buffer
			col.Encode(&encoded)
		}
	})

	b.Run("Borrowed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			col := new(String)
			if err := col.AppendRow(BorrowedBytes(payload)); err != nil {
				b.Fatal(err)
			}
			var encoded proto.Buffer
			col.Encode(&encoded)
		}
	})
}
