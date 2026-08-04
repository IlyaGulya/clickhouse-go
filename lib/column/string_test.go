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
	require.Same(t, &payload[0], &col.borrowed.RowBytes(0)[0])

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
	require.Equal(t, "first", col.Row(0, false))
	require.Equal(t, "second", col.Row(1, false))
}

func TestStringBorrowedModeHandlesLeadingNulls(t *testing.T) {
	payload := BorrowedBytes("payload")
	col := new(String)

	require.NoError(t, col.AppendRow(nil))
	require.NoError(t, col.AppendRow(payload))
	require.Equal(t, 2, col.Rows())
	require.Empty(t, col.borrowed.RowBytes(0))
	require.Equal(t, []byte("payload"), col.borrowed.RowBytes(1))
}

func TestStringEmptyValuesDoNotSelectOwnershipMode(t *testing.T) {
	col := new(String)
	require.NoError(t, col.AppendRow(""))
	require.NoError(t, col.AppendRow([]byte(nil)))
	require.NoError(t, col.AppendRow(BorrowedBytes(nil)))
	require.Equal(t, stringInputUndecided, col.inputMode)
	require.Equal(t, 3, col.Rows())

	require.NoError(t, col.AppendRow(BorrowedBytes("payload")))
	require.Equal(t, stringInputBorrowed, col.inputMode)
	require.Equal(t, 4, col.Rows())
}

func TestStringSupportsMixedOwnershipModes(t *testing.T) {
	t.Run("OwnedThenBorrowed", func(t *testing.T) {
		col := new(String)
		require.NoError(t, col.AppendRow([]byte("owned")))
		require.NoError(t, col.AppendRow(BorrowedBytes("borrowed")))
		require.Equal(t, "owned", col.Row(0, false))
		require.Equal(t, "borrowed", col.Row(1, false))
	})

	t.Run("BorrowedThenOwned", func(t *testing.T) {
		col := new(String)
		require.NoError(t, col.AppendRow(BorrowedBytes("borrowed")))
		require.NoError(t, col.AppendRow([]byte("owned")))
		require.Equal(t, "borrowed", col.Row(0, false))
		require.Equal(t, "owned", col.Row(1, false))
	})
}

func TestStringMixedOwnershipPreservesCopyContract(t *testing.T) {
	ownedBefore := []byte("owned-before")
	borrowed := []byte("borrowed")
	ownedAfter := []byte("owned-after")
	col := new(String)

	require.NoError(t, col.AppendRow(ownedBefore))
	require.NoError(t, col.AppendRow(BorrowedBytes(borrowed)))
	require.NoError(t, col.AppendRow(ownedAfter))
	ownedBefore[0] = 'X'
	borrowed[0] = 'B'
	ownedAfter[0] = 'X'

	require.Equal(t, "owned-before", col.Row(0, false))
	require.Equal(t, "Borrowed", col.Row(1, false))
	require.Equal(t, "owned-after", col.Row(2, false))
}

func TestStringAppendBorrowedBytesColumnWithoutSliceConversion(t *testing.T) {
	values := [][]byte{[]byte("first"), []byte("second")}
	col := new(String)

	nulls, err := col.Append(BorrowedBytesColumn(values))
	require.NoError(t, err)
	require.Equal(t, []uint8{0, 0}, nulls)
	require.Same(t, &values[0][0], &col.borrowed.RowBytes(0)[0])
	require.Same(t, &values[1][0], &col.borrowed.RowBytes(1)[0])

	values[0] = []byte("other")
	require.Equal(t, "first", col.Row(0, false))
}

func TestStringResetReleasesBorrowedValues(t *testing.T) {
	col := new(String)
	require.NoError(t, col.AppendRow(BorrowedBytes("payload")))
	col.Reset()

	require.Zero(t, col.Rows())
	require.Empty(t, col.borrowed.Values)
	require.Equal(t, stringInputUndecided, col.inputMode)
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
