package column

import (
	"bytes"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLowCardinalityBorrowedString(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)
	lc := col.(*LowCardinality)

	first := BorrowedBytes([]byte("payload"))
	second := BorrowedBytes([]byte("payload"))
	require.NoError(t, lc.AppendRow(first))
	require.NoError(t, lc.AppendRow(second))
	require.Equal(t, 2, lc.Rows())
	require.Len(t, lc.append.borrowedIndex, 2)

	index := lc.index.(*String)
	require.Equal(t, 3, index.Rows())
	require.Same(t, &first[0], &index.borrowed.RowBytes(1)[0])
	require.Same(t, &second[0], &index.borrowed.RowBytes(2)[0])
	require.Equal(t, []int{1, 2}, lc.append.keys)
}

func TestLowCardinalityBorrowedStringDeduplicatesSourceIdentity(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)
	lc := col.(*LowCardinality)

	value := BorrowedBytes([]byte("payload"))
	require.NoError(t, lc.AppendRow(value))
	require.NoError(t, lc.AppendRow(value))
	require.Equal(t, 2, lc.index.Rows())
	require.Equal(t, []int{1, 1}, lc.append.keys)
}

func TestLowCardinalityBorrowedStringDoesNotRetainEmptyBackingArray(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)
	lc := col.(*LowCardinality)

	value := BorrowedBytes(make([]byte, 0, 1<<20))
	require.NoError(t, lc.AppendRow(value))
	require.Len(t, lc.append.borrowedIndex, 1)
	for source := range lc.append.borrowedIndex {
		require.Nil(t, source.data)
	}
}

func TestLowCardinalityBorrowedStringSupportsMixedOwnership(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)
	lc := col.(*LowCardinality)

	require.NoError(t, lc.AppendRow(BorrowedBytes("borrowed")))
	require.NoError(t, lc.AppendRow("owned"))
	require.Equal(t, 2, lc.Rows())
	var encoded chproto.Buffer
	lc.Encode(&encoded)
	require.Equal(t, "borrowed", lc.Row(0, false))
	require.Equal(t, "owned", lc.Row(1, false))
}

func TestLowCardinalityMixedOwnershipUsesSeparateDictionaryEntries(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)
	lc := col.(*LowCardinality)

	require.NoError(t, lc.AppendRow(BorrowedBytes("same")))
	require.NoError(t, lc.AppendRow("same"))
	require.Equal(t, 3, lc.index.Rows())
	require.Equal(t, []int{1, 2}, lc.append.keys)
}

func TestLowCardinalityAppendBorrowedBytesColumn(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)
	lc := col.(*LowCardinality)
	values := [][]byte{[]byte("first"), []byte("second"), []byte("first")}

	nulls, err := lc.Append(BorrowedBytesColumn(values))
	require.NoError(t, err)
	require.Equal(t, []uint8{0, 0, 0}, nulls)
	require.Equal(t, 3, lc.Rows())
	require.Equal(t, 4, lc.index.Rows())
}

func TestLowCardinalityWriteMatchesEncode(t *testing.T) {
	for _, typeOf := range []Type{
		"LowCardinality(String)",
		"LowCardinality(Nullable(String))",
	} {
		t.Run(string(typeOf), func(t *testing.T) {
			encoded := appendLowCardinalityBorrowedRows(t, typeOf)
			streamed := appendLowCardinalityBorrowedRows(t, typeOf)

			var expected chproto.Buffer
			encoded.Encode(&expected)
			var actual bytes.Buffer
			writer := chproto.NewStreamingWriter(&actual, new(chproto.Buffer))
			WriteData(writer, streamed)
			_, err := writer.Flush()
			require.NoError(t, err)
			require.Equal(t, expected.Buf, actual.Bytes())
		})
	}
}

func appendLowCardinalityBorrowedRows(t *testing.T, typeOf Type) *LowCardinality {
	t.Helper()
	col, err := typeOf.Column("test", nil)
	require.NoError(t, err)
	lc := col.(*LowCardinality)
	for _, value := range []any{
		BorrowedBytes("first"),
		BorrowedBytes("second"),
		BorrowedBytes("first"),
		nil,
		BorrowedBytes(nil),
	} {
		require.NoError(t, lc.AppendRow(value))
	}
	return lc
}

func TestLowCardinalityAppendAnySlice(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for i := range 10 {
		err := lc.AppendRow("value_" + string(rune('A'+i)))
		assert.NoError(t, err)
	}

	assert.Equal(t, 10, lc.Rows())
}

func TestLowCardinalityAppendAnySliceManyRows(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for i := range 500 {
		err := lc.AppendRow("value_" + string(rune('A'+i%26)))
		assert.NoError(t, err)
	}

	assert.Equal(t, 500, lc.Rows())
}

func TestLowCardinalityResetAfterEncode(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for range 10 {
		err := lc.AppendRow("value")
		require.NoError(t, err)
	}

	require.NotNil(t, lc.append.index)

	var buf chproto.Buffer
	lc.Encode(&buf)

	assert.Nil(t, lc.append.index)

	lc.Reset()

	require.NotNil(t, lc.append.index)

	err = lc.AppendRow("new_value")
	assert.NoError(t, err)
}

func TestLowCardinalityAppendAfterEncodeWithoutReset(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for range 10 {
		err := lc.AppendRow("value")
		require.NoError(t, err)
	}

	require.NotNil(t, lc.append.index)

	var buf chproto.Buffer
	lc.Encode(&buf)

	assert.Nil(t, lc.append.index)

	err = lc.AppendRow("new_value")
	assert.NoError(t, err)
}

func TestLowCardinalityEncodeThenResetThenAppend(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for range 10 {
		err := lc.AppendRow("value")
		require.NoError(t, err)
	}

	var buf chproto.Buffer
	lc.Encode(&buf)

	assert.Nil(t, lc.append.index)

	lc.Reset()

	require.NotNil(t, lc.append.index)

	err = lc.AppendRow("new_value")
	assert.NoError(t, err)
	assert.Equal(t, 1, lc.Rows())
}

func TestLowCardinalityAppendManyRowsWithoutPanic(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for i := range 1000 {
		err := lc.AppendRow("value_" + string(rune('A'+i%26)))
		assert.NoError(t, err, "Failed at row %d", i)
	}

	assert.Equal(t, 1000, lc.Rows())
}
