package proto

import (
	"bytes"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
)

func TestBlockWriteMatchesEncode(t *testing.T) {
	block := NewBlock()
	require.NoError(t, block.AddColumn("payload", column.Type("String")))
	require.NoError(t, block.AddColumn("sequence", column.Type("UInt64")))
	require.NoError(t, block.Append(column.BorrowedBytes("first"), uint64(1)))
	require.NoError(t, block.Append(column.BorrowedBytes("second"), uint64(2)))

	var expected chproto.Buffer
	require.NoError(t, block.Encode(&expected, 0))

	var output bytes.Buffer
	writer := chproto.NewStreamingWriter(&output, new(chproto.Buffer))
	require.NoError(t, block.Write(writer, 0))
	_, err := writer.Flush()
	require.NoError(t, err)
	require.Equal(t, expected.Buf, output.Bytes())
}

func FuzzBlockWriteMatchesEncode(f *testing.F) {
	f.Add([]byte(nil), byte(0))
	f.Add([]byte("first\x00second"), byte(1))
	f.Add(bytes.Repeat([]byte("payload"), 1024), byte(2))
	f.Add(fuzzBlockLengthSeed(127), byte(0))
	f.Add(fuzzBlockLengthSeed(128), byte(1))
	f.Add(fuzzBlockLengthSeed(16_383), byte(0))
	f.Add(fuzzBlockLengthSeed(16_384), byte(1))

	f.Fuzz(func(t *testing.T, input []byte, revisionSelector byte) {
		if len(input) > 1<<16 {
			input = input[:1<<16]
		}
		block := NewBlock()
		require.NoError(t, block.AddColumn("payload", column.Type("String")))
		require.NoError(t, block.AddColumn("sequence", column.Type("UInt64")))
		for i, value := range fuzzBlockStrings(input) {
			require.NoError(t, block.Append(column.BorrowedBytes(value), uint64(i)))
		}

		revision := uint64(0)
		if revisionSelector&1 != 0 {
			revision = DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION
		}

		var expected chproto.Buffer
		require.NoError(t, block.Encode(&expected, revision))

		var output bytes.Buffer
		writer := chproto.NewStreamingWriter(&output, new(chproto.Buffer))
		require.NoError(t, block.Write(writer, revision))
		_, err := writer.Flush()
		require.NoError(t, err)
		require.Equal(t, expected.Buf, output.Bytes())
	})
}

func fuzzBlockStrings(input []byte) [][]byte {
	values := make([][]byte, 0, len(input)/8+1)
	for len(input) > 0 {
		size := int(input[0] >> 2)
		input = input[1:]
		if size == 63 && len(input) >= 2 {
			size = int(input[0]) | int(input[1])<<8
			input = input[2:]
		}
		if size > len(input) {
			size = len(input)
		}
		values = append(values, input[:size])
		input = input[size:]
	}
	return values
}

func fuzzBlockLengthSeed(size int) []byte {
	seed := []byte{0xff, byte(size), byte(size >> 8)}
	return append(seed, bytes.Repeat([]byte{'x'}, size)...)
}
