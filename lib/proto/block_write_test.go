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
	require.NoError(t, block.WriteHeader(writer, 0))
	for i := range block.Columns {
		require.NoError(t, block.WriteColumn(writer, 0, i))
	}
	_, err := writer.Flush()
	require.NoError(t, err)
	require.Equal(t, expected.Buf, output.Bytes())
}
