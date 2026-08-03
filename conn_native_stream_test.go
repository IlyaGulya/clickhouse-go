package clickhouse

import (
	"errors"
	"testing"

	"github.com/ClickHouse/ch-go/compress"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestWriteCompressedBlockPropagatesConnectionFailure(t *testing.T) {
	writeErr := errors.New("connection closed during block write")
	conn := createMockConnect(&mockNetConn{writeErr: writeErr})
	conn.compressor = compress.NewWriter(compress.LevelZero, compress.LZ4)
	conn.maxCompressionBuffer = 1
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("value", column.Type("String")))
	require.NoError(t, block.Append(column.BorrowedBytes("borrowed payload")))

	err := conn.writeCompressedBlock(block)
	require.ErrorIs(t, err, writeErr)
}
