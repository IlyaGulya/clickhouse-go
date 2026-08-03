package clickhouse

import (
	"bytes"
	"io"
	"testing"

	"github.com/ClickHouse/ch-go/compress"
	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestHTTPWriteDataToMatchesBlockEncoding(t *testing.T) {
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("value", column.Type("String")))
	require.NoError(t, block.AddColumn("nullable_value", column.Type("Nullable(String)")))
	require.NoError(t, block.AddColumn("array_value", column.Type("Array(String)")))
	require.NoError(t, block.Append(
		column.BorrowedBytes("payload"),
		column.BorrowedBytes("nullable"),
		[]column.BorrowedBytes{column.BorrowedBytes("first"), column.BorrowedBytes("second")},
	))

	var expected chproto.Buffer
	require.NoError(t, block.Encode(&expected, 0))

	for _, method := range []CompressionMethod{CompressionNone, CompressionLZ4, CompressionZSTD} {
		t.Run(method.String(), func(t *testing.T) {
			conn := httpConnect{
				compression:     method,
				blockCompressor: compress.NewWriter(compress.LevelZero, compress.Method(method)),
			}
			var encoded bytes.Buffer
			require.NoError(t, conn.writeDataTo(&encoded, block))

			actual := encoded.Bytes()
			if method == CompressionLZ4 || method == CompressionZSTD {
				decoded := make([]byte, len(expected.Buf))
				_, err := io.ReadFull(compress.NewReader(bytes.NewReader(actual)), decoded)
				require.NoError(t, err)
				actual = decoded
			}
			require.Equal(t, expected.Buf, actual)
		})
	}
}
