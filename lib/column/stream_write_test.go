package column

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

func TestCompositeColumnsPreserveStreamingWrites(t *testing.T) {
	tests := []struct {
		name   string
		typeOf Type
		rows   []any
	}{
		{
			name:   "NullableString",
			typeOf: "Nullable(String)",
			rows:   []any{BorrowedBytes("first"), nil, BorrowedBytes("third")},
		},
		{
			name:   "ArrayString",
			typeOf: "Array(String)",
			rows: []any{
				[]BorrowedBytes{BorrowedBytes("first"), BorrowedBytes("second")},
				[]BorrowedBytes{},
				[]BorrowedBytes{BorrowedBytes("third")},
			},
		},
		{
			name:   "TupleString",
			typeOf: "Tuple(String, String)",
			rows: []any{
				[]any{BorrowedBytes("borrowed-first"), "owned-first"},
				[]any{BorrowedBytes("borrowed-second"), "owned-second"},
			},
		},
		{
			name:   "MapString",
			typeOf: "Map(String, String)",
			rows: []any{
				map[string]string{"first": "payload-first"},
				map[string]string{"second": "payload-second"},
			},
		},
		{
			name:   "SimpleAggregateString",
			typeOf: "SimpleAggregateFunction(anyLast, String)",
			rows:   []any{BorrowedBytes("first"), BorrowedBytes("second")},
		},
		{
			name:   "NestedString",
			typeOf: "Nested(value String)",
			rows: []any{
				[]map[string]any{{"value": BorrowedBytes("first")}},
				[]map[string]any{{"value": BorrowedBytes("second")}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, err := tt.typeOf.Column("value", &ServerContext{})
			require.NoError(t, err)
			for _, row := range tt.rows {
				require.NoError(t, col.AppendRow(row))
			}
			require.Implements(t, (*customWriter)(nil), col)
			requireColumnWriteMatchesEncode(t, col)
		})
	}
}

func requireColumnWriteMatchesEncode(t *testing.T, col Interface) {
	t.Helper()

	var expected proto.Buffer
	col.Encode(&expected)

	var actual bytes.Buffer
	writer := proto.NewStreamingWriter(&actual, new(proto.Buffer))
	WriteData(writer, col)
	_, err := writer.Flush()
	require.NoError(t, err)
	require.Equal(t, expected.Buf, actual.Bytes())
}
