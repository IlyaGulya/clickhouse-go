package tests

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type testStr struct {
	Col1 string
}

func TestBorrowedString(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)

		ctx := context.Background()
		const ddl = `
		CREATE TABLE test_borrowed_string (
			value String,
			nullable_value Nullable(String),
			array_value Array(String),
			low_cardinality_value LowCardinality(String),
			nullable_low_cardinality_value LowCardinality(Nullable(String)),
			low_cardinality_array Array(LowCardinality(String))
		) Engine MergeTree() ORDER BY tuple()
		`
		t.Cleanup(func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_borrowed_string"))
		})
		require.NoError(t, conn.Exec(ctx, ddl))

		value := []byte("payload")
		nullableValue := []byte("nullable")
		firstArrayValue := []byte("first")
		secondArrayValue := []byte("second")
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_borrowed_string")
		require.NoError(t, err)
		require.NoError(t, batch.Append(
			clickhouse.BorrowBytes(value),
			clickhouse.BorrowBytes(nullableValue),
			[]clickhouse.BorrowedBytes{
				clickhouse.BorrowBytes(firstArrayValue),
				clickhouse.BorrowBytes(secondArrayValue),
			},
			clickhouse.BorrowBytes(value),
			clickhouse.BorrowBytes(nullableValue),
			[]clickhouse.BorrowedBytes{
				clickhouse.BorrowBytes(firstArrayValue),
				clickhouse.BorrowBytes(secondArrayValue),
				clickhouse.BorrowBytes(firstArrayValue),
			},
		))
		require.NoError(t, batch.Append(
			clickhouse.BorrowBytes(nil),
			nil,
			[]clickhouse.BorrowedBytes{},
			clickhouse.BorrowBytes(nil),
			nil,
			[]clickhouse.BorrowedBytes{},
		))
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, `
			SELECT value, nullable_value, array_value,
				low_cardinality_value, nullable_low_cardinality_value, low_cardinality_array
			FROM test_borrowed_string
			ORDER BY length(value) DESC
		`)
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var gotValue string
		var gotNullable *string
		var gotArray []string
		var gotLowCardinality string
		var gotNullableLowCardinality *string
		var gotLowCardinalityArray []string
		require.NoError(t, rows.Scan(
			&gotValue,
			&gotNullable,
			&gotArray,
			&gotLowCardinality,
			&gotNullableLowCardinality,
			&gotLowCardinalityArray,
		))
		require.Equal(t, "payload", gotValue)
		require.NotNil(t, gotNullable)
		require.Equal(t, "nullable", *gotNullable)
		require.Equal(t, []string{"first", "second"}, gotArray)
		require.Equal(t, "payload", gotLowCardinality)
		require.NotNil(t, gotNullableLowCardinality)
		require.Equal(t, "nullable", *gotNullableLowCardinality)
		require.Equal(t, []string{"first", "second", "first"}, gotLowCardinalityArray)

		require.True(t, rows.Next())
		gotNullableLowCardinality = nil
		require.NoError(t, rows.Scan(
			&gotValue,
			&gotNullable,
			&gotArray,
			&gotLowCardinality,
			&gotNullableLowCardinality,
			&gotLowCardinalityArray,
		))
		require.Empty(t, gotValue)
		require.Nil(t, gotNullable)
		require.Empty(t, gotArray)
		require.Empty(t, gotLowCardinality)
		require.Nil(t, gotNullableLowCardinality)
		require.Empty(t, gotLowCardinalityArray)
		require.NoError(t, rows.Err())
	})
}

func TestBorrowedStringCompressionBufferLimit(t *testing.T) {
	conn, err := GetConnectionTCPWithOptions(testSet, nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	}, func(options *clickhouse.Options) {
		options.MaxCompressionBuffer = 1 << 20
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := context.Background()
	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE test_borrowed_string_buffer_limit (
			value String
		) Engine MergeTree() ORDER BY tuple()
	`))
	t.Cleanup(func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_borrowed_string_buffer_limit"))
	})

	payload := makeBorrowedRandomPayload(4 << 20)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_borrowed_string_buffer_limit")
	require.NoError(t, err)
	require.NoError(t, batch.Append(clickhouse.BorrowBytes(payload)))
	require.NoError(t, batch.Send())

	var actual []byte
	require.NoError(t, conn.QueryRow(ctx, "SELECT value FROM test_borrowed_string_buffer_limit").Scan(&actual))
	require.True(t, bytes.Equal(payload, actual))
}

func TestBorrowedStringMixedAndColumnar(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)

		ctx := context.Background()
		const table = "test_borrowed_string_mixed"
		require.NoError(t, conn.Exec(ctx, `
			CREATE TABLE test_borrowed_string_mixed (
				sequence UInt8,
				value String,
				low_cardinality LowCardinality(String)
			) Engine MergeTree() ORDER BY sequence
		`))
		t.Cleanup(func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table))
		})

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
		require.NoError(t, err)
		require.NoError(t, batch.Append(uint8(1), "owned-first", "same"))
		require.NoError(t, batch.Append(
			uint8(2),
			clickhouse.BorrowBytes([]byte("borrowed-middle")),
			clickhouse.BorrowBytes([]byte("same")),
		))
		require.NoError(t, batch.Append(uint8(3), "owned-last", "same"))
		require.NoError(t, batch.Send())

		columnar, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
		require.NoError(t, err)
		require.NoError(t, columnar.Column(0).Append([]uint8{4, 5}))
		require.NoError(t, columnar.Column(1).Append(clickhouse.BorrowBytesColumn([][]byte{
			[]byte("column-first"),
			[]byte("column-second"),
		})))
		require.NoError(t, columnar.Column(2).Append(clickhouse.BorrowBytesColumn([][]byte{
			[]byte("column-low-cardinality"),
			[]byte("column-low-cardinality"),
		})))
		require.NoError(t, columnar.Send())

		rows, err := conn.Query(ctx, "SELECT sequence, value, low_cardinality FROM "+table+" ORDER BY sequence")
		require.NoError(t, err)
		defer rows.Close()
		expected := []struct {
			sequence       uint8
			value          string
			lowCardinality string
		}{
			{1, "owned-first", "same"},
			{2, "borrowed-middle", "same"},
			{3, "owned-last", "same"},
			{4, "column-first", "column-low-cardinality"},
			{5, "column-second", "column-low-cardinality"},
		}
		for _, want := range expected {
			require.True(t, rows.Next())
			var sequence uint8
			var value string
			var lowCardinality string
			require.NoError(t, rows.Scan(&sequence, &value, &lowCardinality))
			require.Equal(t, want.sequence, sequence)
			require.Equal(t, want.value, value)
			require.Equal(t, want.lowCardinality, lowCardinality)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})
}

func TestBorrowedStringUncompressedNative(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.Native, nil, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	const table = "test_borrowed_string_uncompressed"
	require.NoError(t, conn.Exec(ctx, "CREATE TABLE "+table+" (value String) Engine MergeTree() ORDER BY tuple()"))
	t.Cleanup(func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table))
	})

	payload := makeBorrowedRandomPayload(4 << 20)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
	require.NoError(t, err)
	require.NoError(t, batch.Append(clickhouse.BorrowBytes(payload)))
	require.NoError(t, batch.Send())

	var actual []byte
	require.NoError(t, conn.QueryRow(ctx, "SELECT value FROM "+table).Scan(&actual))
	require.True(t, bytes.Equal(payload, actual))
}

func TestBorrowedStringNativeFlushLifetime(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.Native, nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	ctx := context.Background()
	const table = "test_borrowed_string_native_flush_lifetime"
	require.NoError(t, conn.Exec(ctx, "CREATE TABLE "+table+" (sequence UInt8, value String) Engine MergeTree() ORDER BY sequence"))
	t.Cleanup(func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table))
	})

	payload := []byte("first")
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
	require.NoError(t, err)
	require.NoError(t, batch.Append(uint8(1), clickhouse.BorrowBytes(payload)))
	require.NoError(t, batch.Flush())
	payload[0] = 'X'
	require.NoError(t, batch.Append(uint8(2), clickhouse.BorrowBytes([]byte("second"))))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT value FROM "+table+" ORDER BY sequence")
	require.NoError(t, err)
	defer rows.Close()
	for _, want := range []string{"first", "second"} {
		require.True(t, rows.Next())
		var value string
		require.NoError(t, rows.Scan(&value))
		require.Equal(t, want, value)
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func makeBorrowedRandomPayload(size int) []byte {
	payload := make([]byte, size)
	state := uint64(0x9e3779b97f4a7c15)
	for i := range payload {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		payload[i] = byte(state)
	}
	return payload
}

func (t testStr) String() string {
	return t.Col1
}

func TestSimpleString(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()

		require.NoError(t, err)
		require.NoError(t, conn.Ping(ctx))
		if !CheckMinServerServerVersion(conn, 21, 9, 0) {
			t.Skip(fmt.Errorf("unsupported clickhouse version"))
			return
		}
		const ddl = `
		CREATE TABLE test_string (
			  	  Col1 String
		        , Col2 String
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_string")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_string")
		require.NoError(t, err)
		require.NoError(t, batch.Append("A", &testStr{"B"}))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())
	})
}

type customStr string

func (s *customStr) Scan(src any) error {
	if t, ok := src.(string); ok {
		*s = customStr(t)
		return nil
	}
	return fmt.Errorf("cannot scan %T into customStr", src)
}

func (s customStr) String() string {
	return string(s)
}

func TestCustomString(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()

		require.NoError(t, err)
		require.NoError(t, conn.Ping(ctx))
		if !CheckMinServerServerVersion(conn, 21, 9, 0) {
			t.Skip(fmt.Errorf("unsupported clickhouse version"))
			return
		}
		const ddl = `
		CREATE TABLE test_string (
			  	  Col1 String
		        , Col2 String
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_string")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_string")
		require.NoError(t, err)

		type data struct {
			Col1 string    `ch:"Col1"`
			Col2 customStr `ch:"Col2"`
		}
		require.NoError(t, batch.AppendStruct(&data{
			Col1: "A",
			Col2: "B",
		}))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())

		var dest data
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_string").ScanStruct(&dest))
		assert.Equal(t, "A", dest.Col1)
		assert.Equal(t, customStr("B"), dest.Col2)
	})
}

func TestString(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		if !CheckMinServerServerVersion(conn, 21, 9, 0) {
			t.Skip(fmt.Errorf("unsupported clickhouse version"))
			return
		}
		const ddl = `
		CREATE TABLE test_string (
			  Col1 String
			, Col2 Array(String)
			, Col3 Nullable(String)
			, Col4 String
			, Col5 Nullable(String)
      		, Col6 String
		    , Col7 String
		    , Col8 Nullable(String)
		    , Col9 String
		    , Col10 Nullable(String)
			, Col11 Nullable(String)
		) Engine MergeTree() ORDER BY tuple()
	`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_string")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_string")
		require.NoError(t, err)
		col6Data := "D"
		col7Data := time.Now()
		col8Data := &time.Time{}
		col9Data := &testStr{"E"}
		var col10Data testStr
		col11Data := "G"
		require.NoError(t, batch.Append(
			"A",
			[]string{"A", "B", "C"},
			nil,
			sql.NullString{String: "D", Valid: true},
			sql.NullString{Valid: false},
			[]byte(col6Data),
			col7Data,
			col8Data,
			col9Data,
			&col10Data,
			&col11Data,
		))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())
		var (
			col1  string
			col2  []string
			col3  *string
			col4  sql.NullString
			col5  sql.NullString
			col6  string
			col7  string
			col8  string
			col9  string
			col10 string
			col11 string
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_string").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9, &col10, &col11))
		require.Nil(t, col3)
		assert.Equal(t, "A", col1)
		assert.Equal(t, []string{"A", "B", "C"}, col2)
		assert.Equal(t, sql.NullString{String: "D", Valid: true}, col4)
		assert.Equal(t, sql.NullString{Valid: false}, col5)
		assert.Equal(t, col6Data, col6)
		assert.Equal(t, col7, col7Data.String())
		assert.Equal(t, col8, col8Data.String())
		assert.Equal(t, col9, col9Data.String())
		assert.Equal(t, col10, col10Data.String())
		assert.Equal(t, "G", col11)
	})
}

func BenchmarkString(b *testing.B) {
	conn, err := GetNativeConnectionTCP(nil, nil, nil)
	ctx := context.Background()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		conn.Exec(ctx, "DROP TABLE benchmark_string")
	}()

	if err = conn.Exec(ctx, `CREATE TABLE benchmark_string (Col1 UInt64, Col2 String) ENGINE = Null`); err != nil {
		b.Fatal(err)
	}

	const rowsInBlock = 10_000_000

	for n := 0; n < b.N; n++ {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO benchmark_string VALUES")
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < rowsInBlock; i++ {
			if err := batch.Append(uint64(1), "test"); err != nil {
				b.Fatal(err)
			}
		}
		if err = batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColumnarString(b *testing.B) {
	conn, err := GetNativeConnectionTCP(nil, nil, nil)
	ctx := context.Background()
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		conn.Exec(ctx, "DROP TABLE benchmark_string")
	}()
	if err = conn.Exec(ctx, `CREATE TABLE benchmark_string (Col1 UInt64, Col2 String) ENGINE = Null`); err != nil {
		b.Fatal(err)
	}

	const rowsInBlock = 10_000_000

	var (
		col1 []uint64
		col2 []string
	)
	for n := 0; n < b.N; n++ {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO benchmark_string VALUES")
		if err != nil {
			b.Fatal(err)
		}
		col1 = col1[:0]
		col2 = col2[:0]
		for i := 0; i < rowsInBlock; i++ {
			col1 = append(col1, uint64(1))
			col2 = append(col2, "test")
		}
		if err := batch.Column(0).Append(col1); err != nil {
			b.Fatal(err)
		}
		if err := batch.Column(1).Append(col2); err != nil {
			b.Fatal(err)
		}
		if err = batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestStringFlush(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		SkipOnHTTP(t, protocol, "Flush")
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		defer func() {
			conn.Exec(ctx, "DROP TABLE string_flush")
		}()
		const ddl = `
		CREATE TABLE string_flush (
			  Col1 FixedString(10)
		) Engine MergeTree() ORDER BY tuple()
		`
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO string_flush")
		require.NoError(t, err)
		vals := [1000]string{}
		for i := 0; i < 1000; i++ {
			vals[i] = RandAsciiString(10)
			batch.Append(vals[i])
			require.Equal(t, 1, batch.Rows())
			batch.Flush()
		}
		require.Equal(t, 0, batch.Rows())
		batch.Send()
		rows, err := conn.Query(ctx, "SELECT * FROM string_flush")
		require.NoError(t, err)
		i := 0
		for rows.Next() {
			var col1 string
			require.NoError(t, rows.Scan(&col1))
			require.Equal(t, vals[i], col1)
			i += 1
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
		require.Equal(t, 1000, i)
	})
}

type testStringSerializer struct {
	val string
}

func (c testStringSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testStringSerializer) Scan(src any) error {
	if t, ok := src.(string); ok {
		*c = testStringSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testStringSerializer", src)
}

func TestStringFromDriverValuerType(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()

		require.NoError(t, err)
		require.NoError(t, conn.Ping(ctx))
		if !CheckMinServerServerVersion(conn, 21, 9, 0) {
			t.Skip(fmt.Errorf("unsupported clickhouse version"))
			return
		}
		const ddl = `
		CREATE TABLE test_string (
			  	  Col1 String
		        , Col2 String
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_string")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_string")
		require.NoError(t, err)

		type data struct {
			Col1 string               `ch:"Col1"`
			Col2 testStringSerializer `ch:"Col2"`
		}
		require.NoError(t, batch.AppendStruct(&data{
			Col1: "Value",
			Col2: testStringSerializer{"Value"},
		}))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())

		var dest data
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_string").ScanStruct(&dest))
		assert.Equal(t, "Value", dest.Col1)
		assert.Equal(t, testStringSerializer{"Value"}, dest.Col2)
	})
}

type testStringPtrSerializer struct {
	val string
}

func (c testStringPtrSerializer) Value() (driver.Value, error) {
	return &c.val, nil
}

func (c *testStringPtrSerializer) Scan(src any) error {
	if t, ok := src.(string); ok {
		*c = testStringPtrSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testStringPtrSerializer", src)
}

func TestStringFromDriverValuerTypeNonStdReturn(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()

		require.NoError(t, err)
		require.NoError(t, conn.Ping(ctx))
		if !CheckMinServerServerVersion(conn, 21, 9, 0) {
			t.Skip(fmt.Errorf("unsupported clickhouse version"))
			return
		}
		const ddl = `
		CREATE TABLE test_string (
			  	  Col1 String
		        , Col2 String
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_string")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_string")
		require.NoError(t, err)

		type data struct {
			Col1 string                  `ch:"Col1"`
			Col2 testStringPtrSerializer `ch:"Col2"`
		}
		s := "Value"
		require.NoError(t, batch.AppendStruct(&data{
			Col1: s,
			Col2: testStringPtrSerializer{s},
		}))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())

		var dest data
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_string").ScanStruct(&dest))
		assert.Equal(t, "Value", dest.Col1)
		assert.Equal(t, testStringPtrSerializer{s}, dest.Col2)
	})
}
