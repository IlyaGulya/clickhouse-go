package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func BenchmarkBorrowedStringInsert(b *testing.B) {
	columns := []struct {
		name       string
		columnType string
		rows       int
		unique     int
		rowSize    int
	}{
		{name: "String", columnType: "String", rows: 512, unique: 512, rowSize: 8 << 10},
		{name: "LowCardinality", columnType: "LowCardinality(String)", rows: 4096, unique: 64, rowSize: 8 << 10},
	}
	transports := []struct {
		name                 string
		protocol             clickhouse.Protocol
		compression          *clickhouse.Compression
		maxCompressionBuffer int
	}{
		{name: "NativePlain", protocol: clickhouse.Native},
		{name: "NativeLZ4", protocol: clickhouse.Native, compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4}},
		{name: "NativeLZ4Buffer1MiB", protocol: clickhouse.Native, compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4}, maxCompressionBuffer: 1 << 20},
		{name: "HTTPLZ4", protocol: clickhouse.HTTP, compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4}},
	}
	for _, columnSpec := range columns {
		for _, random := range []bool{false, true} {
			payloadName := "Compressible"
			if random {
				payloadName = "Random"
			}
			byteValues, stringValues := makeInsertBenchmarkValues(columnSpec.unique, columnSpec.rowSize, random)
			rowStrings := make([]string, columnSpec.rows)
			rowBytes := make([][]byte, columnSpec.rows)
			for row := range columnSpec.rows {
				index := row % columnSpec.unique
				rowStrings[row] = stringValues[index]
				rowBytes[row] = byteValues[index]
			}

			for _, transport := range transports {
				for _, input := range []string{"OwnedRow", "BorrowedRow", "OwnedColumn", "BorrowedColumn"} {
					name := strings.Join([]string{columnSpec.name, payloadName, transport.name, input}, "/")
					b.Run(name, func(b *testing.B) {
						benchmarkBorrowedInsertCase(b, columnSpec.columnType, columnSpec.rows, columnSpec.rowSize, rowStrings, rowBytes, transport.protocol, transport.compression, transport.maxCompressionBuffer, input)
					})
				}
			}
		}
	}
}

func benchmarkBorrowedInsertCase(
	b *testing.B,
	columnType string,
	rows int,
	rowSize int,
	stringValues []string,
	byteValues [][]byte,
	protocol clickhouse.Protocol,
	compression *clickhouse.Compression,
	maxCompressionBuffer int,
	input string,
) {
	b.Helper()
	var conn driver.Conn
	var err error
	if protocol == clickhouse.Native && maxCompressionBuffer > 0 {
		conn, err = GetConnectionTCPWithOptions(testSet, nil, nil, compression, func(options *clickhouse.Options) {
			options.MaxCompressionBuffer = maxCompressionBuffer
		})
	} else {
		conn, err = GetConnection(testSet, nil, protocol, nil, nil, compression)
	}
	if err != nil {
		b.Fatal(err)
	}
	table := fmt.Sprintf("benchmark_borrowed_%s", strings.ToLower(strings.TrimSuffix(strings.TrimPrefix(columnType, "LowCardinality("), ")")))
	ctx := context.Background()
	if err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+table+" (value "+columnType+") Engine Null"); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			b.Errorf("drop benchmark table: %v", err)
		}
		if err := conn.Close(); err != nil {
			b.Errorf("close benchmark connection: %v", err)
		}
	})

	b.ReportAllocs()
	b.SetBytes(int64(rows * rowSize))
	b.ReportMetric(float64(rows), "rows/op")
	b.ResetTimer()
	for range b.N {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
		if err != nil {
			b.Fatal(err)
		}
		if err := appendInsertBenchmarkValues(batch, stringValues, byteValues, input); err != nil {
			b.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}

func appendInsertBenchmarkValues(batch driver.Batch, stringValues []string, byteValues [][]byte, input string) error {
	switch input {
	case "OwnedRow":
		for i := range stringValues {
			if err := batch.Append(stringValues[i]); err != nil {
				return err
			}
		}
	case "BorrowedRow":
		for i := range byteValues {
			if err := batch.Append(clickhouse.BorrowBytes(byteValues[i])); err != nil {
				return err
			}
		}
	case "OwnedColumn":
		return batch.Column(0).Append(stringValues)
	case "BorrowedColumn":
		return batch.Column(0).Append(clickhouse.BorrowBytesColumn(byteValues))
	default:
		return fmt.Errorf("unknown benchmark input %q", input)
	}
	return nil
}

func makeInsertBenchmarkValues(count, size int, random bool) ([][]byte, []string) {
	byteValues := make([][]byte, count)
	stringValues := make([]string, count)
	for i := range count {
		value := make([]byte, size)
		state := uint64(i+1) * 0x9e3779b97f4a7c15
		for j := range value {
			if random {
				state ^= state << 13
				state ^= state >> 7
				state ^= state << 17
				value[j] = byte(state)
			} else {
				value[j] = byte('a' + (j/128+i)%26)
			}
		}
		byteValues[i] = value
		stringValues[i] = string(value)
	}
	return byteValues, stringValues
}
