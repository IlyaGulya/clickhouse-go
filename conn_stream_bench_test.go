package clickhouse

import (
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/ClickHouse/ch-go/compress"
	chproto "github.com/ClickHouse/ch-go/proto"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func BenchmarkBorrowedInsertEncodingMatrix(b *testing.B) {
	cases := []struct {
		name       string
		columnType column.Type
		rows       int
		unique     int
		rowSize    int
	}{
		{name: "String", columnType: "String", rows: 512, unique: 512, rowSize: 8 << 10},
		{name: "LowCardinality", columnType: "LowCardinality(String)", rows: 4096, unique: 64, rowSize: 8 << 10},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for _, random := range []bool{false, true} {
				payloadName := "Compressible"
				if random {
					payloadName = "Random"
				}
				values := makeBenchmarkStringValues(tc.unique, tc.rowSize, random)
				b.Run(payloadName, func(b *testing.B) {
					for _, borrowed := range []bool{false, true} {
						inputName := "Owned"
						if borrowed {
							inputName = "Borrowed"
						}
						b.Run(inputName, func(b *testing.B) {
							for _, encoder := range []string{"Contiguous", "NativeStreaming", "HTTPStreaming"} {
								b.Run(encoder, func(b *testing.B) {
									benchmarkInsertEncoding(b, tc.columnType, tc.rows, tc.rowSize, values, borrowed, encoder)
								})
							}
						})
					}
				})
			}
		})
	}
}

type benchmarkStringValues struct {
	bytes   [][]byte
	strings []string
}

func makeBenchmarkStringValues(count, size int, random bool) benchmarkStringValues {
	values := benchmarkStringValues{
		bytes:   make([][]byte, count),
		strings: make([]string, count),
	}
	for i := range count {
		value := make([]byte, size)
		if random {
			state := uint64(i+1) * 0x9e3779b97f4a7c15
			for j := range value {
				state ^= state << 13
				state ^= state >> 7
				state ^= state << 17
				value[j] = byte(state)
			}
		} else {
			for j := range value {
				value[j] = byte('a' + (j/128+i)%26)
			}
		}
		if len(value) >= 8 {
			binary.LittleEndian.PutUint64(value, uint64(i))
		}
		values.bytes[i] = value
		values.strings[i] = string(value)
	}
	return values
}

func benchmarkInsertEncoding(
	b *testing.B,
	columnType column.Type,
	rows int,
	rowSize int,
	values benchmarkStringValues,
	borrowed bool,
	encoder string,
) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(rows * rowSize))
	var totalRetained, totalWire int64
	b.ResetTimer()
	for range b.N {
		block := proto.NewBlock()
		if err := block.AddColumn("payload", columnType); err != nil {
			b.Fatal(err)
		}
		for row := range rows {
			index := row % len(values.bytes)
			var value any = values.strings[index]
			if borrowed {
				value = column.BorrowedBytes(values.bytes[index])
			}
			if err := block.Append(value); err != nil {
				b.Fatal(err)
			}
		}

		retained, wire, err := encodeBenchmarkBlock(block, encoder)
		if err != nil {
			b.Fatal(err)
		}
		totalRetained += retained
		totalWire += wire
	}
	b.ReportMetric(float64(totalRetained)/float64(b.N), "retained-B/op")
	b.ReportMetric(float64(totalWire)/float64(b.N), "wire-B/op")
}

func encodeBenchmarkBlock(block *proto.Block, encoder string) (retained int64, wire int64, err error) {
	switch encoder {
	case "Contiguous":
		buffer := new(chproto.Buffer)
		compressor := compress.NewWriter(compress.LevelZero, compress.LZ4)
		if err := block.Encode(buffer, ClientTCPProtocolVersion); err != nil {
			return 0, 0, err
		}
		if err := compressor.Compress(buffer.Buf); err != nil {
			return 0, 0, err
		}
		buffer.Buf = append(buffer.Buf[:0], compressor.Data...)
		return int64(cap(buffer.Buf)), int64(len(buffer.Buf)), nil
	case "NativeStreaming":
		conn := &connect{
			buffer:               new(chproto.Buffer),
			compressor:           compress.NewWriter(compress.LevelZero, compress.LZ4),
			compression:          CompressionLZ4,
			revision:             ClientTCPProtocolVersion,
			maxCompressionBuffer: int(^uint(0) >> 1),
		}
		if err := conn.writeCompressedBlock(block); err != nil {
			return 0, 0, err
		}
		return int64(cap(conn.buffer.Buf)), int64(len(conn.buffer.Buf)), nil
	case "HTTPStreaming":
		conn := &httpConnect{
			compression:     CompressionLZ4,
			blockCompressor: compress.NewWriter(compress.LevelZero, compress.LZ4),
		}
		counter := new(byteCountingWriter)
		if err := conn.writeDataTo(counter, block); err != nil {
			return 0, 0, err
		}
		return 0, int64(*counter), nil
	default:
		return 0, 0, fmt.Errorf("unknown benchmark encoder %q", encoder)
	}
}

type byteCountingWriter int64

func (w *byteCountingWriter) Write(p []byte) (int, error) {
	*w += byteCountingWriter(len(p))
	return len(p), nil
}

func BenchmarkCompressedBorrowedStringBlock(b *testing.B) {
	const (
		rows    = 512
		rowSize = 8 << 10
	)

	values := make([]column.BorrowedBytes, rows)
	block := proto.NewBlock()
	if err := block.AddColumn("payload", column.Type("String")); err != nil {
		b.Fatal(err)
	}
	for i := range values {
		values[i] = make([]byte, rowSize)
		for j := range values[i] {
			values[i][j] = byte('a' + (j/128+i)%26)
		}
		if err := block.Append(values[i]); err != nil {
			b.Fatal(err)
		}
	}

	benchmarkCompressedBorrowedBlock(b, block, rows*rowSize)
}

func BenchmarkCompressedBorrowedLowCardinalityStringBlock(b *testing.B) {
	const (
		rows         = 4096
		uniqueValues = 64
		rowSize      = 8 << 10
	)

	values := make([]column.BorrowedBytes, uniqueValues)
	for i := range values {
		values[i] = make([]byte, rowSize)
		for j := range values[i] {
			values[i][j] = byte('a' + (j/128+i)%26)
		}
	}
	block := proto.NewBlock()
	if err := block.AddColumn("payload", column.Type("LowCardinality(String)")); err != nil {
		b.Fatal(err)
	}
	for i := range rows {
		if err := block.Append(values[i%len(values)]); err != nil {
			b.Fatal(err)
		}
	}

	benchmarkCompressedBorrowedBlock(b, block, rows*rowSize)
}

func benchmarkCompressedBorrowedBlock(b *testing.B, block *proto.Block, logicalBytes int) {
	b.Helper()
	b.SetBytes(int64(logicalBytes))
	b.Run("Contiguous", func(b *testing.B) {
		var totalCapacity, totalWireBytes int64
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			buffer := new(chproto.Buffer)
			compressor := compress.NewWriter(compress.LevelZero, compress.LZ4)
			if err := block.Encode(buffer, ClientTCPProtocolVersion); err != nil {
				b.Fatal(err)
			}
			if err := compressor.Compress(buffer.Buf); err != nil {
				b.Fatal(err)
			}
			buffer.Buf = append(buffer.Buf[:0], compressor.Data...)
			totalCapacity += int64(cap(buffer.Buf))
			totalWireBytes += int64(len(buffer.Buf))
		}
		b.ReportMetric(float64(totalCapacity)/float64(b.N), "retained-B/op")
		b.ReportMetric(float64(totalWireBytes)/float64(b.N), "wire-B/op")
	})

	b.Run("NativeStreaming", func(b *testing.B) {
		var totalCapacity, totalWireBytes int64
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			conn := &connect{
				buffer:               new(chproto.Buffer),
				compressor:           compress.NewWriter(compress.LevelZero, compress.LZ4),
				compression:          CompressionLZ4,
				revision:             ClientTCPProtocolVersion,
				maxCompressionBuffer: int(^uint(0) >> 1),
			}
			if err := conn.writeCompressedBlock(block); err != nil {
				b.Fatal(err)
			}
			totalCapacity += int64(cap(conn.buffer.Buf))
			totalWireBytes += int64(len(conn.buffer.Buf))
		}
		b.ReportMetric(float64(totalCapacity)/float64(b.N), "retained-B/op")
		b.ReportMetric(float64(totalWireBytes)/float64(b.N), "wire-B/op")
	})

	b.Run("HTTPStreaming", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			conn := &httpConnect{
				compression:     CompressionLZ4,
				blockCompressor: compress.NewWriter(compress.LevelZero, compress.LZ4),
			}
			if err := conn.writeDataTo(io.Discard, block); err != nil {
				b.Fatal(err)
			}
		}
	})
}
