package clickhouse

import (
	"io"
	"testing"

	"github.com/ClickHouse/ch-go/compress"
	chproto "github.com/ClickHouse/ch-go/proto"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

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
