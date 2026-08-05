package clickhouse

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ClickHouse/ch-go/compress"
	chproto "github.com/ClickHouse/ch-go/proto"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func BenchmarkBorrowedStringAppendAPI(b *testing.B) {
	const (
		rows    = 4096
		rowSize = 1 << 10
	)
	values := makeBenchmarkStringValues(rows, rowSize, true)
	borrowedValues := make([]column.BorrowedBytes, rows)
	for i := range borrowedValues {
		borrowedValues[i] = values.bytes[i]
	}

	for _, borrowed := range []bool{false, true} {
		inputName := "Owned"
		if borrowed {
			inputName = "Borrowed"
		}
		b.Run(inputName, func(b *testing.B) {
			for _, columnar := range []bool{false, true} {
				apiName := "Row"
				if columnar {
					apiName = "Column"
				}
				b.Run(apiName, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(rows * rowSize)
					for range b.N {
						block := proto.NewBlock()
						if err := block.AddColumn("payload", column.Type("String")); err != nil {
							b.Fatal(err)
						}
						if columnar {
							var input any = values.strings
							if borrowed {
								input = column.BorrowedBytesColumn(values.bytes)
							}
							if _, err := block.Columns[0].Append(input); err != nil {
								b.Fatal(err)
							}
							continue
						}
						for row := range rows {
							var input any = values.strings[row]
							if borrowed {
								input = borrowedValues[row]
							}
							if err := block.Append(input); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		})
	}
}

func BenchmarkBorrowedInsertEncoding(b *testing.B) {
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
