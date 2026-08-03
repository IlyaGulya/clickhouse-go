package clickhouse_test

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func ExampleBorrowBytes() {
	payload := []byte("large payload")
	value := clickhouse.BorrowBytes(payload)

	// Add a value to a String, Nullable(String), Array(String), or
	// LowCardinality(String) batch column. Do not change payload until
	// batch.Send returns. If you do not send the batch, do not change payload
	// until batch.Abort or batch.Close returns. You can send a Native batch more
	// than once. Each Send reads the current data in payload.
	fmt.Println(string(value))

	// Output: large payload
}

func ExampleBorrowBytesColumn() {
	values := [][]byte{[]byte("first"), []byte("second")}
	column := clickhouse.BorrowBytesColumn(values)

	// Add a column to a prepared batch without an []BorrowedBytes allocation:
	// err := batch.Column(0).Append(column)
	fmt.Println(string(column[0]), string(column[1]))

	// Output: first second
}
