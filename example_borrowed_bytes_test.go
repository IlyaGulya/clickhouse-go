package clickhouse_test

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func ExampleBorrowBytes() {
	payload := []byte("large payload")
	value := clickhouse.BorrowBytes(payload)

	// Append value to a String, Nullable(String), Array(String), or
	// LowCardinality(String) batch column. Keep payload immutable until
	// batch.Send returns, or until batch.Abort or batch.Close returns when the
	// batch is not sent.
	fmt.Println(string(value))

	// Output: large payload
}
