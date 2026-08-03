package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/compress"
	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

var errStreamingDestinationClosed = errors.New("streaming destination closed")

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

func TestHTTPWriteDataToPropagatesDestinationFailure(t *testing.T) {
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("value", column.Type("String")))
	require.NoError(t, block.Append(column.BorrowedBytes(bytes.Repeat([]byte("payload"), 1024))))

	for _, method := range []CompressionMethod{CompressionNone, CompressionLZ4, CompressionZSTD} {
		t.Run(method.String(), func(t *testing.T) {
			conn := httpConnect{
				compression:     method,
				blockCompressor: compress.NewWriter(compress.LevelZero, compress.Method(method)),
			}
			destination := &failAfterWriter{remaining: 1, err: errStreamingDestinationClosed}
			err := conn.writeDataTo(destination, block)
			require.ErrorIs(t, err, errStreamingDestinationClosed)
		})
	}
}

func TestHTTPBatchCancellationWaitsForBorrowedProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producerStarted := make(chan struct{})
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var firstByte [1]byte
		_, err := req.Body.Read(firstByte[:])
		if err != nil {
			return nil, err
		}
		close(producerStarted)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	compressionPool, err := createCompressionPool(&Compression{Method: CompressionNone})
	require.NoError(t, err)
	endpoint, err := url.Parse("http://clickhouse.invalid/")
	require.NoError(t, err)
	conn := &httpConnect{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		opt:             &Options{},
		url:             endpoint,
		client:          &http.Client{Transport: transport},
		compression:     CompressionNone,
		compressionPool: compressionPool,
	}
	payload := bytes.Repeat([]byte("borrowed payload"), 1<<18)
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("value", column.Type("String")))
	require.NoError(t, block.Append(column.BorrowedBytes(payload)))
	batch := &httpBatch{
		ctx:         ctx,
		conn:        conn,
		connRelease: func(nativeTransport, error) {},
		block:       block,
		query:       "INSERT INTO test VALUES",
	}

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- batch.Send()
	}()
	select {
	case <-producerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP producer did not start")
	}
	cancel()
	select {
	case err := <-sendDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP Send did not stop after cancellation")
	}

	// Send must not return while the producer can still read caller-owned data.
	// The race detector finds a regression in the producer lifetime.
	for i := range payload {
		payload[i]++
	}
}

type failAfterWriter struct {
	remaining int
	err       error
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if w.remaining == 0 {
		return 0, w.err
	}
	if len(p) > w.remaining {
		written := w.remaining
		w.remaining = 0
		return written, w.err
	}
	w.remaining -= len(p)
	return len(p), nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
