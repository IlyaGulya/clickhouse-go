package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/ClickHouse/ch-go/compress"
	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestWriteUncompressedBlockMatchesEncode(t *testing.T) {
	transport := new(recordingNetConn)
	conn := &connect{
		conn:     transport,
		buffer:   new(chproto.Buffer),
		revision: ClientTCPProtocolVersion,
	}
	conn.buffer.PutRaw([]byte("prefix"))
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("value", column.Type("String")))
	require.NoError(t, block.Append(column.BorrowedBytes("first")))
	require.NoError(t, block.Append(column.BorrowedBytes("second")))

	var expected chproto.Buffer
	expected.PutRaw([]byte("prefix"))
	require.NoError(t, block.Encode(&expected, ClientTCPProtocolVersion))
	require.NoError(t, conn.writeUncompressedBlock(block))

	require.Equal(t, expected.Buf, transport.data)
	require.Empty(t, conn.buffer.Buf)
}

func TestWriteUncompressedBlockPropagatesShortWrite(t *testing.T) {
	transport := &recordingNetConn{shortWrite: true}
	conn := &connect{
		conn:     transport,
		buffer:   new(chproto.Buffer),
		revision: ClientTCPProtocolVersion,
	}
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("value", column.Type("String")))
	require.NoError(t, block.Append(column.BorrowedBytes("payload")))

	err := conn.writeUncompressedBlock(block)
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrShortWrite)
}

func TestSendDataClosesConnectionAfterShortWrite(t *testing.T) {
	transport := &recordingNetConn{shortWrite: true}
	conn := newShortWriteConnect(transport)
	block := newStringBlock(t)

	err := conn.sendData(block, "")
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.True(t, conn.isClosed())
	require.Equal(t, 1, transport.closeCalls)
}

func TestBatchFlushReleasesConnectionAfterShortWrite(t *testing.T) {
	transport := &recordingNetConn{shortWrite: true}
	conn := newShortWriteConnect(transport)
	block := newStringBlock(t)
	reacquireErr := errors.New("get new connection")
	var releaseErr error
	batch := &batch{
		ctx:   context.Background(),
		conn:  conn,
		block: block,
		connRelease: func(_ *connect, err error) {
			releaseErr = err
		},
		connAcquire: func(context.Context) (*connect, error) {
			return nil, reacquireErr
		},
	}

	err := batch.Flush()
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.ErrorIs(t, releaseErr, io.ErrShortWrite)
	require.True(t, batch.released)
	writes := len(transport.writeSizes)

	err = batch.Flush()
	require.ErrorIs(t, err, reacquireErr)
	require.Len(t, transport.writeSizes, writes)
}

func TestBatchCloseFinalizesAfterShortWrite(t *testing.T) {
	transport := &recordingNetConn{shortWrite: true}
	conn := newShortWriteConnect(transport)
	payload := []byte("payload")
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("value", column.Type("String")))
	require.NoError(t, block.Append(column.BorrowedBytes(payload)))
	acquireCalls := 0
	batch := &batch{
		ctx:   context.Background(),
		conn:  conn,
		block: block,
		connRelease: func(*connect, error) {
		},
		connAcquire: func(context.Context) (*connect, error) {
			acquireCalls++
			return nil, errors.New("unexpected connection request")
		},
	}

	require.ErrorIs(t, batch.Flush(), io.ErrShortWrite)
	require.NoError(t, batch.Close())
	require.True(t, batch.IsSent())
	require.Zero(t, block.Rows())
	require.ErrorIs(t, batch.Send(), ErrBatchAlreadySent)
	require.Zero(t, acquireCalls)
}

func TestBatchAbortFinalizesBorrowedBlock(t *testing.T) {
	payload := []byte("payload")
	batch, acquireCalls := newBorrowedLifecycleBatch(t, payload)

	require.NoError(t, batch.Abort())
	payload[0] = 'X'
	require.True(t, batch.IsSent())
	require.Zero(t, batch.block.Rows())
	require.ErrorIs(t, batch.Send(), ErrBatchAlreadySent)
	require.Zero(t, *acquireCalls)
}

func TestBatchCloseAfterSendPreventsResend(t *testing.T) {
	payload := []byte("payload")
	batch, acquireCalls := newBorrowedLifecycleBatch(t, payload)
	batch.sent = true
	batch.released = true

	require.NoError(t, batch.Close())
	payload[0] = 'X'
	require.True(t, batch.IsSent())
	require.Zero(t, batch.block.Rows())
	require.ErrorIs(t, batch.Send(), ErrBatchAlreadySent)
	require.Zero(t, *acquireCalls)
}

func newBorrowedLifecycleBatch(t *testing.T, payload []byte) (*batch, *int) {
	t.Helper()
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("value", column.Type("String")))
	require.NoError(t, block.Append(column.BorrowedBytes(payload)))
	acquireCalls := 0
	return &batch{
		ctx:   context.Background(),
		conn:  newShortWriteConnect(new(recordingNetConn)),
		block: block,
		connRelease: func(*connect, error) {
		},
		connAcquire: func(context.Context) (*connect, error) {
			acquireCalls++
			return nil, errors.New("unexpected connection request")
		},
	}, &acquireCalls
}

func newShortWriteConnect(transport *recordingNetConn) *connect {
	return &connect{
		conn:        transport,
		buffer:      new(chproto.Buffer),
		revision:    ClientTCPProtocolVersion,
		compression: CompressionNone,
		logger:      newNoopLogger(),
		opt:         &Options{},
	}
}

func newStringBlock(t *testing.T) *proto.Block {
	t.Helper()
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("value", column.Type("String")))
	require.NoError(t, block.Append("payload"))
	return block
}

func TestCompressedBlockSinkHonorsBufferLimit(t *testing.T) {
	transport := new(recordingNetConn)
	conn := &connect{
		conn:                 transport,
		buffer:               new(chproto.Buffer),
		maxCompressionBuffer: 8,
	}
	conn.buffer.PutRaw([]byte("header"))
	sink := compressedBlockSink{connect: conn}

	written, err := sink.Write(bytes.Repeat([]byte{'x'}, 10))
	require.NoError(t, err)
	require.Equal(t, 10, written)
	require.Empty(t, conn.buffer.Buf)
	require.Equal(t, []int{6, 10}, transport.writeSizes)
	require.Equal(t, "headerxxxxxxxxxx", string(transport.data))
	require.LessOrEqual(t, cap(conn.buffer.Buf), 8)

	written, err = sink.Write([]byte("small"))
	require.NoError(t, err)
	require.Equal(t, 5, written)
	require.Equal(t, "small", string(conn.buffer.Buf))

	written, err = sink.Write([]byte("more"))
	require.NoError(t, err)
	require.Equal(t, 4, written)
	require.Equal(t, "more", string(conn.buffer.Buf))
	require.Equal(t, []int{6, 10, 5}, transport.writeSizes)
}

func TestCompressedBlockSinkPropagatesDirectShortWrite(t *testing.T) {
	transport := &recordingNetConn{shortWrite: true}
	conn := &connect{
		conn:                 transport,
		buffer:               new(chproto.Buffer),
		maxCompressionBuffer: 4,
	}

	written, err := (compressedBlockSink{connect: conn}).Write([]byte("large"))
	require.Equal(t, 4, written)
	require.ErrorIs(t, err, io.ErrShortWrite)
}

func TestWriteCompressedBlockPropagatesConnectionFailure(t *testing.T) {
	writeErr := errors.New("connection closed during block write")
	conn := createMockConnect(&mockNetConn{writeErr: writeErr})
	conn.compressor = compress.NewWriter(compress.LevelZero, compress.LZ4)
	conn.maxCompressionBuffer = 1
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("value", column.Type("String")))
	require.NoError(t, block.Append(column.BorrowedBytes("borrowed payload")))

	err := conn.writeCompressedBlock(block)
	require.ErrorIs(t, err, writeErr)
}

type recordingNetConn struct {
	mockNetConn
	writeSizes []int
	data       []byte
	shortWrite bool
	closeCalls int
}

func (c *recordingNetConn) Close() error {
	c.closeCalls++
	return c.mockNetConn.Close()
}

func (c *recordingNetConn) Write(p []byte) (int, error) {
	c.writeSizes = append(c.writeSizes, len(p))
	written := len(p)
	if c.shortWrite {
		written--
		c.data = append(c.data, p[:written]...)
		return written, io.ErrShortWrite
	}
	c.data = append(c.data, p[:written]...)
	return written, nil
}
