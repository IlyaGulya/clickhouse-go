package column

import (
	"database/sql"
	"database/sql/driver"
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type String struct {
	name           string
	col            proto.ColStr
	borrowed       proto.BorrowedColStr
	inputMode      stringInputMode
	pendingEmpties int
}

type stringInputMode uint8

const (
	stringInputUndecided stringInputMode = iota
	stringInputOwned
	stringInputBorrowed
)

// BorrowedBytes is an opt-in String value that remains owned by the caller.
// Unlike []byte, appending BorrowedBytes does not copy its contents into the
// column buffer. The caller must keep the underlying bytes immutable until the
// enclosing batch's Send returns, or until Abort or Close returns if the batch
// is not sent.
type BorrowedBytes []byte

func (col *String) Reset() {
	col.col.Reset()
	col.borrowed.Reset()
	col.inputMode = stringInputUndecided
	col.pendingEmpties = 0
}

func (col String) Name() string {
	return col.name
}

func (String) Type() Type {
	return "String"
}

func (String) ScanType() reflect.Type {
	return scanTypeString
}

func (col *String) Rows() int {
	switch col.inputMode {
	case stringInputBorrowed:
		return col.borrowed.Rows()
	case stringInputUndecided:
		return col.pendingEmpties
	default:
		return col.col.Rows()
	}
}

func (col *String) Row(i int, ptr bool) any {
	var val string
	switch col.inputMode {
	case stringInputBorrowed:
		val = string(col.borrowed.RowBytes(i))
	case stringInputUndecided:
		val = ""
	default:
		val = col.col.Row(i)
	}
	if ptr {
		return &val
	}
	return val
}

func (col *String) ScanRow(dest any, row int) error {
	val := col.Row(row, false).(string)
	switch d := dest.(type) {
	case *string:
		*d = val
	case **string:
		*d = new(string)
		**d = val
	case *sql.NullString:
		return d.Scan(val)
	case *[]byte:
		*d = binary.Str2Bytes(val, len(val))
	case **[]byte:
		*d = new([]byte)
		**d = binary.Str2Bytes(val, len(val))
	case *json.RawMessage:
		*d = binary.Str2Bytes(val, len(val))
	case **json.RawMessage:
		*d = new(json.RawMessage)
		**d = binary.Str2Bytes(val, len(val))
	case encoding.BinaryUnmarshaler:
		return d.UnmarshalBinary(binary.Str2Bytes(val, len(val)))
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(val)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "String",
		}
	}
	return nil
}

func (col *String) AppendRow(v any) error {
	switch v := v.(type) {
	case string:
		return col.appendOwnedString(v)
	case *string:
		switch {
		case v != nil:
			return col.appendOwnedString(*v)
		default:
			col.appendEmpty()
		}
	case sql.NullString:
		switch v.Valid {
		case true:
			return col.appendOwnedString(v.String)
		default:
			col.appendEmpty()
		}
	case *sql.NullString:
		switch v != nil && v.Valid {
		case true:
			return col.appendOwnedString(v.String)
		default:
			col.appendEmpty()
		}
	case json.RawMessage:
		return col.appendOwnedBytes(v)
	case *json.RawMessage:
		if v == nil {
			col.appendEmpty()
			break
		}
		return col.appendOwnedBytes(*v)
	case []byte:
		return col.appendOwnedBytes(v)
	case *[]byte:
		if v == nil {
			col.appendEmpty()
			break
		}
		return col.appendOwnedBytes(*v)
	case BorrowedBytes:
		return col.appendBorrowed(v)
	case *BorrowedBytes:
		if v != nil {
			return col.appendBorrowed(*v)
		} else {
			col.appendEmpty()
		}
	case nil:
		col.appendEmpty()
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "String",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}

		if s, ok := v.(fmt.Stringer); ok {
			return col.AppendRow(s.String())
		}

		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "String",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *String) selectInputMode(mode stringInputMode) error {
	if col.inputMode != stringInputUndecided && col.inputMode != mode {
		return fmt.Errorf("clickhouse: cannot mix borrowed and owned String values in one column")
	}
	if col.inputMode == mode {
		return nil
	}
	col.inputMode = mode
	for range col.pendingEmpties {
		if mode == stringInputBorrowed {
			col.borrowed.Append(nil)
		} else {
			col.col.Append("")
		}
	}
	col.pendingEmpties = 0
	return nil
}

func (col *String) appendOwnedString(v string) error {
	if len(v) == 0 {
		col.appendEmpty()
		return nil
	}
	if err := col.selectInputMode(stringInputOwned); err != nil {
		return err
	}
	col.col.Append(v)
	return nil
}

func (col *String) appendOwnedBytes(v []byte) error {
	if len(v) == 0 {
		col.appendEmpty()
		return nil
	}
	if err := col.selectInputMode(stringInputOwned); err != nil {
		return err
	}
	col.col.AppendBytes(v)
	return nil
}

func (col *String) appendBorrowed(v []byte) error {
	if len(v) == 0 {
		col.appendEmpty()
		return nil
	}
	if err := col.selectInputMode(stringInputBorrowed); err != nil {
		return err
	}
	col.borrowed.Append(v)
	return nil
}

func (col *String) appendEmpty() {
	switch col.inputMode {
	case stringInputOwned:
		col.col.Append("")
	case stringInputBorrowed:
		col.borrowed.Append(nil)
	default:
		col.pendingEmpties++
	}
}

func (col *String) finalizeInputMode() {
	if col.inputMode != stringInputUndecided {
		return
	}
	col.inputMode = stringInputOwned
	for range col.pendingEmpties {
		col.col.Append("")
	}
	col.pendingEmpties = 0
}

func (col *String) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []string:
		if err := col.selectInputMode(stringInputOwned); err != nil {
			return nil, err
		}
		col.col.AppendArr(v)
		nulls = make([]uint8, len(v))
	case []*string:
		if err := col.selectInputMode(stringInputOwned); err != nil {
			return nil, err
		}
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(*v[i])
			default:
				col.col.Append("")
				nulls[i] = 1
			}
		}
	case []sql.NullString:
		nulls = make([]uint8, len(v))
		for i := range v {
			if err := col.AppendRow(v[i]); err != nil {
				return nil, err
			}
		}
	case []*sql.NullString:
		nulls = make([]uint8, len(v))
		for i := range v {
			if v[i] == nil {
				nulls[i] = 1
			}
			if err := col.AppendRow(v[i]); err != nil {
				return nil, err
			}
		}
	case []json.RawMessage:
		if err := col.selectInputMode(stringInputOwned); err != nil {
			return nil, err
		}
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(string(v[i]))
		}
	case []*json.RawMessage:
		if err := col.selectInputMode(stringInputOwned); err != nil {
			return nil, err
		}
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(string(*v[i]))
		}
	case []byte:
		if err := col.selectInputMode(stringInputOwned); err != nil {
			return nil, err
		}
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(string(v[i]))
		}
	case []*byte:
		if err := col.selectInputMode(stringInputOwned); err != nil {
			return nil, err
		}
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(string(*v[i]))
		}
	case [][]byte:
		if err := col.selectInputMode(stringInputOwned); err != nil {
			return nil, err
		}
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(string(v[i]))
		}
	case []BorrowedBytes:
		if err := col.selectInputMode(stringInputBorrowed); err != nil {
			return nil, err
		}
		nulls = make([]uint8, len(v))
		for i := range v {
			col.borrowed.Append(v[i])
		}
	case []*BorrowedBytes:
		if err := col.selectInputMode(stringInputBorrowed); err != nil {
			return nil, err
		}
		nulls = make([]uint8, len(v))
		for i := range v {
			if v[i] == nil {
				nulls[i] = 1
				col.borrowed.Append(nil)
				continue
			}
			col.borrowed.Append(*v[i])
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "String",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "String",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *String) Decode(reader *proto.Reader, rows int) error {
	col.borrowed.Reset()
	col.pendingEmpties = 0
	col.inputMode = stringInputOwned
	return col.col.DecodeColumn(reader, rows)
}

func (col *String) Encode(buffer *proto.Buffer) {
	col.finalizeInputMode()
	if col.inputMode == stringInputBorrowed {
		col.borrowed.EncodeColumn(buffer)
		return
	}
	col.col.EncodeColumn(buffer)
}

// Write streams String input into writer. Borrowed mode chains caller-owned
// values directly; owned mode preserves the existing ColStr behavior.
func (col *String) Write(writer *proto.Writer) {
	col.finalizeInputMode()
	if col.inputMode == stringInputBorrowed {
		col.borrowed.WriteColumn(writer)
		return
	}
	col.col.WriteColumn(writer)
}

var _ Interface = (*String)(nil)
var _ CustomWriting = (*String)(nil)
