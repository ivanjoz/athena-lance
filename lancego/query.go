package lancego

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// Query runs read-only SQL and decodes the rows into T by lance column name, so
// "SELECT * FROM <table> WHERE ..." returns []T. Columns without a field in T are ignored
// and fields without a column keep their zero value.
func (t *Table[T]) Query(ctx context.Context, sql string, opts QueryOptions) ([]T, *QueryResult, error) {
	res, err := t.c.Query(ctx, sql, opts)
	if err != nil {
		return nil, nil, err
	}
	rows, err := DecodeRows[T](res)
	return rows, res, err
}

// DecodeRows decodes a query result into structs with lance tags (column names), e.g. for
// an aggregate: struct{ Region string `lance:"region"`; Total float64 `lance:"total"` }.
func DecodeRows[T any](res *QueryResult) ([]T, error) {
	l, err := layoutOf(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	var raw []map[string]json.RawMessage
	if err := json.Unmarshal(res.Rows, &raw); err != nil {
		return nil, fmt.Errorf("lancego: decoding rows: %w", err)
	}
	out := make([]T, len(raw))
	for i, row := range raw {
		v := reflect.ValueOf(&out[i]).Elem()
		if v.Kind() == reflect.Pointer {
			v.Set(reflect.New(v.Type().Elem()))
			v = v.Elem()
		}
		if err := decodeColumns(v, l, row); err != nil {
			return nil, fmt.Errorf("lancego: row %d: %w", i, err)
		}
	}
	return out, nil
}

func decodeColumns(v reflect.Value, l *layout, row map[string]json.RawMessage) error {
	for _, col := range l.columns {
		raw, ok := row[col.field.Name]
		if !ok {
			continue
		}
		if err := decodeValue(v.FieldByIndex(col.index), raw); err != nil {
			return fmt.Errorf("column %s: %w", col.field.Name, err)
		}
	}
	return nil
}

var null = []byte("null")

// decodeValue sets v from the service's JSON rendering of an Arrow value (arrow-json):
// timestamps and dates are strings, binary is hex, structs are objects keyed by column name.
func decodeValue(v reflect.Value, raw json.RawMessage) error {
	if bytes.Equal(raw, null) {
		v.SetZero()
		return nil
	}
	t := v.Type()
	switch {
	case t.Kind() == reflect.Pointer:
		p := reflect.New(t.Elem())
		if err := decodeValue(p.Elem(), raw); err != nil {
			return err
		}
		v.Set(p)
	case t == timeType:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
		tm, err := parseTime(s)
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(tm))
	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
		b, err := hex.DecodeString(s)
		if err != nil {
			return fmt.Errorf("binary value is not hex: %w", err)
		}
		v.SetBytes(b)
	case t.Kind() == reflect.Slice || t.Kind() == reflect.Array:
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return err
		}
		if t.Kind() == reflect.Slice {
			v.Set(reflect.MakeSlice(t, len(items), len(items)))
		} else if len(items) > v.Len() {
			return fmt.Errorf("%d items do not fit in %s", len(items), t)
		}
		for i, item := range items {
			if err := decodeValue(v.Index(i), item); err != nil {
				return err
			}
		}
	case t.Kind() == reflect.Struct:
		l, err := layoutOf(t)
		if err != nil {
			return err
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return err
		}
		return decodeColumns(v, l, obj)
	default:
		return json.Unmarshal(raw, v.Addr().Interface())
	}
	return nil
}

// Timestamps with a time zone come with an offset; without one they are naive (read as UTC).
var timeLayouts = []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02"}

func parseTime(s string) (time.Time, error) {
	for _, layout := range timeLayouts {
		if tm, err := time.Parse(layout, s); err == nil {
			return tm, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized time %q", s)
}
