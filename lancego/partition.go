package lancego

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// ParquetIDer is optional on row types, as in the smartberry datalake (ParqRegLedger): a
// partition identified by one integer. It is stored in the registry as the partition ID.
type ParquetIDer interface {
	GetParquetID() int64
}

// partitionCols resolves PartitionKeys to columns of the layout. Partition columns must be
// top-level, non-nullable scalars (ints, strings, bools, time.Time).
func (l *layout) partitionCols(keys []string) ([]int, error) {
	idx := make([]int, len(keys))
	for i, k := range keys {
		found := -1
		for c, col := range l.columns {
			if col.field.Name == k {
				found = c
				break
			}
		}
		if found < 0 {
			return nil, fmt.Errorf("lancego: partition key %q is not a column", k)
		}
		f := l.columns[found].field
		if f.Nullable {
			return nil, fmt.Errorf("lancego: partition key %q must not be nullable", k)
		}
		switch f.Type.ID() {
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32,
			arrow.UINT64, arrow.STRING, arrow.BOOL, arrow.TIMESTAMP:
		default:
			return nil, fmt.Errorf("lancego: partition key %q has unsupported type %s", k, f.Type)
		}
		idx[i] = found
	}
	return idx, nil
}

// partitionOf returns the partition's typed values (for the wire) and its key string,
// Hive style: "filial_id=1/fecha=2026-10-01" (values URL-path-escaped).
func (l *layout) partitionOf(row reflect.Value, keys []string, cols []int) ([]any, string) {
	values := make([]any, len(cols))
	parts := make([]string, len(cols))
	for i, c := range cols {
		v := row.FieldByIndex(l.columns[c].index)
		var val any
		var text string
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			val, text = v.Int(), fmt.Sprint(v.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			val, text = v.Uint(), fmt.Sprint(v.Uint())
		case reflect.Bool:
			val, text = v.Bool(), fmt.Sprint(v.Bool())
		case reflect.String:
			val, text = v.String(), v.String()
		default: // time.Time: microseconds, the column's unit
			t := v.Interface().(time.Time).UTC()
			val, text = t.UnixMicro(), t.Format(time.RFC3339Nano)
		}
		values[i] = val
		parts[i] = keys[i] + "=" + url.PathEscape(text)
	}
	return values, strings.Join(parts, "/")
}

// DefaultHash is the partition hash used when Table.HashFunc is nil: the sum of a 64-bit
// FNV-1a hash of each row's column values. A sum doesn't depend on row order, so the
// same set of rows always gives the same hash (the smartberry datalake hashes the same way).
func DefaultHash[T any](rows []T) uint64 {
	l, err := layoutOf(reflect.TypeFor[T]())
	if err != nil {
		panic(err)
	}
	var sum uint64
	h := fnv.New64a()
	var buf [8]byte
	for i := range rows {
		row := reflect.ValueOf(&rows[i]).Elem()
		if row.Kind() == reflect.Pointer {
			row = row.Elem()
		}
		h.Reset()
		for _, col := range l.columns {
			hashValue(h, row.FieldByIndex(col.index), &buf)
		}
		sum += h.Sum64()
	}
	return sum
}

type hashWriter interface{ Write([]byte) (int, error) }

func hashValue(h hashWriter, v reflect.Value, buf *[8]byte) {
	put := func(u uint64) {
		binary.LittleEndian.PutUint64(buf[:], u)
		h.Write(buf[:])
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			h.Write([]byte{0})
			return
		}
		h.Write([]byte{1})
		hashValue(h, v.Elem(), buf)
	case reflect.Bool:
		if v.Bool() {
			put(1)
		} else {
			put(0)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		put(uint64(v.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		put(v.Uint())
	case reflect.Float32, reflect.Float64:
		put(math.Float64bits(v.Float()))
	case reflect.String:
		put(uint64(v.Len()))
		h.Write([]byte(v.String()))
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			h.Write([]byte{0})
			return
		}
		put(uint64(v.Len()))
		if v.Type().Elem().Kind() == reflect.Uint8 && v.Kind() == reflect.Slice {
			h.Write(v.Bytes())
			return
		}
		for i := range v.Len() {
			hashValue(h, v.Index(i), buf)
		}
	case reflect.Struct:
		if t, ok := v.Interface().(time.Time); ok {
			put(uint64(t.UnixMicro()))
			return
		}
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				hashValue(h, v.Field(i), buf)
			}
		}
	}
}

// partitionGroup is the rows of one partition within a Save call.
type partitionGroup[T any] struct {
	key    string
	values []any
	rows   []T
	id     int64
}

// groupByPartition splits rows by partition, keeping first-seen order.
func groupByPartition[T any](l *layout, keys []string, cols []int, rows []T) []*partitionGroup[T] {
	byKey := map[string]*partitionGroup[T]{}
	var order []*partitionGroup[T]
	for i := range rows {
		row := reflect.ValueOf(&rows[i]).Elem()
		if row.Kind() == reflect.Pointer {
			row = row.Elem()
		}
		values, key := l.partitionOf(row, keys, cols)
		g, ok := byKey[key]
		if !ok {
			g = &partitionGroup[T]{key: key, values: values}
			if ider, ok := any(rows[i]).(ParquetIDer); ok {
				g.id = ider.GetParquetID()
			}
			byKey[key] = g
			order = append(order, g)
		}
		g.rows = append(g.rows, rows[i])
	}
	return order
}
