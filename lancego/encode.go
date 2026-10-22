package lancego

import (
	"bytes"
	"fmt"
	"io"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ToRecord converts rows into one Arrow record batch using T's lance tags.
// T must be a struct or a pointer to a struct; nil pointers are rejected.
// The caller must Release the returned record.
func ToRecord[T any](rows []T) (arrow.RecordBatch, error) {
	l, err := layoutOf(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	return l.record(memory.DefaultAllocator, reflect.ValueOf(rows))
}

// record builds a record from a slice value of rows.
func (l *layout) record(mem memory.Allocator, rows reflect.Value) (rec arrow.RecordBatch, err error) {
	b := array.NewRecordBuilder(mem, l.schema)
	defer b.Release()
	builders := b.Fields()
	n := rows.Len()
	for _, fb := range builders {
		fb.Reserve(n)
	}
	// Builders panic on a type mismatch; surface that as an error instead.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("lancego: encoding rows: %v", r)
		}
	}()
	for i := range n {
		row := rows.Index(i)
		if row.Kind() == reflect.Pointer {
			if row.IsNil() {
				return nil, fmt.Errorf("lancego: row %d is a nil pointer", i)
			}
			row = row.Elem()
		}
		for c, col := range l.columns {
			col.append(builders[c], row.FieldByIndex(col.index))
		}
	}
	return b.NewRecordBatch(), nil
}

// WriteIPC writes rows as an Arrow IPC stream (Zstd-compressed buffers) to w, in record
// batches of at most batchRows rows (0 = 65536). The stream embeds the schema.
// An empty rows slice still produces a valid, schema-only stream.
func WriteIPC[T any](w io.Writer, rows []T, batchRows int) error {
	l, err := layoutOf(reflect.TypeFor[T]())
	if err != nil {
		return err
	}
	if batchRows <= 0 {
		batchRows = 65536
	}
	mem := memory.DefaultAllocator
	iw := ipc.NewWriter(w, ipc.WithSchema(l.schema), ipc.WithZstd(), ipc.WithAllocator(mem))
	all := reflect.ValueOf(rows)
	for start := 0; start < all.Len(); start += batchRows {
		end := min(start+batchRows, all.Len())
		rec, err := l.record(mem, all.Slice(start, end))
		if err != nil {
			iw.Close()
			return err
		}
		err = iw.Write(rec)
		rec.Release()
		if err != nil {
			iw.Close()
			return fmt.Errorf("lancego: writing IPC batch: %w", err)
		}
	}
	return iw.Close()
}

// SchemaIPC serializes a schema as a schema-only Arrow IPC stream (no record batches),
// the form create_table expects.
func SchemaIPC(schema *arrow.Schema) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("lancego: writing schema IPC: %w", err)
	}
	return buf.Bytes(), nil
}
