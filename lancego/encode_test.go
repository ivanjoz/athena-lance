package lancego

import (
	"bytes"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type Region string

type Audit struct {
	CreatedBy string `lance:"created_by"`
}

type Address struct {
	City string  `lance:"city"`
	Zip  *string `lance:"zip"`
}

type Sale struct {
	Audit              // promoted: created_by
	ID       int64     `lance:"id,index=btree"`
	Region   Region    `lance:"region,sort=1,index=bitmap"`
	Amount   *float64  `lance:"*"`
	Qty      int32     `lance:"qty"`
	Paid     bool      `lance:"paid"`
	At       time.Time `lance:"ts,sort=2,index=zonemap"`
	Tags     []string  `lance:"tags,index=label_list"`
	Raw      []byte    `lance:"raw"`
	Ship     *Address  `lance:"ship"`
	Note     string    `lance:"note,compress=zstd"`
	Internal string    // no tag: ignored
	Skipped  string    `lance:"-"`
}

func ptr[T any](v T) *T { return &v }

func sampleSales() []Sale {
	t0 := time.Date(2026, 10, 21, 12, 0, 0, 123456000, time.UTC)
	return []Sale{
		{Audit{"ana"}, 1, "PE", ptr(10.5), 3, true, t0, []string{"a", "b"}, []byte{1, 2}, &Address{"Lima", ptr("15001")}, "x", "i", "s"},
		{Audit{"bob"}, 2, "CL", nil, 0, false, t0.Add(time.Hour), nil, nil, nil, "", "", ""},
		{Audit{"cy"}, 3, "MX", ptr(-1.0), 7, true, t0.Add(2 * time.Hour), []string{}, []byte{}, &Address{"CDMX", nil}, "y", "", ""},
	}
}

func TestSchemaFromTags(t *testing.T) {
	schema, err := SchemaOf[Sale]()
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		name     string
		typ      string
		nullable bool
	}{
		{"created_by", "utf8", false},
		{"id", "int64", false},
		{"region", "utf8", false},
		{"amount", "float64", true},
		{"qty", "int32", false},
		{"paid", "bool", false},
		{"ts", "timestamp[us, tz=UTC]", false},
		{"tags", "list<item: utf8>", true},
		{"raw", "binary", true},
		{"ship", "struct<city: utf8, zip: utf8 nullable>", true},
		{"note", "utf8", false},
	}
	if schema.NumFields() != len(want) {
		t.Fatalf("got %d fields, want %d: %s", schema.NumFields(), len(want), schema)
	}
	for i, w := range want {
		f := schema.Field(i)
		if f.Name != w.name || f.Type.String() != w.typ || f.Nullable != w.nullable {
			t.Errorf("field %d = %s %s nullable=%v, want %s %s nullable=%v", i, f.Name, f.Type, f.Nullable, w.name, w.typ, w.nullable)
		}
	}

	l, _ := layoutOf(reflectType[Sale]())
	if got := strings.Join(l.sortKey, ","); got != "region,ts" {
		t.Errorf("sort key = %s", got)
	}
	wantIdx := map[string]IndexType{"id": IndexBTree, "region": IndexBitmap, "ts": IndexZoneMap, "tags": IndexLabelList}
	if len(l.indexes) != len(wantIdx) {
		t.Errorf("indexes = %v", l.indexes)
	}
	for _, ix := range l.indexes {
		if wantIdx[ix.Column] != ix.Type {
			t.Errorf("index %v unexpected", ix)
		}
	}
	if l.fieldCompression["note"] != "zstd" {
		t.Errorf("field compression = %v", l.fieldCompression)
	}
}

func TestToRecordValues(t *testing.T) {
	rec, err := ToRecord(sampleSales())
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if rec.NumRows() != 3 {
		t.Fatalf("rows = %d", rec.NumRows())
	}
	col := func(name string) arrow.Array {
		idx := rec.Schema().FieldIndices(name)
		return rec.Column(idx[0])
	}
	if got := col("region").(*array.String).Value(1); got != "CL" {
		t.Errorf("region[1] = %q", got)
	}
	amount := col("amount").(*array.Float64)
	if amount.Value(0) != 10.5 || !amount.IsNull(1) || amount.Value(2) != -1 {
		t.Errorf("amount = %v", amount)
	}
	ts := col("ts").(*array.Timestamp)
	if got := ts.Value(0).ToTime(arrow.Microsecond); !got.Equal(time.Date(2026, 10, 21, 12, 0, 0, 123456000, time.UTC)) {
		t.Errorf("ts[0] = %v", got)
	}
	tags := col("tags").(*array.List)
	if tags.IsNull(0) || !tags.IsNull(1) || tags.IsNull(2) {
		t.Errorf("tags nulls = %v", tags)
	}
	if s, e := tags.ValueOffsets(0); e-s != 2 {
		t.Errorf("tags[0] len = %d", e-s)
	}
	if s, e := tags.ValueOffsets(2); e-s != 0 {
		t.Errorf("tags[2] len = %d", e-s)
	}
	raw := col("raw").(*array.Binary)
	if !bytes.Equal(raw.Value(0), []byte{1, 2}) || !raw.IsNull(1) || raw.IsNull(2) {
		t.Errorf("raw = %v", raw)
	}
	ship := col("ship").(*array.Struct)
	if ship.IsNull(0) || !ship.IsNull(1) || ship.IsNull(2) {
		t.Errorf("ship nulls = %v", ship)
	}
	if got := ship.Field(0).(*array.String).Value(2); got != "CDMX" {
		t.Errorf("ship.city[2] = %q", got)
	}
	if !ship.Field(1).IsNull(2) {
		t.Errorf("ship.zip[2] should be null")
	}
	if got := col("created_by").(*array.String).Value(2); got != "cy" {
		t.Errorf("created_by[2] = %q", got)
	}
}

func TestPointerRows(t *testing.T) {
	rows := []*Sale{&sampleSales()[0]}
	rec, err := ToRecord(rows)
	if err != nil {
		t.Fatal(err)
	}
	rec.Release()
	if _, err := ToRecord([]*Sale{nil}); err == nil {
		t.Error("nil row pointer should fail")
	}
}

func TestIPCRoundTripInBatches(t *testing.T) {
	var rows []Sale
	for range 5 {
		rows = append(rows, sampleSales()...)
	}
	var buf bytes.Buffer
	if err := WriteIPC(&buf, rows, 4); err != nil {
		t.Fatal(err)
	}
	r, err := ipc.NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	want, _ := SchemaOf[Sale]()
	if !r.Schema().Equal(want) {
		t.Errorf("schema mismatch:\n%s\n%s", r.Schema(), want)
	}
	var total, batches int64
	for r.Next() {
		total += r.RecordBatch().NumRows()
		batches++
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	if total != 15 || batches != 4 {
		t.Errorf("rows=%d batches=%d, want 15 rows in 4 batches", total, batches)
	}
}

func TestSchemaIPCIsSchemaOnly(t *testing.T) {
	schema, _ := SchemaOf[Sale]()
	b, err := SchemaIPC(schema)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	if !r.Schema().Equal(schema) {
		t.Error("schema mismatch")
	}
	if r.Next() {
		t.Error("schema-only stream must have no batches")
	}
}

func TestSnakeCase(t *testing.T) {
	cases := map[string]string{
		"Amount":     "amount",
		"ID":         "id",
		"CustomerID": "customer_id",
		"HTTPServer": "http_server",
		"SoldAt":     "sold_at",
		"SoldAt2":    "sold_at2",
		"Field2Name": "field2_name",
		"UserIDs":    "user_ids",
		"A":          "a",
		"already_ok": "already_ok",
	}
	for in, want := range cases {
		if got := snakeCase(in); got != want {
			t.Errorf("snakeCase(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTagErrors(t *testing.T) {
	type noTags struct{ A int }
	type emptyName struct {
		A int `lance:""`
	}
	type badIndex struct {
		A int `lance:"a,index=hash"`
	}
	type dupColumn struct {
		A int `lance:"x"`
		B int `lance:"x"`
	}
	type dupSort struct {
		A int `lance:"a,sort=1"`
		B int `lance:"b,sort=1"`
	}
	type badType struct {
		M map[string]int `lance:"m"`
	}
	type unexported struct {
		a int `lance:"a"`
	}
	type upperName struct {
		A int `lance:"Amount"`
	}
	type badChars struct {
		A int `lance:"total-amount"`
	}
	type starClash struct {
		CustomerID int `lance:"*"`
		B          int `lance:"customer_id"`
	}
	cases := map[string]error{}
	_, cases["upperName"] = SchemaOf[upperName]()
	_, cases["badChars"] = SchemaOf[badChars]()
	_, cases["starClash"] = SchemaOf[starClash]()
	_, cases["noTags"] = SchemaOf[noTags]()
	_, cases["emptyName"] = SchemaOf[emptyName]()
	_, cases["badIndex"] = SchemaOf[badIndex]()
	_, cases["dupColumn"] = SchemaOf[dupColumn]()
	_, cases["dupSort"] = SchemaOf[dupSort]()
	_, cases["badType"] = SchemaOf[badType]()
	_, cases["unexported"] = SchemaOf[unexported]()
	_, cases["notStruct"] = SchemaOf[int]()
	for name, err := range cases {
		if err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

// TestWriteInteropFile writes testdata/sales.arrows for the Rust interop check
// (spikes/spike-a, bin go_interop). Run with LANCEGO_WRITE_TESTDATA=1.
func TestWriteInteropFile(t *testing.T) {
	if os.Getenv("LANCEGO_WRITE_TESTDATA") == "" {
		t.Skip("set LANCEGO_WRITE_TESTDATA=1 to write testdata/sales.arrows")
	}
	var rows []Sale
	for i := range 1000 {
		for _, s := range sampleSales() {
			s.ID = int64(i*3) + s.ID
			rows = append(rows, s)
		}
	}
	var buf bytes.Buffer
	if err := WriteIPC(&buf, rows, 1024); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("testdata/sales.arrows", buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func reflectType[T any]() reflect.Type { return reflect.TypeFor[T]() }
