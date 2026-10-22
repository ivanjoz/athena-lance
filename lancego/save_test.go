package lancego

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// fakeService implements the transport by simulating the Lance service contract:
// replace_partitions commits immediately (visible after one "not found" poll) and keeps
// the partition registry, like the DynamoDB ledger will.
type fakeService struct {
	mu       sync.Mutex
	objects  map[string][]byte
	registry map[string]PartitionInfo
	batches  map[string]BatchStatus
	polls    map[string]int
	version  int64
	requests []replacePartitionsRequest
	failNext bool
	partRows map[string]int // rows currently stored per partition key
}

func newFake() *fakeService {
	return &fakeService{
		objects: map[string][]byte{}, registry: map[string]PartitionInfo{},
		batches: map[string]BatchStatus{}, polls: map[string]int{}, partRows: map[string]int{},
	}
}

func (f *fakeService) putObject(_ context.Context, bucket, key, _ string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects["s3://"+bucket+"/"+key] = append([]byte(nil), body...)
	return nil
}

// replacePartitions applies a replace_partitions request (caller holds f.mu).
func (f *fakeService) replacePartitions(payload []byte) (*BatchStatus, *APIError, error) {
	var req replacePartitionsRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, nil, err
	}
	f.requests = append(f.requests, req)
	if f.failNext {
		f.failNext = false
		apiErr := &APIError{Code: "SCHEMA_MISMATCH", Message: "boom"}
		f.batches[req.BatchID] = BatchStatus{BatchID: req.BatchID, Status: BatchFailed, Error: apiErr}
		return nil, apiErr, nil
	}

	// Count the rows of each partition in the staged file, and check they match.
	got := map[string]int{}
	if req.Source != nil {
		r, err := ipc.NewReader(bytes.NewReader(f.objects[req.Source.URIs[0]]))
		if err != nil {
			return nil, nil, err
		}
		for r.Next() {
			rec := r.RecordBatch()
			for i := range int(rec.NumRows()) {
				key := fmt.Sprintf("filial_id=%v/fecha=%v", rec.Column(0).ValueStr(i), rec.Column(1).ValueStr(i))
				got[key]++
			}
		}
		r.Release()
	}
	f.version++
	now := time.Now().Unix()
	for _, p := range req.Partitions {
		if got[p.Key] != p.Rows {
			return nil, nil, fmt.Errorf("partition %s: file has %d rows, request says %d", p.Key, got[p.Key], p.Rows)
		}
		if p.Rows == 0 {
			delete(f.registry, p.Key)
			delete(f.partRows, p.Key)
			continue
		}
		f.partRows[p.Key] = p.Rows // replace, never add
		f.registry[p.Key] = PartitionInfo{Key: p.Key, Values: p.Values, ID: p.ID, Hash: p.Hash,
			RecordsUpdated: p.RecordsUpdated, Rows: int64(p.Rows), CommittedAt: now,
			Version: f.version, BatchID: req.BatchID}
	}
	st := BatchStatus{BatchID: req.BatchID, Status: BatchCommitted, Version: f.version, CommittedAt: now}
	f.batches[req.BatchID] = st
	return &st, nil, nil
}

func (f *fakeService) invoke(_ context.Context, function string, payload []byte, async bool) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var req struct {
		Op      string `json:"op"`
		BatchID string `json:"batch_id"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	var result any
	switch req.Op {
	case "replace_partitions":
		if function != "lance-ingest" {
			return nil, fmt.Errorf("write sent to %s", function)
		}
		st, apiErr, err := f.replacePartitions(payload)
		switch {
		case err != nil:
			return nil, err
		case async:
			return nil, nil
		case apiErr != nil:
			return json.Marshal(envelope{RequestID: "r", Error: apiErr})
		}
		result = st
	case "list_partitions":
		parts := []PartitionInfo{}
		for _, p := range f.registry {
			parts = append(parts, p)
		}
		result = map[string]any{"partitions": parts}
	case "get_batch":
		// The first poll finds nothing, as when an async invocation has not started yet.
		f.polls[req.BatchID]++
		st, ok := f.batches[req.BatchID]
		if !ok || f.polls[req.BatchID] == 1 {
			return json.Marshal(envelope{RequestID: "r", Error: &APIError{Code: "BATCH_NOT_FOUND", Message: "not yet"}})
		}
		result = st
	default:
		return nil, fmt.Errorf("unexpected op %s", req.Op)
	}
	raw, _ := json.Marshal(result)
	return json.Marshal(envelope{RequestID: "r", OK: true, Result: raw})
}

type Venta struct {
	FilialID int16     `lance:"filial_id"`
	Fecha    string    `lance:"fecha"`
	ID       int64     `lance:"id"`
	Amount   float64   `lance:"amount"`
	At       time.Time `lance:"at"`
}

func (v Venta) GetParquetID() int64 { return int64(v.FilialID)*1000 + int64(len(v.Fecha)) }

var ventasDef = TableDef{Name: "ventas", PartitionKeys: []string{"filial_id", "fecha"}}

func testTable(t *testing.T) (*Table[Venta], *fakeService) {
	t.Helper()
	fake := newFake()
	tbl, err := NewTable[Venta](testClient(t, fake), ventasDef)
	if err != nil {
		t.Fatal(err)
	}
	return tbl, fake
}

var testConfig = Config{
	Bucket:          "bucket--usw2-az1--x-s3",
	ControlFunction: "lance-control",
	IngestFunction:  "lance-ingest",
}

func testClient(t *testing.T, tr transport) *Client {
	t.Helper()
	c, err := newClient(testConfig, tr)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestConfigRequiredAndDefaults(t *testing.T) {
	_, err := newClient(Config{ControlFunction: "c"}, newFake())
	for _, field := range []string{"IngestFunction", "Bucket"} {
		if err == nil || !strings.Contains(err.Error(), "Config."+field+" is required") {
			t.Errorf("missing %s not reported: %v", field, err)
		}
	}
	c := testClient(t, newFake())
	if c.cfg.StagingPrefix != DefaultStagingPrefix || c.cfg.BatchRows != DefaultBatchRows ||
		c.cfg.MaxRowsPerRequest != DefaultMaxRowsPerRequest || c.cfg.WaitTimeout != DefaultWaitTimeout {
		t.Errorf("defaults not applied: %+v", c.cfg)
	}
}

func ventas() []Venta {
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	var rows []Venta
	for i := range 30 {
		rows = append(rows, Venta{FilialID: int16(1 + i%2), Fecha: []string{"2026-10-01", "2026-10-02", "2026-10-03"}[i%3],
			ID: int64(i), Amount: float64(i) * 1.5, At: t0.Add(time.Duration(i) * time.Minute)})
	}
	return rows // 6 partitions x 5 rows
}

func TestSaveReplacesOnlyChangedPartitions(t *testing.T) {
	tbl, fake := testTable(t)
	ctx := context.Background()

	res, err := tbl.Save(ctx, ventas(), SaveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sent != 6 || res.Unchanged != 0 || len(res.Batches) != 1 || res.Batches[0].Status != BatchCommitted {
		t.Fatalf("first save = %+v", res)
	}
	if len(fake.registry) != 6 {
		t.Fatalf("registry has %d partitions", len(fake.registry))
	}

	// Same rows in another order: same hashes, nothing sent.
	shuffled := ventas()
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	res, err = tbl.Save(ctx, shuffled, SaveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sent != 0 || res.Unchanged != 6 || len(res.Batches) != 0 {
		t.Fatalf("unchanged save = %+v", res)
	}

	// One row changes: only its partition is replaced, with all its rows.
	changed := ventas()
	changed[0].Amount = 999
	res, err = tbl.Save(ctx, changed, SaveOptions{RecordsUpdated: map[int64]int64{changed[0].GetParquetID(): 1792640000}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sent != 1 || res.Unchanged != 5 {
		t.Fatalf("one-change save = %+v", res)
	}
	last := fake.requests[len(fake.requests)-1]
	p := last.Partitions[0]
	if p.Key != "filial_id=1/fecha=2026-10-01" || p.Rows != 5 || p.ID != changed[0].GetParquetID() || p.RecordsUpdated != 1792640000 {
		t.Fatalf("partition write = %+v", p)
	}
	if fake.partRows[p.Key] != 5 {
		t.Fatalf("partition should be replaced, not appended: %d rows", fake.partRows[p.Key])
	}
	if v, ok := p.Values[0].(float64); !ok || v != 1 || p.Values[1] != "2026-10-01" {
		t.Fatalf("typed values = %#v", p.Values)
	}
}

func TestSaveSplitsBatchesWithoutSplittingPartitions(t *testing.T) {
	tbl, fake := testTable(t)
	res, err := tbl.Save(context.Background(), ventas(), SaveOptions{MaxRowsPerBatch: 12, Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Batches) != 3 { // 5+5 | 5+5 | 5+5 (a third partition would make 15 > 12)
		t.Fatalf("batches = %d", len(res.Batches))
	}
	for _, req := range fake.requests {
		total := 0
		for _, p := range req.Partitions {
			total += p.Rows
		}
		if total > 12 {
			t.Errorf("batch %s has %d rows", req.BatchID, total)
		}
	}
}

func TestDeletePartitionsUsesSaveKeys(t *testing.T) {
	tbl, fake := testTable(t)
	ctx := context.Background()
	if _, err := tbl.Save(ctx, ventas(), SaveOptions{}); err != nil {
		t.Fatal(err)
	}
	st, err := tbl.DeletePartitions(ctx, []any{1, "2026-10-01"}, []any{int16(2), "2026-10-02"})
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != BatchCommitted || len(fake.registry) != 4 {
		t.Fatalf("status %+v, registry %d", st, len(fake.registry))
	}
	for _, gone := range []string{"filial_id=1/fecha=2026-10-01", "filial_id=2/fecha=2026-10-02"} {
		if _, ok := fake.registry[gone]; ok {
			t.Errorf("%s still registered", gone)
		}
	}
	if _, err := tbl.DeletePartitions(ctx, []any{1}); err == nil {
		t.Error("wrong number of values should fail")
	}
	if _, err := tbl.DeletePartitions(ctx, []any{"x", "2026-10-01"}); err == nil {
		t.Error("string for an int column should fail")
	}
}

func TestSaveReportsFailedBatch(t *testing.T) {
	tbl, fake := testTable(t)
	fake.failNext = true
	_, err := tbl.Save(context.Background(), ventas(), SaveOptions{})
	var apiErr *APIError
	if !errors.Is(err, ErrBatchFailed) || !errors.As(err, &apiErr) || apiErr.Code != "SCHEMA_MISMATCH" {
		t.Fatalf("err = %v", err)
	}
}

func TestSaveNoWaitThenWait(t *testing.T) {
	tbl, _ := testTable(t)
	ctx := context.Background()
	res, err := tbl.Save(ctx, ventas(), SaveOptions{NoWait: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Batches) != 1 || res.Batches[0].Status != BatchPending {
		t.Fatalf("no-wait save = %+v", res)
	}
	st, err := tbl.Wait(ctx, res.Batches[0].BatchID)
	if err != nil || st.Status != BatchCommitted {
		t.Fatalf("wait = %+v, %v", st, err)
	}
}

func TestPartitionedTableRejectsInsert(t *testing.T) {
	tbl, _ := testTable(t)
	if _, err := tbl.Insert(context.Background(), ventas(), InsertOptions{}); err == nil {
		t.Error("Insert on a partitioned table should fail")
	}
}

func TestTableDefValidation(t *testing.T) {
	type withPtr struct {
		A *int32 `lance:"a"`
		B int32  `lance:"b"`
	}
	c := testClient(t, newFake())
	if _, err := NewTable[Venta](c, TableDef{Name: "v", PartitionKeys: []string{"nope"}}); err == nil {
		t.Error("unknown partition key should fail")
	}
	if _, err := NewTable[withPtr](c, TableDef{Name: "w", PartitionKeys: []string{"a"}}); err == nil {
		t.Error("nullable partition key should fail")
	}
	if _, err := NewTable[Venta](c, TableDef{Name: "Ventas"}); err == nil {
		t.Error("uppercase table name should fail")
	}
	if _, err := NewTable[Venta](c, TableDef{Name: "v", SortKey: []string{"zzz"}}); err == nil {
		t.Error("unknown sort column should fail")
	}
}

func TestDefaultHash(t *testing.T) {
	a := ventas()
	b := ventas()
	rand.Shuffle(len(b), func(i, j int) { b[i], b[j] = b[j], b[i] })
	if DefaultHash(a) != DefaultHash(b) {
		t.Error("hash must not depend on row order")
	}
	b[3].Amount += 0.01
	if DefaultHash(a) == DefaultHash(b) {
		t.Error("hash must change when a value changes")
	}
	if DefaultHash(a[:1]) == DefaultHash(append(a[:1:1], a[0])) {
		t.Error("a duplicated row must change the hash")
	}
}

func TestHashJSON(t *testing.T) {
	for _, h := range []Hash{0, 1, 0xa1b2c3d4e5f60718, 1<<64 - 1} {
		b, _ := json.Marshal(h)
		var back Hash
		if err := json.Unmarshal(b, &back); err != nil || back != h {
			t.Errorf("%016x -> %s -> %016x (%v)", uint64(h), b, uint64(back), err)
		}
	}
}
