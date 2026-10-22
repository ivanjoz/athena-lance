package lancego

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"regexp"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// TableDef declares a table, like GlueTable in the smartberry datalake. Columns, their
// types and per-column options come from the row type's lance tags.
type TableDef struct {
	Name string
	// PartitionKeys are the columns that identify a partition, in order (like
	// GlueTable.PartitionKeys). Save replaces whole partitions; empty = append-only table.
	PartitionKeys []string
	// SortKey orders the rows inside each file (like IcebergSortOrder). If empty, the
	// columns tagged sort=<n> are used.
	SortKey []string
	// Indexes are added to the ones declared with index=<type> tags.
	Indexes []IndexSpec
}

// Table is a typed handle on a Lance table whose rows are T.
type Table[T any] struct {
	c       *Client
	def     TableDef
	layout  *layout
	partCol []int // layout column index of each partition key
	sortKey []string
	indexes []IndexSpec
	// HashFunc computes a partition's hash in Save. Nil = DefaultHash. It must not depend
	// on row order if the source can return rows in a different order.
	HashFunc func(rows []T) uint64
}

// NewTable returns a handle for the table declared by def, with rows of type T. It
// validates T's tags and def but does not contact the service.
func NewTable[T any](c *Client, def TableDef) (*Table[T], error) {
	if !columnNameRe.MatchString(def.Name) {
		return nil, fmt.Errorf("lancego: invalid table name %q (must match %s)", def.Name, columnNameRe)
	}
	l, err := layoutOf(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	partCol, err := l.partitionCols(def.PartitionKeys)
	if err != nil {
		return nil, err
	}
	t := &Table[T]{c: c, def: def, layout: l, partCol: partCol, sortKey: l.sortKey, indexes: l.indexes}
	if len(def.SortKey) > 0 {
		t.sortKey = def.SortKey
	}
	t.indexes = append(append([]IndexSpec(nil), l.indexes...), def.Indexes...)
	for _, name := range append(append([]string(nil), t.sortKey...), indexColumns(def.Indexes)...) {
		if l.schema.FieldIndices(name) == nil {
			return nil, fmt.Errorf("lancego: table %s: %q is not a column", def.Name, name)
		}
	}
	return t, nil
}

func indexColumns(ix []IndexSpec) []string {
	cols := make([]string, len(ix))
	for i, x := range ix {
		cols[i] = x.Column
	}
	return cols
}

func (t *Table[T]) Name() string            { return t.def.Name }
func (t *Table[T]) Schema() *arrow.Schema   { return t.layout.schema }
func (t *Table[T]) PartitionKeys() []string { return t.def.PartitionKeys }
func (t *Table[T]) SortKey() []string       { return t.sortKey }
func (t *Table[T]) Indexes() []IndexSpec    { return t.indexes }

// CreateOptions tune create_table.
type CreateOptions struct {
	IfNotExists    bool   // return ok (created=false) if the table exists with the same spec
	StorageVersion string // Lance file format; empty = service default ("2.2")
}

// Create asks lance-control to create the table from T's schema, sort key and indexes.
func (t *Table[T]) Create(ctx context.Context, opts CreateOptions) (*CreateTableResult, error) {
	schemaIPC, err := SchemaIPC(t.layout.schema)
	if err != nil {
		return nil, err
	}
	req := createTableRequest{
		Op:             "create_table",
		Table:          t.def.Name,
		SchemaIPCB64:   base64.StdEncoding.EncodeToString(schemaIPC),
		PartitionKeys:  t.def.PartitionKeys,
		SortKey:        t.sortKey,
		Indexes:        t.indexes,
		StorageVersion: opts.StorageVersion,
		IfNotExists:    opts.IfNotExists,
	}
	if len(t.layout.fieldCompression) > 0 {
		req.FieldCompression = t.layout.fieldCompression
	}
	var out CreateTableResult
	if err := t.c.invoke(ctx, t.c.cfg.Lambda.ControlFunction, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Describe returns the table descriptor from lance-control.
func (t *Table[T]) Describe(ctx context.Context) (json.RawMessage, error) {
	return t.c.DescribeTable(ctx, t.def.Name)
}

// InsertOptions tune Stage and Insert.
type InsertOptions struct {
	// BatchID identifies the batch for idempotency: re-sending the same BatchID is a
	// no-op on the service. Empty = a new unique ID. Allowed: [A-Za-z0-9_.-], max 100.
	BatchID string
	// BatchRows is the max rows per Arrow record batch; 0 = config ingest.batch_rows.
	BatchRows int
}

// StagedBatch describes a batch uploaded to the staging area.
type StagedBatch struct {
	Table   string
	BatchID string
	URIs    []string
	Rows    int
	Bytes   int
}

// InsertResult is returned by Insert once the rows are committed.
type InsertResult struct {
	StagedBatch
	Batch BatchStatus
}

var batchIDRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)

// Stage converts rows to an Arrow IPC stream and uploads it to
// s3://<bucket>/<staging_prefix>/<table>/<batch_id>/part-0001.arrows without queueing it.
func (t *Table[T]) Stage(ctx context.Context, rows []T, opts InsertOptions) (*StagedBatch, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("lancego: no rows to insert")
	}
	cfg := t.c.cfg
	if cfg.Storage.Bucket == "" {
		return nil, fmt.Errorf("lancego: storage.bucket not configured")
	}
	batchID := opts.BatchID
	if batchID == "" {
		batchID = newBatchID()
	} else if !batchIDRe.MatchString(batchID) {
		return nil, fmt.Errorf("lancego: invalid batch id %q", batchID)
	}
	batchRows := opts.BatchRows
	if batchRows == 0 {
		batchRows = cfg.Ingest.BatchRows
	}

	var buf bytes.Buffer
	if err := WriteIPC(&buf, rows, batchRows); err != nil {
		return nil, err
	}
	size := buf.Len()
	key := path.Join(cfg.Storage.StagingPrefix, t.def.Name, batchID, "part-0001.arrows")
	if err := t.c.tr.putObject(ctx, cfg.Storage.Bucket, key, "application/vnd.apache.arrow.stream", buf.Bytes()); err != nil {
		return nil, fmt.Errorf("lancego: uploading s3://%s/%s: %w", cfg.Storage.Bucket, key, err)
	}
	return &StagedBatch{
		Table:   t.def.Name,
		BatchID: batchID,
		URIs:    []string{fmt.Sprintf("s3://%s/%s", cfg.Storage.Bucket, key)},
		Rows:    len(rows),
		Bytes:   size,
	}, nil
}

// Insert appends rows to a table without PartitionKeys and returns once they are
// committed. Retrying with the same BatchID is safe.
func (t *Table[T]) Insert(ctx context.Context, rows []T, opts InsertOptions) (*InsertResult, error) {
	if len(t.def.PartitionKeys) > 0 {
		return nil, fmt.Errorf("lancego: table %s is partitioned; use Save so partitions are replaced, not duplicated", t.def.Name)
	}
	staged, err := t.Stage(ctx, rows, opts)
	if err != nil {
		return nil, err
	}
	st, err := t.Ingest(ctx, staged)
	if err != nil {
		return nil, err
	}
	return &InsertResult{StagedBatch: *staged, Batch: *st}, nil
}

// Ingest commits an already staged batch (append) and returns its committed status.
func (t *Table[T]) Ingest(ctx context.Context, staged *StagedBatch) (*BatchStatus, error) {
	req := ingestRequest{
		Op:      "ingest",
		Table:   staged.Table,
		BatchID: staged.BatchID,
		Source:  ingestSource{Format: "arrow_ipc", URIs: staged.URIs},
	}
	var st BatchStatus
	if err := t.c.write(ctx, req, &st, false); err != nil {
		return nil, fmt.Errorf("lancego: ingest of batch %s: %w", staged.BatchID, err)
	}
	return &st, nil
}

// Info returns the table status (see Client.TableInfo).
func (t *Table[T]) Info(ctx context.Context, detail bool) (*TableInfo, error) {
	return t.c.TableInfo(ctx, t.def.Name, detail)
}

// CompactOptions bound one compaction run. Adjacent small files are merged into one file
// while the group stays within BOTH MaxFileMB and MaxFiles. Zero = service default.
type CompactOptions struct {
	MaxFileMB int // max size of each output file, in MB (e.g. 50)
	MaxFiles  int // max input files merged into one output file (e.g. 20)
	MaxTasks  int // max groups rewritten in this run, so it fits in the Lambda timeout
}

// Compact runs one bounded compaction of the table and returns its result. It holds the
// table lock, so it never runs at the same time as a write to the same table. If
// result.More is true, more files were eligible than MaxTasks groups: call it again.
// It also removes versions older than the service retention (and the files only they use).
func (t *Table[T]) Compact(ctx context.Context, opts CompactOptions) (*CompactResult, error) {
	if opts.MaxFileMB < 0 || opts.MaxFiles < 0 || opts.MaxTasks < 0 {
		return nil, fmt.Errorf("lancego: compaction limits must not be negative")
	}
	if opts.MaxFiles == 1 {
		return nil, fmt.Errorf("lancego: MaxFiles must be at least 2 (one file has nothing to merge)")
	}
	// Time-first, so operations sort with the write batches in the ledger.
	opID := newBatchID() + "-compact"
	req := compactRequest{
		Op:          "compact",
		Table:       t.def.Name,
		OperationID: opID,
		MaxFileMB:   opts.MaxFileMB,
		MaxFiles:    opts.MaxFiles,
		MaxTasks:    opts.MaxTasks,
	}
	var out CompactResult
	if err := t.c.write(ctx, req, &out, false); err != nil {
		return nil, fmt.Errorf("lancego: compaction of %s (%s): %w", t.def.Name, opID, err)
	}
	return &out, nil
}

// newBatchID returns a sortable unique ID like 20261021T120000Z-1a2b3c4d5e6f.
func newBatchID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
}
