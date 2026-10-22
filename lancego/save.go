package lancego

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// SaveOptions tune Save.
type SaveOptions struct {
	// RecordsUpdated is, per partition ID (GetParquetID), the source's "updated" marker
	// to store with the partition, like GlueTable.RecordsUpdatedByParquetID. Stored as is.
	RecordsUpdated map[int64]int64
	// Force sends every partition, even if its hash matches the registry.
	Force bool
	// NoWait sends the batches as asynchronous Lambda invocations and returns without
	// waiting for the commit; use Wait / Batch with the returned batch IDs.
	NoWait bool
	// MaxRowsPerBatch caps the rows sent in one request (one Lance commit). A partition is
	// never split. 0 = Config.MaxRowsPerRequest.
	MaxRowsPerBatch int
}

// SaveResult reports what Save did.
type SaveResult struct {
	Unchanged int           // partitions skipped because their hash matched the registry
	Sent      int           // partitions written
	Batches   []BatchStatus // one per request; committed unless NoWait
}

// Version is the table version after the last batch, for QueryOptions.MinVersions
// (0 when nothing was committed, e.g. with NoWait or no changes).
func (r *SaveResult) Version() int64 {
	var v int64
	for _, b := range r.Batches {
		v = max(v, b.Version, b.DataVersion)
	}
	return v
}

// Save writes rows partition by partition, like SaveRecordsToParquet in the smartberry
// datalake: rows are grouped by PartitionKeys, each partition gets a hash, partitions whose
// hash matches the registry are skipped, and the rest REPLACE their previous data. Each
// request is one atomic Lance commit, so a partition is never seen half-written.
// Unless opts.NoWait is set, Save returns when every batch is committed (data queryable).
func (t *Table[T]) Save(ctx context.Context, rows []T, opts SaveOptions) (*SaveResult, error) {
	if len(t.def.PartitionKeys) == 0 {
		return nil, fmt.Errorf("lancego: table %s has no PartitionKeys; use Insert", t.def.Name)
	}
	if err := checkNilRows(rows); err != nil {
		return nil, err
	}
	groups := groupByPartition(t.layout, t.def.PartitionKeys, t.partCol, rows)

	hashFn := t.HashFunc
	if hashFn == nil {
		hashFn = DefaultHash[T]
	}
	var registry map[string]PartitionInfo
	if !opts.Force {
		parts, err := t.Partitions(ctx)
		if err != nil {
			return nil, fmt.Errorf("lancego: reading partition registry: %w", err)
		}
		registry = make(map[string]PartitionInfo, len(parts))
		for _, p := range parts {
			registry[p.Key] = p
		}
	}

	res := &SaveResult{}
	var pending []pendingPartition[T]
	for _, g := range groups {
		h := Hash(hashFn(g.rows))
		if prev, ok := registry[g.key]; ok && prev.Hash == h {
			res.Unchanged++
			continue
		}
		pending = append(pending, pendingPartition[T]{
			group: g,
			write: partitionWrite{
				Key: g.key, Values: g.values, ID: g.id, Hash: h,
				RecordsUpdated: opts.RecordsUpdated[g.id], Rows: len(g.rows),
			},
		})
	}
	res.Sent = len(pending)

	maxRows := opts.MaxRowsPerBatch
	if maxRows == 0 {
		maxRows = t.c.cfg.MaxRowsPerRequest
	}
	for _, batch := range splitBatches(pending, maxRows) {
		st, err := t.sendPartitions(ctx, batch, !opts.NoWait)
		if err != nil {
			return res, err
		}
		res.Batches = append(res.Batches, *st)
	}
	return res, nil
}

// DeletePartitions removes whole partitions (all their rows) in one atomic commit.
// Each partition is given as its values in PartitionKeys order, e.g. {1, "2026-10-01"}.
func (t *Table[T]) DeletePartitions(ctx context.Context, partitions ...[]any) (*BatchStatus, error) {
	if len(t.def.PartitionKeys) == 0 {
		return nil, fmt.Errorf("lancego: table %s has no PartitionKeys", t.def.Name)
	}
	var batch []pendingPartition[T]
	for _, values := range partitions {
		if len(values) != len(t.def.PartitionKeys) {
			return nil, fmt.Errorf("lancego: partition %v: want %d values (%v)", values, len(t.def.PartitionKeys), t.def.PartitionKeys)
		}
		// Build the key from a zero row with the partition values set, so it is formatted
		// exactly like the keys Save produces.
		key, typed, err := t.partitionKeyOf(values)
		if err != nil {
			return nil, err
		}
		batch = append(batch, pendingPartition[T]{write: partitionWrite{Key: key, Values: typed}})
	}
	return t.sendPartitions(ctx, batch, true)
}

// Partitions returns the registry: every partition with its hash, partition ID, row count,
// and the unix time and Lance version of its last committed write.
func (t *Table[T]) Partitions(ctx context.Context) ([]PartitionInfo, error) {
	var out struct {
		Partitions []PartitionInfo `json:"partitions"`
	}
	err := t.c.invoke(ctx, t.c.cfg.ControlFunction, tableRequest{Op: "list_partitions", Table: t.def.Name}, &out)
	return out.Partitions, err
}

// Batch returns the current status of a write batch.
func (t *Table[T]) Batch(ctx context.Context, batchID string) (*BatchStatus, error) {
	var out BatchStatus
	req := batchRequest{Op: "get_batch", Table: t.def.Name, BatchID: batchID}
	if err := t.c.invoke(ctx, t.c.cfg.ControlFunction, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ErrBatchFailed is returned (wrapped) by Wait when the service reports the batch failed.
var ErrBatchFailed = errors.New("lancego: batch failed")

// Wait polls the batch until it is committed or failed, or ctx ends. Without a deadline on
// ctx it gives up after Config.WaitTimeout.
func (t *Table[T]) Wait(ctx context.Context, batchID string) (*BatchStatus, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.c.cfg.WaitTimeout)
		defer cancel()
	}
	delay := 250 * time.Millisecond
	for {
		st, err := t.Batch(ctx, batchID)
		var apiErr *APIError
		switch {
		case errors.As(err, &apiErr) && apiErr.Code == "BATCH_NOT_FOUND":
			// The asynchronous invocation has not started yet.
		case err != nil:
			return nil, err
		case st.Status == BatchCommitted:
			return st, nil
		case st.Status == BatchFailed:
			return st, fmt.Errorf("%w: %s: %v", ErrBatchFailed, batchID, st.Error)
		}
		select {
		case <-ctx.Done():
			return st, fmt.Errorf("lancego: waiting for batch %s: %w", batchID, ctx.Err())
		case <-time.After(delay):
		}
		delay = min(delay*2, 2*time.Second)
	}
}

type pendingPartition[T any] struct {
	group *partitionGroup[T]
	write partitionWrite
}

// splitBatches packs partitions into batches of at most maxRows rows, never splitting a
// partition (a partition bigger than maxRows gets a batch of its own).
func splitBatches[T any](parts []pendingPartition[T], maxRows int) [][]pendingPartition[T] {
	var batches [][]pendingPartition[T]
	var cur []pendingPartition[T]
	rows := 0
	for _, p := range parts {
		if len(cur) > 0 && maxRows > 0 && rows+p.write.Rows > maxRows {
			batches = append(batches, cur)
			cur, rows = nil, 0
		}
		cur = append(cur, p)
		rows += p.write.Rows
	}
	if len(cur) > 0 {
		batches = append(batches, cur)
	}
	return batches
}

// sendPartitions stages the rows of a batch (if any) and sends replace_partitions; with
// wait it returns the committed status.
func (t *Table[T]) sendPartitions(ctx context.Context, batch []pendingPartition[T], wait bool) (*BatchStatus, error) {
	batchID := newBatchID()
	req := replacePartitionsRequest{Op: "replace_partitions", Table: t.def.Name, BatchID: batchID}
	var rows []T
	for _, p := range batch {
		req.Partitions = append(req.Partitions, p.write)
		if p.group != nil {
			rows = append(rows, p.group.rows...)
		}
	}
	if len(rows) > 0 {
		staged, err := t.Stage(ctx, rows, InsertOptions{BatchID: batchID})
		if err != nil {
			return nil, err
		}
		req.Source = &ingestSource{Format: "arrow_ipc", URIs: staged.URIs}
	}
	if !wait {
		if err := t.c.write(ctx, req, nil, true); err != nil {
			return nil, fmt.Errorf("lancego: sending batch %s: %w", batchID, err)
		}
		return &BatchStatus{BatchID: batchID, Status: BatchPending}, nil
	}
	var st BatchStatus
	if err := t.c.write(ctx, req, &st, false); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			// The service rejected the batch: nothing of it was committed.
			return &BatchStatus{BatchID: batchID, Status: BatchFailed, Error: apiErr}, fmt.Errorf("%w: %s: %w", ErrBatchFailed, batchID, err)
		}
		return nil, fmt.Errorf("lancego: batch %s (outcome unknown, check Batch): %w", batchID, err)
	}
	return &st, nil
}

// partitionKeyOf formats partition values given by the caller exactly like Save does.
func (t *Table[T]) partitionKeyOf(values []any) (string, []any, error) {
	row := reflect.New(reflect.TypeFor[T]()).Elem()
	if row.Kind() == reflect.Pointer {
		row = reflect.New(row.Type().Elem()).Elem()
	}
	for i, c := range t.partCol {
		f := row.FieldByIndex(t.layout.columns[c].index)
		v := reflect.ValueOf(values[i])
		if !v.IsValid() || !v.Type().ConvertibleTo(f.Type()) || (v.Kind() == reflect.String) != (f.Kind() == reflect.String) {
			return "", nil, fmt.Errorf("lancego: partition value %v (%T) does not fit column %s (%s)",
				values[i], values[i], t.def.PartitionKeys[i], f.Type())
		}
		f.Set(v.Convert(f.Type()))
	}
	typed, key := t.layout.partitionOf(row, t.def.PartitionKeys, t.partCol)
	return key, typed, nil
}

// checkNilRows rejects nil pointers in a slice of pointer rows.
func checkNilRows[T any](rows []T) error {
	if reflect.TypeFor[T]().Kind() != reflect.Pointer {
		return nil
	}
	for i := range rows {
		if reflect.ValueOf(rows[i]).IsNil() {
			return fmt.Errorf("lancego: row %d is a nil pointer", i)
		}
	}
	return nil
}
