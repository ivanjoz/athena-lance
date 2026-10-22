package lancego

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Wire types for the Lance service API (plan §4). The Rust service must accept and
// produce exactly these shapes.

type createTableRequest struct {
	Op               string            `json:"op"` // "create_table"
	Table            string            `json:"table"`
	SchemaIPCB64     string            `json:"schema_ipc_b64"`
	PartitionKeys    []string          `json:"partition_keys,omitempty"`
	SortKey          []string          `json:"sort_key,omitempty"`
	Indexes          []IndexSpec       `json:"indexes,omitempty"`
	StorageVersion   string            `json:"storage_version,omitempty"`
	FieldCompression map[string]string `json:"field_compression,omitempty"`
	IfNotExists      bool              `json:"if_not_exists"`
	Replace          bool              `json:"replace,omitempty"`
}

type tableRequest struct {
	Op    string `json:"op"` // "describe_table" | "list_partitions"
	Table string `json:"table"`
}

type ingestRequest struct {
	Op      string       `json:"op"` // "ingest"
	Table   string       `json:"table"`
	BatchID string       `json:"batch_id"`
	Source  ingestSource `json:"source"`
}

type ingestSource struct {
	Format string   `json:"format"` // "arrow_ipc"
	URIs   []string `json:"uris"`
}

// replacePartitionsRequest replaces whole partitions in ONE Lance commit: every row of
// each listed partition is deleted and the rows of that partition in the source file
// are inserted. A partition with rows=0 (or no source at all) is deleted.
type replacePartitionsRequest struct {
	Op         string           `json:"op"` // "replace_partitions"
	Table      string           `json:"table"`
	BatchID    string           `json:"batch_id"`
	Source     *ingestSource    `json:"source,omitempty"`
	Partitions []partitionWrite `json:"partitions"`
}

type partitionWrite struct {
	Key            string `json:"key"`    // "filial_id=1/fecha=2026-10-01"
	Values         []any  `json:"values"` // typed values, in partition_keys order
	ID             int64  `json:"id,omitempty"`
	Hash           Hash   `json:"hash"`
	RecordsUpdated int64  `json:"records_updated,omitempty"`
	Rows           int    `json:"rows"`
}

type batchRequest struct {
	Op      string `json:"op"` // "get_batch"
	Table   string `json:"table"`
	BatchID string `json:"batch_id"`
}

// Hash is an 8-byte partition hash. On the wire it is 16 lowercase hex digits, so no
// JSON implementation can lose precision.
type Hash uint64

func (h Hash) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%016x"`, uint64(h))), nil
}

func (h *Hash) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return fmt.Errorf("invalid hash %q: %w", s, err)
	}
	*h = Hash(v)
	return nil
}

// Batch status values.
const (
	BatchPending   = "pending"
	BatchCommitted = "committed"
	BatchFailed    = "failed"
)

// BatchStatus is the service's record of one write batch (or compaction).
type BatchStatus struct {
	BatchID     string    `json:"batch_id"`
	Op          string    `json:"op,omitempty"`
	Status      string    `json:"status"`
	DataVersion int64     `json:"data_version,omitempty"` // Lance version of the data commit
	Version     int64     `json:"version,omitempty"`      // latest version after the batch (index updates)
	StartedAt   int64     `json:"started_at,omitempty"`   // unix seconds
	CommittedAt int64     `json:"committed_at,omitempty"` // unix seconds
	Rows        int64     `json:"rows,omitempty"`
	Partitions  int64     `json:"partitions,omitempty"`
	Error       *APIError `json:"error,omitempty"`
	// Only in the response to the write itself:
	AlreadyApplied bool  `json:"already_applied,omitempty"` // a retry of a committed batch
	RowsDeleted    int64 `json:"rows_deleted,omitempty"`    // rows of the replaced partitions
}

// PartitionInfo is the registry entry of one partition, as of its last committed write.
type PartitionInfo struct {
	Key            string `json:"key"`
	Values         []any  `json:"values"`
	ID             int64  `json:"id,omitempty"`
	Hash           Hash   `json:"hash"`
	RecordsUpdated int64  `json:"records_updated,omitempty"`
	Rows           int64  `json:"rows"`
	CommittedAt    int64  `json:"committed_at"` // unix seconds
	Version        int64  `json:"version"`
	BatchID        string `json:"batch_id"`
}

type compactRequest struct {
	Op          string `json:"op"` // "compact"
	Table       string `json:"table"`
	OperationID string `json:"operation_id"`
	MaxFileMB   int    `json:"max_file_mb,omitempty"` // 0 = service default
	MaxFiles    int    `json:"max_files,omitempty"`   // 0 = service default
	MaxTasks    int    `json:"max_tasks,omitempty"`   // 0 = service default
}

// CompactResult is the compact response.
type CompactResult struct {
	OperationID string `json:"operation_id"`
	Limits      struct {
		MaxFileBytes int64 `json:"max_file_bytes"`
		MaxFiles     int   `json:"max_files"`
		MaxTasks     int   `json:"max_tasks"`
	} `json:"limits"`
	VersionBefore    int64 `json:"version_before"`
	Version          int64 `json:"version"`
	FragmentsRemoved int   `json:"fragments_removed"`
	FragmentsAdded   int   `json:"fragments_added"`
	FilesRemoved     int   `json:"files_removed"`
	FilesAdded       int   `json:"files_added"`
	More             bool  `json:"more"` // more groups were eligible than MaxTasks: run again
	Cleanup          struct {
		OldVersionsRemoved int64 `json:"old_versions_removed"`
		BytesRemoved       int64 `json:"bytes_removed"`
	} `json:"cleanup"`
	AlreadyApplied bool `json:"already_applied"`
}

type tableInfoRequest struct {
	Op     string `json:"op"` // "table_info"
	Table  string `json:"table"`
	Detail bool   `json:"detail,omitempty"`
}

// TableInfo is the table_info response.
type TableInfo struct {
	Table          string `json:"table"`
	URI            string `json:"uri"`
	Version        int64  `json:"version"`
	VersionTime    int64  `json:"version_time"` // unix seconds
	StorageVersion string `json:"storage_version"`
	Rows           int64  `json:"rows"`
	DeletedRows    int64  `json:"deleted_rows"` // soft-deleted, removed by compaction
	Columns        []struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Nullable bool   `json:"nullable"`
	} `json:"columns"`
	Spec  json.RawMessage `json:"spec"`
	Files struct {
		Fragments     int   `json:"fragments"`
		DataFiles     int   `json:"data_files"`
		DeletionFiles int   `json:"deletion_files"`
		TotalBytes    int64 `json:"total_bytes"`
		MinBytes      int64 `json:"min_bytes"`
		MaxBytes      int64 `json:"max_bytes"`
		AvgBytes      int64 `json:"avg_bytes"`
		UnknownSize   int   `json:"unknown_size"`
		SizeBuckets   []struct {
			Range     string `json:"range"`
			Fragments int    `json:"fragments"`
			Bytes     int64  `json:"bytes"`
		} `json:"size_buckets"`
	} `json:"files"`
	Storage struct {
		DataObjects       int   `json:"data_objects"`
		DataBytes         int64 `json:"data_bytes"`
		UnreferencedFiles int   `json:"unreferenced_files"` // not used by the current version
		UnreferencedBytes int64 `json:"unreferenced_bytes"`
	} `json:"storage"`
	Versions struct {
		Count           int   `json:"count"`
		Oldest          int64 `json:"oldest"`
		OldestTime      int64 `json:"oldest_time"`
		RetentionHours  int   `json:"retention_hours"`
		CleanupEligible int   `json:"cleanup_eligible"`
	} `json:"versions"`
	Indexes struct {
		Built []struct {
			Name               string   `json:"name"`
			Type               string   `json:"type"`
			Columns            []string `json:"columns"`
			Segments           int      `json:"segments"`
			FragmentsIndexed   int      `json:"fragments_indexed"`
			FragmentsUnindexed int      `json:"fragments_unindexed"`
		} `json:"built"`
		Missing []string `json:"missing"`
	} `json:"indexes"`
	Compaction struct {
		Groups      int   `json:"groups"`
		Fragments   int   `json:"fragments"`
		Bytes       int64 `json:"bytes"`
		More        bool  `json:"more"`
		Recommended bool  `json:"recommended"`
	} `json:"compaction"`
	Registry struct {
		Partitions        int   `json:"partitions"`
		Rows              int64 `json:"rows"`
		OldestCommittedAt int64 `json:"oldest_committed_at"`
		NewestCommittedAt int64 `json:"newest_committed_at"`
	} `json:"registry"`
	Operations struct {
		Lock *struct {
			Owner     string `json:"owner"`
			Op        string `json:"op"`
			ExpiresAt int64  `json:"expires_at"` // unix ms
			Held      bool   `json:"held"`
		} `json:"lock"`
		Pending int           `json:"pending"`
		Failed  int           `json:"failed"`
		Recent  []BatchStatus `json:"recent"`
	} `json:"operations"`
	// Detail (detail=true): per-fragment files and min/max, pruning and partition spread.
	Detail json.RawMessage `json:"detail,omitempty"`
}

type queryRequest struct {
	Op          string           `json:"op"` // "query" | "explain"
	SQL         string           `json:"sql"`
	Format      string           `json:"format"` // "json"
	MaxRows     int              `json:"max_rows,omitempty"`
	TimeoutMS   int              `json:"timeout_ms,omitempty"`
	MinVersions map[string]int64 `json:"min_versions,omitempty"`
}

// envelope is the common response wrapper.
type envelope struct {
	RequestID string          `json:"request_id"`
	OK        bool            `json:"ok"`
	Result    json.RawMessage `json:"result"`
	Error     *APIError       `json:"error"`
}

// APIError is an error returned by the service.
type APIError struct {
	Code      string          `json:"code"`
	Message   string          `json:"message"`
	Details   json.RawMessage `json:"details,omitempty"`
	RequestID string          `json:"-"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("lance service: %s: %s (request %s)", e.Code, e.Message, e.RequestID)
}

// CreateTableResult is the create_table response.
type CreateTableResult struct {
	Created bool            `json:"created"`
	Table   json.RawMessage `json:"table"` // table descriptor, as in describe_table
}

// QueryStats are the per-query statistics the service reports.
type QueryStats struct {
	ElapsedMS       float64          `json:"elapsed_ms"`
	PlanMS          float64          `json:"plan_ms"`
	ExecMS          float64          `json:"exec_ms"`
	BytesRead       int64            `json:"bytes_read"`
	GetRequests     int64            `json:"get_requests"`
	ColdStart       bool             `json:"cold_start"`
	DatasetVersions map[string]int64 `json:"dataset_versions"`
}

// QueryResult is the query response. Rows is a JSON array of objects keyed by column name,
// with every column present (null included); decode it with DecodeRows or Table.Query.
type QueryResult struct {
	Columns []struct {
		Name     string `json:"name"`
		Type     string `json:"type"` // Arrow type, e.g. "Int64", "Utf8", "Timestamp(µs, \"UTC\")"
		Nullable bool   `json:"nullable"`
	} `json:"columns"`
	Rows     json.RawMessage `json:"rows"`
	RowCount int             `json:"row_count"`
	// Truncated: the query had more rows than MaxRows, or they did not fit in the response
	// service's response size limit. Add a LIMIT or aggregate more.
	Truncated bool       `json:"truncated"`
	Stats     QueryStats `json:"stats"`
	Plan      string     `json:"plan,omitempty"` // explain only
}
