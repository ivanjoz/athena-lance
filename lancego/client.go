// Package lancego converts Go structs to Apache Arrow and sends them to the Lance
// service (lance-control / lance-ingest on AWS Lambda).
//
//	c, _ := lancego.New(ctx, lancego.Config{ // filled from the application's own configuration
//		Region: "us-west-2", Bucket: "...--x-s3",
//		ControlFunction: "...", IngestFunction: "...",
//	})
//	sales, _ := lancego.NewTable[Sale](c, lancego.TableDef{Name: "sales", PartitionKeys: []string{"fecha"}})
//	sales.Create(ctx, lancego.CreateOptions{IfNotExists: true})
//	sales.Save(ctx, rows, lancego.SaveOptions{}) // returns once the data is committed
//	res, _ := c.Query(ctx, "SELECT region, count(*) FROM sales GROUP BY region", lancego.QueryOptions{})
package lancego

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Client talks to the Lance service. It is safe for concurrent use.
type Client struct {
	cfg Config
	tr  transport
}

// transport is how the client reaches AWS: awsTransport in production, a fake in tests.
type transport interface {
	// invoke calls a Lambda and returns its response payload. With async it only queues
	// the event (Lambda retries it) and returns no payload.
	invoke(ctx context.Context, function string, payload []byte, async bool) ([]byte, error)
	putObject(ctx context.Context, bucket, key, contentType string, body []byte) error
}

type awsTransport struct {
	s3     *s3.Client
	lambda *lambda.Client
}

func (a *awsTransport) invoke(ctx context.Context, function string, payload []byte, async bool) ([]byte, error) {
	in := &lambda.InvokeInput{FunctionName: &function, Payload: payload}
	if async {
		in.InvocationType = types.InvocationTypeEvent
	}
	resp, err := a.lambda.Invoke(ctx, in)
	if err != nil {
		return nil, err
	}
	if resp.FunctionError != nil {
		return nil, fmt.Errorf("function error (%s): %s", *resp.FunctionError, resp.Payload)
	}
	return resp.Payload, nil
}

func (a *awsTransport) putObject(ctx context.Context, bucket, key, contentType string, body []byte) error {
	_, err := a.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        bytes.NewReader(body),
		ContentType: &contentType,
	})
	return err
}

// New creates a client, loading AWS credentials for cfg.Region (and cfg.Profile, if set)
// from the default credential chain.
func New(ctx context.Context, cfg Config) (*Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(cfg.Profile))
	}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("lancego: loading AWS config: %w", err)
	}
	return NewWithAWS(cfg, awsCfg)
}

// NewWithAWS creates a client from an AWS configuration the application already loaded
// (cfg.Region and cfg.Profile are not used).
func NewWithAWS(cfg Config, awsCfg aws.Config) (*Client, error) {
	var tr transport = &awsTransport{
		s3: s3.NewFromConfig(awsCfg),
		// Writes run up to the function timeout (15 min) before answering.
		lambda: lambda.NewFromConfig(awsCfg, func(o *lambda.Options) {
			o.RetryMaxAttempts = 3
		}),
	}
	if cfg.LocalCLI != "" {
		tr = &cliTransport{bin: cfg.LocalCLI, logs: cfg.LocalCLILogs, s3: tr}
	}
	return newClient(cfg, tr)
}

func newClient(cfg Config, tr transport) (*Client, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Client{cfg: cfg, tr: tr}, nil
}

// QueryOptions tune a query. Zero values use the service defaults.
type QueryOptions struct {
	MaxRows   int
	TimeoutMS int
	// MinVersions is, per table, the oldest Lance version the query may read. Pass the
	// version a write confirmed (SaveResult.Version) to read your own writes: the service
	// refreshes its cached copy of the table instead of waiting for query.freshness_seconds.
	// A cached result computed at an older version is not used either.
	MinVersions map[string]int64
	// CacheSeconds is the max age of a cached result the query may be answered with:
	// 0 = the service default (query.cache_default_seconds, 5 minutes), CacheOff = no cache
	// (the result is neither read from nor written to it), > 0 = that age (capped at
	// query.cache_max_seconds). A write does not invalidate cached results: to read your own
	// write, pass MinVersions or CacheOff. Explain ignores it.
	CacheSeconds int
}

// CacheOff, as QueryOptions.CacheSeconds, runs the query without the result cache.
const CacheOff = -1

// Query runs read-only SQL on lance-ingest. The service cuts it at TimeoutMS (default and cap:
// query.default_timeout_ms / max_timeout_ms, 30 s), well before the function's own timeout.
func (c *Client) Query(ctx context.Context, sql string, opts QueryOptions) (*QueryResult, error) {
	return c.query(ctx, "query", sql, opts)
}

// Explain runs sql with EXPLAIN ANALYZE and returns the plan in Plan (to check scalar
// index use and pruning); Rows is empty.
func (c *Client) Explain(ctx context.Context, sql string, opts QueryOptions) (*QueryResult, error) {
	return c.query(ctx, "explain", sql, opts)
}

func (c *Client) query(ctx context.Context, op, sql string, opts QueryOptions) (*QueryResult, error) {
	var out QueryResult
	req := queryRequest{
		Op: op, SQL: sql, Format: "json", MaxRows: opts.MaxRows, TimeoutMS: opts.TimeoutMS,
		MinVersions: opts.MinVersions, CacheSeconds: opts.CacheSeconds,
	}
	if err := c.invoke(ctx, c.cfg.IngestFunction, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DescribeTable returns the table descriptor (schema, spec, version, indexes) as JSON.
func (c *Client) DescribeTable(ctx context.Context, table string) (json.RawMessage, error) {
	var out json.RawMessage
	err := c.invoke(ctx, c.cfg.ControlFunction, tableRequest{Op: "describe_table", Table: table}, &out)
	return out, err
}

// TableInfo returns the status of a table: version, rows, files and their sizes, unused
// files and versions awaiting cleanup, index coverage, what a compaction would merge,
// the partition registry summary, the table lock and recent / pending / failed operations.
// detail adds the per-file list with min/max of the partition and sort columns (it scans
// those columns, so it is slower on big tables).
func (c *Client) TableInfo(ctx context.Context, table string, detail bool) (*TableInfo, error) {
	var out TableInfo
	req := tableInfoRequest{Op: "table_info", Table: table, Detail: detail}
	if err := c.invoke(ctx, c.cfg.ControlFunction, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListTables returns the names of the existing tables.
func (c *Client) ListTables(ctx context.Context) ([]string, error) {
	var out struct {
		Tables []struct {
			Table string `json:"table"`
		} `json:"tables"`
	}
	if err := c.invoke(ctx, c.cfg.ControlFunction, map[string]string{"op": "list_tables"}, &out); err != nil {
		return nil, err
	}
	names := make([]string, len(out.Tables))
	for i, t := range out.Tables {
		names[i] = t.Table
	}
	return names, nil
}

// write sends a write (ingest, replace_partitions, compact) to the ingest function. The
// service serializes writes per table with a lock and answers after the commit, so a nil
// error means the data is visible. With async the request is only queued by Lambda and
// out is not filled; follow it with Batch / Wait.
// A write whose outcome is unknown (transport error, INTERNAL, LOCK_LOST) is retried with
// the same request: the service applies a batch/operation ID once, and a retry of a batch
// that did commit just finishes its bookkeeping and returns it.
func (c *Client) write(ctx context.Context, req, out any, async bool) error {
	fn := c.cfg.IngestFunction
	if async {
		payload, err := json.Marshal(req)
		if err != nil {
			return err
		}
		if _, err := c.tr.invoke(ctx, fn, payload, true); err != nil {
			return fmt.Errorf("lancego: invoking %s: %w", fn, err)
		}
		return nil
	}
	delay := time.Second
	for attempt := 1; ; attempt++ {
		err := c.invoke(ctx, fn, req, out)
		var apiErr *APIError
		uncertain := err != nil && (!errors.As(err, &apiErr) || apiErr.Code == "INTERNAL" || apiErr.Code == "LOCK_LOST")
		if !uncertain || attempt == writeAttempts || ctx.Err() != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
		delay *= 2
	}
}

const writeAttempts = 3

// invoke calls a Lambda function synchronously and unwraps the response envelope into out.
func (c *Client) invoke(ctx context.Context, function string, req, out any) error {
	if function == "" {
		return fmt.Errorf("lancego: Lambda function name not configured")
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := c.tr.invoke(ctx, function, payload, false)
	if err != nil {
		return fmt.Errorf("lancego: invoking %s: %w", function, err)
	}
	var env envelope
	if err := json.Unmarshal(resp, &env); err != nil {
		return fmt.Errorf("lancego: decoding %s response: %w", function, err)
	}
	if !env.OK {
		if env.Error == nil {
			env.Error = &APIError{Code: "UNKNOWN", Message: "ok=false without error"}
		}
		env.Error.RequestID = env.RequestID
		return env.Error
	}
	if out == nil || len(env.Result) == 0 {
		return nil
	}
	if err := json.Unmarshal(env.Result, out); err != nil {
		return fmt.Errorf("lancego: decoding %s result: %w", function, err)
	}
	return nil
}
