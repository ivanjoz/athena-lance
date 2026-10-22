package lancego

import (
	"errors"
	"time"
)

// Config is everything the client needs to reach the Lance service. The application fills
// it from its own configuration and passes it to New / NewWithAWS: the library reads no
// configuration files or environment variables.
type Config struct {
	// Region of the functions and the bucket, used by New to load AWS credentials.
	// NewWithAWS takes it from the aws.Config instead.
	Region string
	// Profile is an optional AWS shared-config profile for New (local development).
	// Empty: the default credential chain (environment, ECS / EC2 / Lambda role).
	Profile string

	// Lambda function names (or ARNs). Both are required.
	ControlFunction string // create_table, table_info, partitions, batch status
	IngestFunction  string // writes, compaction, and query / explain

	// Bucket is the S3 Express directory bucket of the service; write batches are uploaded
	// under StagingPrefix (default "staging") before the service commits them. Required.
	Bucket        string
	StagingPrefix string

	// BatchRows is the number of rows per Arrow record batch in uploaded files. Default 65536.
	BatchRows int
	// MaxRowsPerRequest caps the rows of one write request (one Lance commit); a partition
	// is never split. Default 1,000,000.
	MaxRowsPerRequest int
	// WaitTimeout bounds Wait (and Save with NoWait followed by Wait) when ctx has no
	// deadline. Default 15 minutes.
	WaitTimeout time.Duration

	// LocalCLI, for development and tests: the path of the service's lance-cli binary. When
	// set, requests run through it locally instead of invoking Lambda (uploads still go to S3).
	LocalCLI string
	// LocalCLILogs copies the service logs of LocalCLI runs to stderr.
	LocalCLILogs bool
}

// Defaults applied by New / NewWithAWS to zero fields.
const (
	DefaultStagingPrefix     = "staging"
	DefaultBatchRows         = 65536
	DefaultMaxRowsPerRequest = 1_000_000
	DefaultWaitTimeout       = 15 * time.Minute
)

// withDefaults validates the required fields and fills the optional ones.
func (c Config) withDefaults() (Config, error) {
	var missing []error
	for _, f := range []struct{ name, value string }{
		{"ControlFunction", c.ControlFunction},
		{"IngestFunction", c.IngestFunction},
		{"Bucket", c.Bucket},
	} {
		if f.value == "" {
			missing = append(missing, errors.New("lancego: Config."+f.name+" is required"))
		}
	}
	if err := errors.Join(missing...); err != nil {
		return c, err
	}
	if c.StagingPrefix == "" {
		c.StagingPrefix = DefaultStagingPrefix
	}
	if c.BatchRows <= 0 {
		c.BatchRows = DefaultBatchRows
	}
	if c.MaxRowsPerRequest <= 0 {
		c.MaxRowsPerRequest = DefaultMaxRowsPerRequest
	}
	if c.WaitTimeout <= 0 {
		c.WaitTimeout = DefaultWaitTimeout
	}
	return c, nil
}
