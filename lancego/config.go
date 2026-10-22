package lancego

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// Config holds the subset of the repo's config.toml that clients need.
type Config struct {
	AWS struct {
		Profile string `toml:"profile"` // empty: default credential chain (e.g. a Lambda role)
		Region  string `toml:"region"`
	} `toml:"aws"`
	Storage struct {
		Bucket        string `toml:"bucket"`
		StagingPrefix string `toml:"staging_prefix"`
	} `toml:"storage"`
	Lambda struct {
		ControlFunction string `toml:"control_function"`
		IngestFunction  string `toml:"ingest_function"` // writes and compaction
		QueryFunction   string `toml:"query_function"`
	} `toml:"lambda"`
	Ingest struct {
		BatchRows          int `toml:"batch_rows"`
		MaxRowsPerRequest  int `toml:"max_rows_per_request"`
		WaitTimeoutSeconds int `toml:"wait_timeout_seconds"`
	} `toml:"ingest"`
}

// LoadConfig reads config.toml from path. With an empty path it uses $LANCE_CONFIG,
// or else the first config.toml found walking up from the working directory
// (the same lookup the Rust service uses).
func LoadConfig(path string) (Config, error) {
	var cfg Config
	if path == "" {
		var err error
		if path, err = findConfig(); err != nil {
			return cfg, err
		}
	}
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return cfg, fmt.Errorf("lancego: reading %s: %w", path, err)
	}
	return cfg, nil
}

func findConfig() (string, error) {
	if p := os.Getenv("LANCE_CONFIG"); p != "" {
		return p, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		p := filepath.Join(dir, "config.toml")
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("lancego: config.toml not found (set LANCE_CONFIG)")
		}
		dir = parent
	}
}
