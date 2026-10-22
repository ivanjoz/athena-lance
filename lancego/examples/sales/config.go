package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/ivanjoz/athena-lance/lancego"
)

// This example plays the backend: it reads its configuration from the repo's config.toml
// ($LANCE_CONFIG, or the first one found walking up from the working directory) and hands
// it to lancego. A real backend fills lancego.Config from wherever it keeps its settings.
type repoConfig struct {
	AWS struct {
		Profile string `toml:"profile"`
		Region  string `toml:"region"`
	} `toml:"aws"`
	Storage struct {
		Bucket        string `toml:"bucket"`
		StagingPrefix string `toml:"staging_prefix"`
	} `toml:"storage"`
	Lambda struct {
		ControlFunction string `toml:"control_function"`
		IngestFunction  string `toml:"ingest_function"`
	} `toml:"lambda"`
	Ingest struct {
		BatchRows          int `toml:"batch_rows"`
		MaxRowsPerRequest  int `toml:"max_rows_per_request"`
		WaitTimeoutSeconds int `toml:"wait_timeout_seconds"`
	} `toml:"ingest"`
}

// loadConfig builds the lancego configuration. LANCEGO_CLI=<path to lance-cli> runs the
// service locally instead of Lambda; LANCEGO_CLI_LOGS=1 shows its logs.
func loadConfig() (lancego.Config, error) {
	path, err := findConfig()
	if err != nil {
		return lancego.Config{}, err
	}
	var rc repoConfig
	if _, err := toml.DecodeFile(path, &rc); err != nil {
		return lancego.Config{}, fmt.Errorf("reading %s: %w", path, err)
	}
	return lancego.Config{
		Region:            rc.AWS.Region,
		Profile:           rc.AWS.Profile,
		ControlFunction:   rc.Lambda.ControlFunction,
		IngestFunction:    rc.Lambda.IngestFunction,
		Bucket:            rc.Storage.Bucket,
		StagingPrefix:     rc.Storage.StagingPrefix,
		BatchRows:         rc.Ingest.BatchRows,
		MaxRowsPerRequest: rc.Ingest.MaxRowsPerRequest,
		WaitTimeout:       time.Duration(rc.Ingest.WaitTimeoutSeconds) * time.Second,
		LocalCLI:          os.Getenv("LANCEGO_CLI"),
		LocalCLILogs:      os.Getenv("LANCEGO_CLI_LOGS") != "",
	}, nil
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
			return "", fmt.Errorf("config.toml not found (set LANCE_CONFIG)")
		}
		dir = parent
	}
}
