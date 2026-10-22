package lancego

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
)

// cliTransport runs the service locally: each invocation executes the lance-cli binary
// (service/src/bin/cli.rs), which handles the request against the real bucket and ledger
// exactly like the Lambda would. Uploads still go to S3. Enabled by Config.LocalCLI;
// meant for development and tests.
type cliTransport struct {
	bin  string
	logs bool      // copy the service logs (stderr) to our stderr
	s3   transport // putObject only
}

func (c *cliTransport) invoke(ctx context.Context, _ string, payload []byte, _ bool) ([]byte, error) {
	cmd := exec.CommandContext(ctx, c.bin, string(payload))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if c.logs {
		os.Stderr.Write(stderr.Bytes())
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w: %s", c.bin, err, stderr.String())
	}
	return out, nil
}

func (c *cliTransport) putObject(ctx context.Context, bucket, key, contentType string, body []byte) error {
	return c.s3.putObject(ctx, bucket, key, contentType, body)
}
