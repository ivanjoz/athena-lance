package lancego

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ErrReadOnly is returned by a client built with Config.Endpoint for what only the direct
// Lambda invoke can do: uploading write batches and asynchronous requests.
var ErrReadOnly = errors.New("lancego: read-only client (Config.Endpoint); writes need the direct Lambda invoke")

// apiPath is the service's read-only JSON API on the ingest Function URL.
const apiPath = "v1/lance"

// httpTransport sends each request to the service's read-only JSON API with the API key. The
// body and the response are the same envelope as a direct invoke, so the client works unchanged;
// the service rejects ops that write (READ_ONLY).
type httpTransport struct {
	url    string
	key    string
	client *http.Client
}

func newHTTPTransport(endpoint, key string) *httpTransport {
	// No client timeout: ctx bounds the call, and the service cuts a query at 30 s anyway.
	return &httpTransport{url: strings.TrimSuffix(endpoint, "/") + "/" + apiPath, key: key, client: &http.Client{}}
}

func (h *httpTransport) invoke(ctx context.Context, _ string, payload []byte, async bool) ([]byte, error) {
	if async {
		return nil, ErrReadOnly
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.key)
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading the response (HTTP %d): %w", resp.StatusCode, err)
	}
	// The service answers its own errors (bad key, bad request) as an envelope too; anything
	// else that is not 200 comes from in front of it (Lambda, the Function URL).
	if resp.StatusCode != http.StatusOK && !isEnvelope(body) {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(body, 500))
	}
	return body, nil
}

func isEnvelope(body []byte) bool {
	var env envelope
	return json.Unmarshal(body, &env) == nil && env.RequestID != ""
}

func (h *httpTransport) putObject(context.Context, string, string, string, []byte) error {
	return ErrReadOnly
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}
