package lancego

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// apiServer simulates the service's /v1/lance: the key, then an envelope per op.
func apiServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/lance" {
			http.Error(w, "Bad Gateway", http.StatusBadGateway) // not the service: no envelope
			return
		}
		if r.Header.Get("Authorization") != "Bearer secret" {
			w.WriteHeader(http.StatusUnauthorized)
			io.WriteString(w, `{"error":{"code":"UNAUTHORIZED","message":"missing or wrong API key"},"ok":false,"request_id":"r1"}`)
			return
		}
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)
		switch req["op"] {
		case "query":
			io.WriteString(w, `{"request_id":"r2","ok":true,"result":{"columns":[],"rows":[{"n":3}],"row_count":1,"truncated":false,"stats":{}}}`)
		default:
			io.WriteString(w, `{"request_id":"r3","ok":false,"error":{"code":"READ_ONLY","message":"writes"}}`)
		}
	}))
}

func endpointClient(t *testing.T, url, key string) *Client {
	t.Helper()
	c, err := New(context.Background(), Config{Endpoint: url + "/", APIKey: key})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestEndpointQuery(t *testing.T) {
	srv := apiServer(t)
	defer srv.Close()
	res, err := endpointClient(t, srv.URL, "secret").Query(context.Background(), "SELECT 3 AS n", QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 1 || !strings.Contains(string(res.Rows), `"n":3`) {
		t.Fatalf("got %+v", res)
	}
}

func TestEndpointErrors(t *testing.T) {
	srv := apiServer(t)
	defer srv.Close()
	ctx := context.Background()

	// A wrong key is the service's own error, decoded like any other.
	_, err := endpointClient(t, srv.URL, "wrong").Query(ctx, "SELECT 1", QueryOptions{})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "UNAUTHORIZED" || apiErr.RequestID != "r1" {
		t.Fatalf("wrong key: %v", err)
	}
	// A write op is answered READ_ONLY by the service.
	c := endpointClient(t, srv.URL, "secret")
	err = c.invoke(ctx, c.cfg.IngestFunction, map[string]string{"op": "replace_partitions"}, nil)
	if !errors.As(err, &apiErr) || apiErr.Code != "READ_ONLY" {
		t.Fatalf("write op: %v", err)
	}
	// Uploads and async requests never leave the client.
	if err := c.tr.putObject(ctx, "b", "k", "t", nil); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("putObject: %v", err)
	}
	if _, err := c.tr.invoke(ctx, "", nil, true); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("async invoke: %v", err)
	}
	// Something in front of the service (no envelope) keeps its status.
	bad := endpointClient(t, srv.URL+"/other", "secret")
	if _, err := bad.Query(ctx, "SELECT 1", QueryOptions{}); err == nil || !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("non-envelope: %v", err)
	}
}

func TestEndpointConfig(t *testing.T) {
	if _, err := New(context.Background(), Config{Endpoint: "https://x"}); err == nil {
		t.Fatal("Endpoint without APIKey accepted")
	}
	c := endpointClient(t, "https://x.lambda-url.us-west-2.on.aws", "k")
	if tr := c.tr.(*httpTransport); tr.url != "https://x.lambda-url.us-west-2.on.aws/v1/lance" {
		t.Fatalf("url %q", tr.url)
	}
}
