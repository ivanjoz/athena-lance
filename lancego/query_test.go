package lancego

import (
	"encoding/json"
	"testing"
	"time"
)

type queryRow struct {
	ID       int64      `lance:"id"`
	Region   string     `lance:"region"`
	Discount *float64   `lance:"discount"`
	SoldAt   time.Time  `lance:"sold_at"`
	Day      *time.Time `lance:"day"`
	Tags     []string   `lance:"tags"`
	Raw      []byte     `lance:"raw"`
	Geo      struct {
		Lat float64 `lance:"lat"`
	} `lance:"geo"`
	Note string // not a column
}

// Rows as the service renders them (arrow-json with explicit nulls).
const serviceRows = `[
 {"id":1,"region":"PE","discount":0.1,"sold_at":"2026-10-01T00:00:01Z","day":"2026-10-01","tags":["a","b"],"raw":"0aff","geo":{"lat":-12.5},"extra":7},
 {"id":2,"region":"CL","discount":null,"sold_at":"2026-10-02T03:04:05.123456","day":null,"tags":null,"raw":null,"geo":{"lat":null}}
]`

func TestDecodeRows(t *testing.T) {
	rows, err := DecodeRows[queryRow](&QueryResult{Rows: json.RawMessage(serviceRows)})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows", len(rows))
	}
	r := rows[0]
	if r.ID != 1 || r.Region != "PE" || r.Discount == nil || *r.Discount != 0.1 {
		t.Fatalf("row 0 scalars: %+v", r)
	}
	if !r.SoldAt.Equal(time.Date(2026, 10, 1, 0, 0, 1, 0, time.UTC)) || r.Day == nil || r.Day.Day() != 1 {
		t.Fatalf("row 0 times: %v %v", r.SoldAt, r.Day)
	}
	if len(r.Tags) != 2 || r.Tags[1] != "b" || len(r.Raw) != 2 || r.Raw[1] != 0xff || r.Geo.Lat != -12.5 {
		t.Fatalf("row 0 nested: %+v", r)
	}
	r = rows[1]
	if r.Discount != nil || r.Day != nil || r.Tags != nil || r.Raw != nil {
		t.Fatalf("row 1 nulls: %+v", r)
	}
	if !r.SoldAt.Equal(time.Date(2026, 10, 2, 3, 4, 5, 123456000, time.UTC)) {
		t.Fatalf("naive timestamp: %v", r.SoldAt)
	}
}

func TestDecodeRowsAggregate(t *testing.T) {
	type total struct {
		Region string  `lance:"region"`
		N      int64   `lance:"n"`
		Total  float64 `lance:"total"`
	}
	res := &QueryResult{Rows: json.RawMessage(`[{"region":"PE","n":3,"total":4.5}]`)}
	rows, err := DecodeRows[*total](res)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Region != "PE" || rows[0].N != 3 || rows[0].Total != 4.5 {
		t.Fatalf("got %+v", rows[0])
	}
}

// cache_seconds is left out at 0 (the service default, 5 minutes) and sent as -1 for CacheOff.
func TestQueryRequestCacheSeconds(t *testing.T) {
	for _, c := range []struct {
		seconds int
		want    string
	}{
		{0, `{"op":"query","sql":"SELECT 1","format":"json"}`},
		{CacheOff, `{"op":"query","sql":"SELECT 1","format":"json","cache_seconds":-1}`},
		{60, `{"op":"query","sql":"SELECT 1","format":"json","cache_seconds":60}`},
	} {
		b, err := json.Marshal(queryRequest{Op: "query", SQL: "SELECT 1", Format: "json", CacheSeconds: c.seconds})
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != c.want {
			t.Fatalf("cache %d: got %s", c.seconds, b)
		}
	}
	var res QueryResult
	if err := json.Unmarshal([]byte(`{"stats":{"cache_hit":true,"cache_age_ms":1200}}`), &res); err != nil {
		t.Fatal(err)
	}
	if !res.Stats.CacheHit || res.Stats.CacheAgeMS != 1200 {
		t.Fatalf("stats: %+v", res.Stats)
	}
}

func TestSaveResultVersion(t *testing.T) {
	r := SaveResult{Batches: []BatchStatus{{DataVersion: 4, Version: 5}, {DataVersion: 7}}}
	if v := r.Version(); v != 7 {
		t.Fatalf("Version() = %d, want 7", v)
	}
}
