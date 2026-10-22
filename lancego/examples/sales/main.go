// Example: declare a partitioned table (like a GlueTable) and save rows by partition.
//
//	go run ./examples/sales                    # stage only: upload the Arrow file to S3
//	go run ./examples/sales -create -save      # create_table, then Save (waits for the commit)
//	go run ./examples/sales -save -change 7    # only the partition of row 7 is sent again
//	go run ./examples/sales -delete 1/2026-10-01 -partitions
//	go run ./examples/sales -compact -info -detail
//	go run ./examples/sales -query -explain    # SQL on lance-query, typed rows, EXPLAIN ANALYZE
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ivanjoz/athena-lance/lancego"
)

type Venta struct {
	FilialID   int16     `lance:"filial_id"`
	Fecha      string    `lance:"fecha"`
	ID         int64     `lance:"id,index=btree"`
	Region     string    `lance:"region,index=bitmap"`
	CustomerID string    `lance:"*,index=btree"` // column customer_id
	Amount     float64   `lance:"amount"`
	Discount   *float64  `lance:"*"` // column discount, nullable
	SoldAt     time.Time `lance:"sold_at,index=zonemap"`
	Note       string    // not a column
}

// GetParquetID identifies the partition with one integer (optional), as in the smartberry
// datalake: filial (3 digits) + yyyymmdd.
func (v Venta) GetParquetID() int64 {
	t, _ := time.Parse("2006-01-02", v.Fecha)
	return int64(v.FilialID)*100_000_000 + int64(t.Year()*10000+int(t.Month())*100+t.Day())
}

var VentasDef = lancego.TableDef{
	Name:          "ventas",
	PartitionKeys: []string{"filial_id", "fecha"},
	SortKey:       []string{"region", "sold_at"},
}

func main() {
	create := flag.Bool("create", false, "call create_table on lance-control")
	save := flag.Bool("save", false, "Save by partition and wait for the commit")
	change := flag.Int("change", -1, "modify row N before saving (only its partition is sent)")
	del := flag.String("delete", "", "delete one partition: filial/fecha, e.g. 1/2026-10-01")
	compact := flag.Bool("compact", false, "compact (50 MB / 20 files)")
	partitions := flag.Bool("partitions", false, "print the partition registry")
	info := flag.Bool("info", false, "print table_info")
	detail := flag.Bool("detail", false, "table_info with per-file stats")
	query := flag.Bool("query", false, "run example queries on lance-query")
	explain := flag.Bool("explain", false, "print EXPLAIN ANALYZE of an indexed lookup")
	n := flag.Int("rows", 1000, "rows to generate")
	flag.Parse()
	ctx := context.Background()

	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	client, err := lancego.New(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	ventas, err := lancego.NewTable[Venta](client, VentasDef)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("schema:\n%s\npartition keys: %v\nsort key: %v\nindexes: %+v\n\n",
		ventas.Schema(), ventas.PartitionKeys(), ventas.SortKey(), ventas.Indexes())

	regions := []string{"PE", "CL", "MX", "US"}
	rows := make([]Venta, *n)
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for i := range rows {
		day := t0.AddDate(0, 0, i%3)
		rows[i] = Venta{
			FilialID:   int16(1 + i%2),
			Fecha:      day.Format("2006-01-02"),
			ID:         int64(i),
			Region:     regions[i%len(regions)],
			CustomerID: fmt.Sprintf("C%05d", i%500),
			Amount:     float64(i%1000) * 1.5,
			SoldAt:     day.Add(time.Duration(i) * time.Second),
		}
		if i%10 == 0 {
			d := 0.1
			rows[i].Discount = &d
		}
	}

	if *create {
		res, err := ventas.Create(ctx, lancego.CreateOptions{IfNotExists: true})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("create_table: created=%v\n", res.Created)
	}

	if *change >= 0 {
		// Change one row: only its partition is sent again on the next save.
		rows[*change].Amount += 1
	}

	// Queries after a Save in this run ask for at least the version it committed.
	minVersions := map[string]int64{}
	if *save {
		start := time.Now()
		res, err := ventas.Save(ctx, rows, lancego.SaveOptions{})
		if err != nil {
			log.Fatal(err)
		}
		show(fmt.Sprintf("save (%s)", time.Since(start).Round(time.Millisecond)), res)
		if v := res.Version(); v > 0 {
			minVersions[ventas.Name()] = v
		}
	}
	if *del != "" {
		var filial int16
		var fecha string
		if _, err := fmt.Sscanf(*del, "%d/%s", &filial, &fecha); err != nil {
			log.Fatalf("-delete wants filial/fecha: %v", err)
		}
		st, err := ventas.DeletePartitions(ctx, []any{filial, fecha})
		if err != nil {
			log.Fatal(err)
		}
		show("delete", st)
	}
	if *compact {
		res, err := ventas.Compact(ctx, lancego.CompactOptions{MaxFileMB: 50, MaxFiles: 20})
		if err != nil {
			log.Fatal(err)
		}
		show("compact", res)
	}
	if *partitions {
		parts, err := ventas.Partitions(ctx)
		if err != nil {
			log.Fatal(err)
		}
		show("partitions", parts)
	}
	if *info {
		ti, err := ventas.Info(ctx, *detail)
		if err != nil {
			log.Fatal(err)
		}
		show("table_info", ti)
	}

	if *query {
		opts := lancego.QueryOptions{MinVersions: minVersions}
		type regionTotal struct {
			Region string  `lance:"region"`
			N      int64   `lance:"n"`
			Total  float64 `lance:"total"`
		}
		start := time.Now()
		res, err := client.Query(ctx, `SELECT region, count(*) AS n, sum(amount) AS total
			FROM ventas WHERE fecha = '2026-10-01' GROUP BY region ORDER BY region`, opts)
		if err != nil {
			log.Fatal(err)
		}
		totals, err := lancego.DecodeRows[regionTotal](res)
		if err != nil {
			log.Fatal(err)
		}
		show(fmt.Sprintf("totals by region (%s)", time.Since(start).Round(time.Millisecond)), totals)
		show("stats", res.Stats)

		start = time.Now()
		found, res, err := ventas.Query(ctx, "SELECT * FROM ventas WHERE customer_id = 'C00007' ORDER BY id", opts)
		if err != nil {
			log.Fatal(err)
		}
		show(fmt.Sprintf("rows of customer C00007 (%s)", time.Since(start).Round(time.Millisecond)), found)
		show("stats", res.Stats)
	}
	if *explain {
		res, err := client.Explain(ctx, "SELECT id, amount FROM ventas WHERE id = 42", lancego.QueryOptions{MinVersions: minVersions})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("explain:\n%s\n", res.Plan)
	}

	if !*create && !*save && *del == "" && !*compact && !*partitions && !*info && !*query && !*explain {
		staged, err := ventas.Stage(ctx, rows, lancego.InsertOptions{})
		if err != nil {
			log.Fatal(err)
		}
		show("staged", staged)
	}
}

func show(title string, v any) {
	out, _ := json.MarshalIndent(v, "", "  ")
	fmt.Printf("%s:\n%s\n", title, out)
}
