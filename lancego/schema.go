package lancego

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// TagName is the struct tag read by this package.
//
//	type Sale struct {
//		ID       int64      `lance:"id,index=btree"`
//		Region   string     `lance:"region,sort=1,index=bitmap"`
//		Amount   *float64   `lance:"*"`                 // column "amount", nullable
//		Note     string     `lance:"note,compress=zstd"`
//		Internal string                                  // no tag: not a column
//	}
//
// The first tag element is the column name, or "*" to use the Go field name converted
// to snake_case (CustomerID -> customer_id). Column names must match ^[a-z][a-z0-9_]{0,62}$.
// Options:
//   - index=<type>   build a scalar index on the column (btree, bitmap, zonemap,
//     bloomfilter, label_list, ngram, inverted)
//   - sort=<n>       position of the column in the table's sort key (1 = first)
//   - compress=<c>   per-column compression hint for the service (e.g. zstd)
//
// Untagged fields and fields tagged "-" are ignored. An untagged embedded struct has
// its fields promoted, like encoding/json does.
const TagName = "lance"

// IndexType is a Lance scalar index type, as named in the service API.
type IndexType string

const (
	IndexBTree       IndexType = "BTREE"
	IndexBitmap      IndexType = "BITMAP"
	IndexZoneMap     IndexType = "ZONEMAP"
	IndexBloomFilter IndexType = "BLOOMFILTER"
	IndexLabelList   IndexType = "LABEL_LIST"
	IndexNGram       IndexType = "NGRAM"
	IndexInverted    IndexType = "INVERTED"
)

var indexTypes = map[string]IndexType{
	"btree": IndexBTree, "bitmap": IndexBitmap, "zonemap": IndexZoneMap,
	"bloomfilter": IndexBloomFilter, "label_list": IndexLabelList,
	"ngram": IndexNGram, "inverted": IndexInverted,
}

// IndexSpec is one requested index, as sent in create_table.
type IndexSpec struct {
	Column string    `json:"column"`
	Type   IndexType `json:"type"`
	Name   string    `json:"name,omitempty"`
}

// appender appends one Go value to an Arrow builder.
type appender func(b array.Builder, v reflect.Value)

type column struct {
	field  arrow.Field
	index  []int // reflect field index path from the row struct
	append appender
}

// layout is everything derived from a row type's tags. Built once per type.
type layout struct {
	schema           *arrow.Schema
	columns          []column
	sortKey          []string
	indexes          []IndexSpec
	fieldCompression map[string]string
}

var layouts sync.Map // reflect.Type -> *layout

var timeType = reflect.TypeFor[time.Time]()

// SchemaOf returns the Arrow schema derived from T's lance tags.
func SchemaOf[T any]() (*arrow.Schema, error) {
	l, err := layoutOf(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	return l.schema, nil
}

func layoutOf(t reflect.Type) (*layout, error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if cached, ok := layouts.Load(t); ok {
		return cached.(*layout), nil
	}
	if t.Kind() != reflect.Struct || t == timeType {
		return nil, fmt.Errorf("lancego: row type must be a struct, got %s", t)
	}

	fields, err := structColumns(t, nil, t.Name())
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("lancego: %s has no fields tagged %q", t, TagName)
	}

	l := &layout{fieldCompression: map[string]string{}}
	arrowFields := make([]arrow.Field, len(fields))
	type sortCol struct {
		pos  int
		name string
	}
	var sorts []sortCol
	sortPositions := map[int]string{}
	for i, f := range fields {
		arrowFields[i] = f.col.field
		l.columns = append(l.columns, f.col)
		name := f.col.field.Name
		if f.tag.sort > 0 {
			if prev, dup := sortPositions[f.tag.sort]; dup {
				return nil, fmt.Errorf("lancego: %s: columns %q and %q both use sort=%d", t, prev, name, f.tag.sort)
			}
			sortPositions[f.tag.sort] = name
			sorts = append(sorts, sortCol{f.tag.sort, name})
		}
		if f.tag.index != "" {
			l.indexes = append(l.indexes, IndexSpec{Column: name, Type: f.tag.index})
		}
		if f.tag.compress != "" {
			l.fieldCompression[name] = f.tag.compress
		}
	}
	sort.Slice(sorts, func(i, j int) bool { return sorts[i].pos < sorts[j].pos })
	for _, s := range sorts {
		l.sortKey = append(l.sortKey, s.name)
	}
	l.schema = arrow.NewSchema(arrowFields, nil)

	actual, _ := layouts.LoadOrStore(t, l)
	return actual.(*layout), nil
}

type tagInfo struct {
	name     string
	index    IndexType
	sort     int
	compress string
}

type taggedColumn struct {
	col column
	tag tagInfo
}

// structColumns collects the tagged fields of t (promoting untagged embedded structs).
// prefix is the index path of t inside the row struct; where names t in error messages.
func structColumns(t reflect.Type, prefix []int, where string) ([]taggedColumn, error) {
	var out []taggedColumn
	seen := map[string]bool{}
	for i := range t.NumField() {
		sf := t.Field(i)
		path := append(append([]int(nil), prefix...), i)
		raw, tagged := sf.Tag.Lookup(TagName)

		if !tagged && sf.Anonymous {
			et := sf.Type
			if et.Kind() == reflect.Pointer {
				return nil, fmt.Errorf("lancego: %s.%s: embedded pointer structs are not supported; embed by value or tag it", where, sf.Name)
			}
			if et.Kind() == reflect.Struct && et != timeType {
				inner, err := structColumns(et, path, where+"."+sf.Name)
				if err != nil {
					return nil, err
				}
				for _, c := range inner {
					if seen[c.col.field.Name] {
						return nil, fmt.Errorf("lancego: %s: duplicate column %q", where, c.col.field.Name)
					}
					seen[c.col.field.Name] = true
					out = append(out, c)
				}
			}
			continue
		}
		if !tagged || raw == "-" {
			continue
		}
		if !sf.IsExported() {
			return nil, fmt.Errorf("lancego: %s.%s: tagged field must be exported", where, sf.Name)
		}
		tag, err := parseTag(raw, sf.Name)
		if err != nil {
			return nil, fmt.Errorf("lancego: %s.%s: %w", where, sf.Name, err)
		}
		if seen[tag.name] {
			return nil, fmt.Errorf("lancego: %s: duplicate column %q", where, tag.name)
		}
		seen[tag.name] = true

		dt, nullable, app, err := arrowType(sf.Type, where+"."+sf.Name)
		if err != nil {
			return nil, err
		}
		out = append(out, taggedColumn{
			col: column{field: arrow.Field{Name: tag.name, Type: dt, Nullable: nullable}, index: path, append: app},
			tag: tag,
		})
	}
	return out, nil
}

func parseTag(raw, fieldName string) (tagInfo, error) {
	parts := strings.Split(raw, ",")
	var t tagInfo
	switch name := strings.TrimSpace(parts[0]); name {
	case "":
		return t, fmt.Errorf(`empty column name in tag %q (use "*" for the field name)`, raw)
	case "*":
		t.name = snakeCase(fieldName)
	default:
		t.name = name
	}
	if !columnNameRe.MatchString(t.name) {
		return t, fmt.Errorf("invalid column name %q (must match %s)", t.name, columnNameRe)
	}
	for _, opt := range parts[1:] {
		key, val, _ := strings.Cut(strings.TrimSpace(opt), "=")
		switch key {
		case "index":
			it, ok := indexTypes[strings.ToLower(val)]
			if !ok {
				return t, fmt.Errorf("unknown index type %q", val)
			}
			t.index = it
		case "sort":
			n, err := strconv.Atoi(val)
			if err != nil || n < 1 {
				return t, fmt.Errorf("sort must be a positive integer, got %q", val)
			}
			t.sort = n
		case "compress":
			if val == "" {
				return t, fmt.Errorf("compress needs a value")
			}
			t.compress = val
		default:
			return t, fmt.Errorf("unknown tag option %q", key)
		}
	}
	return t, nil
}

// columnNameRe is the service's column naming rule (plan §3.3).
var columnNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// snakeCase converts a Go field name to snake_case, keeping acronyms together:
// CustomerID -> customer_id, HTTPServer -> http_server, UserIDs -> user_ids, SoldAt2 -> sold_at2.
func snakeCase(s string) string {
	r := []rune(s)
	var b strings.Builder
	for i, c := range r {
		if unicode.IsUpper(c) && i > 0 {
			prev := r[i-1]
			nextLower := i+1 < len(r) && unicode.IsLower(r[i+1])
			// A plural acronym ("IDs") keeps its trailing "s": it ends the word.
			if nextLower && r[i+1] == 's' && (i+2 == len(r) || unicode.IsUpper(r[i+2])) {
				nextLower = false
			}
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower) {
				b.WriteByte('_')
			}
		}
		b.WriteRune(unicode.ToLower(c))
	}
	return b.String()
}

// arrowType maps a Go type to an Arrow type, its nullability and an appender.
// Pointers, byte slices and slices are nullable (nil -> null); everything else is not.
func arrowType(t reflect.Type, where string) (arrow.DataType, bool, appender, error) {
	switch {
	case t.Kind() == reflect.Pointer:
		if t.Elem().Kind() == reflect.Pointer {
			return nil, false, nil, fmt.Errorf("lancego: %s: pointer to pointer is not supported", where)
		}
		dt, _, inner, err := arrowType(t.Elem(), where)
		if err != nil {
			return nil, false, nil, err
		}
		return dt, true, func(b array.Builder, v reflect.Value) {
			if v.IsNil() {
				b.AppendNull()
				return
			}
			inner(b, v.Elem())
		}, nil

	case t == timeType:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, false,
			func(b array.Builder, v reflect.Value) {
				b.(*array.TimestampBuilder).AppendTime(v.Interface().(time.Time))
			}, nil

	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		return arrow.BinaryTypes.Binary, true, func(b array.Builder, v reflect.Value) {
			if v.IsNil() {
				b.AppendNull()
				return
			}
			b.(*array.BinaryBuilder).Append(v.Bytes())
		}, nil

	case t.Kind() == reflect.Slice || t.Kind() == reflect.Array:
		elemType, elemNullable, elem, err := arrowType(t.Elem(), where+"[]")
		if err != nil {
			return nil, false, nil, err
		}
		isSlice := t.Kind() == reflect.Slice
		dt := arrow.ListOfField(arrow.Field{Name: "item", Type: elemType, Nullable: elemNullable})
		return dt, isSlice, func(b array.Builder, v reflect.Value) {
			lb := b.(*array.ListBuilder)
			if isSlice && v.IsNil() {
				lb.AppendNull()
				return
			}
			lb.Append(true)
			vb := lb.ValueBuilder()
			for i := range v.Len() {
				elem(vb, v.Index(i))
			}
		}, nil

	case t.Kind() == reflect.Struct:
		children, err := structColumns(t, nil, where)
		if err != nil {
			return nil, false, nil, err
		}
		if len(children) == 0 {
			return nil, false, nil, fmt.Errorf("lancego: %s: struct %s has no fields tagged %q", where, t, TagName)
		}
		fields := make([]arrow.Field, len(children))
		for i, c := range children {
			fields[i] = c.col.field
		}
		return arrow.StructOf(fields...), false, func(b array.Builder, v reflect.Value) {
			sb := b.(*array.StructBuilder)
			sb.Append(true)
			for i, c := range children {
				c.col.append(sb.FieldBuilder(i), v.FieldByIndex(c.col.index))
			}
		}, nil
	}

	dt, app := scalarType(t.Kind())
	if dt == nil {
		return nil, false, nil, fmt.Errorf("lancego: %s: unsupported type %s", where, t)
	}
	return dt, false, app, nil
}

func scalarType(k reflect.Kind) (arrow.DataType, appender) {
	switch k {
	case reflect.Bool:
		return arrow.FixedWidthTypes.Boolean, func(b array.Builder, v reflect.Value) { b.(*array.BooleanBuilder).Append(v.Bool()) }
	case reflect.Int8:
		return arrow.PrimitiveTypes.Int8, func(b array.Builder, v reflect.Value) { b.(*array.Int8Builder).Append(int8(v.Int())) }
	case reflect.Int16:
		return arrow.PrimitiveTypes.Int16, func(b array.Builder, v reflect.Value) { b.(*array.Int16Builder).Append(int16(v.Int())) }
	case reflect.Int32:
		return arrow.PrimitiveTypes.Int32, func(b array.Builder, v reflect.Value) { b.(*array.Int32Builder).Append(int32(v.Int())) }
	case reflect.Int64, reflect.Int:
		return arrow.PrimitiveTypes.Int64, func(b array.Builder, v reflect.Value) { b.(*array.Int64Builder).Append(v.Int()) }
	case reflect.Uint8:
		return arrow.PrimitiveTypes.Uint8, func(b array.Builder, v reflect.Value) { b.(*array.Uint8Builder).Append(uint8(v.Uint())) }
	case reflect.Uint16:
		return arrow.PrimitiveTypes.Uint16, func(b array.Builder, v reflect.Value) { b.(*array.Uint16Builder).Append(uint16(v.Uint())) }
	case reflect.Uint32:
		return arrow.PrimitiveTypes.Uint32, func(b array.Builder, v reflect.Value) { b.(*array.Uint32Builder).Append(uint32(v.Uint())) }
	case reflect.Uint64, reflect.Uint:
		return arrow.PrimitiveTypes.Uint64, func(b array.Builder, v reflect.Value) { b.(*array.Uint64Builder).Append(v.Uint()) }
	case reflect.Float32:
		return arrow.PrimitiveTypes.Float32, func(b array.Builder, v reflect.Value) { b.(*array.Float32Builder).Append(float32(v.Float())) }
	case reflect.Float64:
		return arrow.PrimitiveTypes.Float64, func(b array.Builder, v reflect.Value) { b.(*array.Float64Builder).Append(v.Float()) }
	case reflect.String:
		return arrow.BinaryTypes.String, func(b array.Builder, v reflect.Value) { b.(*array.StringBuilder).Append(v.String()) }
	}
	return nil, nil
}
