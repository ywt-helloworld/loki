package logs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/columnar"
	"github.com/grafana/loki/v3/pkg/memory"
)

func TestBatchReader_Read(t *testing.T) {
	t.Run("returns the records of the section as one array for each column", func(t *testing.T) {
		section := buildTestProjectionSection(t)

		reader := NewBatchReader(section)
		require.NoError(t, reader.Open(t.Context()))
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		var alloc memory.Allocator
		batch, err := reader.Read(t.Context(), &alloc, 100)
		require.NoError(t, err)

		require.Equal(t, 3, batch.Rows)
		require.Equal(t, []int64{1, 2, 2}, batch.StreamID.Values())

		// The section sorts the rows of a stream by descending timestamp.
		require.Equal(t, []int64{10e9, 30e9, 20e9}, batch.Timestamp.Values())
		require.Equal(t, "line-1", string(batch.Message.Get(0)))
		require.Equal(t, "line-3", string(batch.Message.Get(1)))
		require.Equal(t, "line-2", string(batch.Message.Get(2)))
		require.Len(t, batch.Metadata, 2)
		require.Equal(t, "env", batch.Metadata[0].Name)
		require.Equal(t, "trace_id", batch.Metadata[1].Name)
		require.Equal(t, `{env="prod", trace_id="t1"}`, batch.MetadataLabels(0).String())
		require.Equal(t, `{env="dev", trace_id="t3"}`, batch.MetadataLabels(1).String())
		require.Equal(t, `{env="prod", trace_id="t2"}`, batch.MetadataLabels(2).String())

		_, err = reader.Read(t.Context(), &alloc, 100)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("returns the same records as the row reader", func(t *testing.T) {
		records := randomRecords(rand.New(rand.NewSource(1)), 3000)
		sections := map[string]*Section{
			"sorted by stream":    buildSectionFromRecords(t, SortStreamASC, records),
			"sorted by timestamp": buildSectionFromRecords(t, SortTimestampDESC, records),
		}

		projections := []struct {
			name          string
			types         []ColumnType
			metadataNames []string
		}{
			{name: "every column"},
			{name: "stream ID and timestamp", types: []ColumnType{ColumnTypeStreamID, ColumnTypeTimestamp}},
			{name: "with the message", types: []ColumnType{ColumnTypeStreamID, ColumnTypeTimestamp, ColumnTypeMessage}},
			{name: "with one metadata key", types: []ColumnType{ColumnTypeStreamID, ColumnTypeTimestamp}, metadataNames: []string{"env"}},
			{name: "with every metadata key", types: []ColumnType{ColumnTypeStreamID, ColumnTypeTimestamp, ColumnTypeMetadata}},
			{name: "with a metadata key that the section lacks", types: []ColumnType{ColumnTypeTimestamp}, metadataNames: []string{"missing"}},
		}

		lineHasError := func(line []byte) bool { return bytes.Contains(line, []byte("error")) }
		predicates := []struct {
			name       string
			streams    []int64
			predicates []RowPredicate
		}{
			{name: "no predicate"},
			{name: "a matched stream", streams: []int64{2, 4}},
			{name: "a time range that includes the start", predicates: []RowPredicate{TimeRangeRowPredicate{StartTime: records[100].Timestamp, EndTime: records[2000].Timestamp, IncludeStart: true}}},
			{name: "a time range that includes the end", predicates: []RowPredicate{TimeRangeRowPredicate{StartTime: records[100].Timestamp, EndTime: records[2000].Timestamp, IncludeEnd: true}}},
			{name: "a time range that holds no record", predicates: []RowPredicate{TimeRangeRowPredicate{StartTime: unixSecond(5), EndTime: unixSecond(4), IncludeStart: true}}},
			{name: "a metadata equality", predicates: []RowPredicate{MetadataMatcherRowPredicate{Key: "env", Value: "prod"}}},
			{name: "a metadata equality on an empty value", predicates: []RowPredicate{MetadataMatcherRowPredicate{Key: "env", Value: ""}}},
			{name: "a metadata equality on a missing key", predicates: []RowPredicate{MetadataMatcherRowPredicate{Key: "missing", Value: "x"}}},
			{name: "a metadata filter", predicates: []RowPredicate{MetadataFilterRowPredicate{Key: "region", Keep: func(_, value string) bool { return strings.HasPrefix(value, "eu") }}}},
			{name: "a negated metadata filter", predicates: []RowPredicate{MetadataFilterRowPredicate{Key: "region", Keep: func(_, value string) bool { return value != "us-east" }}}},
			{name: "a line filter", predicates: []RowPredicate{LogMessageFilterRowPredicate{Keep: lineHasError}}},
			{name: "a line filter with a batch function", predicates: []RowPredicate{LogMessageFilterRowPredicate{Keep: lineHasError, KeepBatch: keepBatchOf(lineHasError)}}},
			{name: "a line filter that keeps no line", predicates: []RowPredicate{LogMessageFilterRowPredicate{Keep: func([]byte) bool { return false }, KeepBatch: keepBatchOf(func([]byte) bool { return false })}}},
			{name: "a line filter after a metadata equality", predicates: []RowPredicate{
				MetadataMatcherRowPredicate{Key: "env", Value: "prod"},
				LogMessageFilterRowPredicate{Keep: lineHasError, KeepBatch: keepBatchOf(lineHasError)},
			}},
			{name: "an or of a metadata equality and a line filter", predicates: []RowPredicate{OrRowPredicate{
				Left:  MetadataMatcherRowPredicate{Key: "env", Value: "dev"},
				Right: LogMessageFilterRowPredicate{Keep: lineHasError, KeepBatch: keepBatchOf(lineHasError)},
			}}},
			{name: "a negated line filter", predicates: []RowPredicate{NotRowPredicate{Inner: LogMessageFilterRowPredicate{Keep: lineHasError}}}},
			{name: "streams, a time range, a metadata equality and a line filter", streams: []int64{1, 2, 3}, predicates: []RowPredicate{
				TimeRangeRowPredicate{StartTime: unixSecond(500), EndTime: unixSecond(2500), IncludeStart: true},
				MetadataMatcherRowPredicate{Key: "env", Value: "prod"},
				LogMessageFilterRowPredicate{Keep: lineHasError, KeepBatch: keepBatchOf(lineHasError)},
			}},
		}

		for sectionName, section := range sections {
			for _, projection := range projections {
				for _, predicate := range predicates {
					for _, batchSize := range []int{7} {
						t.Run(fmt.Sprintf("%s/%s/%s/batch=%d", sectionName, projection.name, predicate.name, batchSize), func(t *testing.T) {
							configure := func(r interface {
								SetProjectedColumns([]ColumnType, []string) error
								MatchStreams(iter.Seq[int64]) error
								SetPredicates([]RowPredicate) error
							}) {
								if projection.types != nil || projection.metadataNames != nil {
									require.NoError(t, r.SetProjectedColumns(projection.types, projection.metadataNames))
								}
								if predicate.streams != nil {
									require.NoError(t, r.MatchStreams(slices.Values(predicate.streams)))
								}
								require.NoError(t, r.SetPredicates(predicate.predicates))
							}

							rowReader := NewRowReader(section)
							configure(rowReader)
							require.NoError(t, rowReader.Open(t.Context()))
							want := readAllRecords(t, rowReader)
							require.NoError(t, rowReader.Close())

							batchReader := NewBatchReader(section)
							configure(batchReader)
							require.NoError(t, batchReader.Open(t.Context()))
							got := readAllBatchRecords(t, batchReader, batchSize)
							require.NoError(t, batchReader.Close())

							requireSameRecords(t, want, got)
						})
					}
				}
			}
		}
	})

	t.Run("returns every record in one batch when the batch size exceeds the section", func(t *testing.T) {
		records := randomRecords(rand.New(rand.NewSource(3)), 1000)
		section := buildSectionFromRecords(t, SortStreamASC, records)

		rowReader := NewRowReader(section)
		require.NoError(t, rowReader.Open(t.Context()))
		want := readAllRecords(t, rowReader)
		require.NoError(t, rowReader.Close())

		batchReader := NewBatchReader(section)
		require.NoError(t, batchReader.Open(t.Context()))
		var alloc memory.Allocator
		batch, err := batchReader.Read(t.Context(), &alloc, 100_000)
		require.NoError(t, err)
		require.Equal(t, len(want), batch.Rows)
		require.NoError(t, batchReader.Close())
	})

	t.Run("returns io.EOF at the end of the section and on every later call", func(t *testing.T) {
		section := buildTestSection(t)

		reader := NewBatchReader(section)
		require.NoError(t, reader.Open(t.Context()))
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		var alloc memory.Allocator
		batch, err := reader.Read(t.Context(), &alloc, 10)
		require.NoError(t, err)
		require.Equal(t, 2, batch.Rows)

		for range 3 {
			batch, err = reader.Read(t.Context(), &alloc, 10)
			require.Nil(t, batch)
			require.ErrorIs(t, err, io.EOF)
		}
	})

	t.Run("returns an error before the reader is opened", func(t *testing.T) {
		reader := NewBatchReader(buildTestSection(t))

		var alloc memory.Allocator
		batch, err := reader.Read(t.Context(), &alloc, 10)
		require.Nil(t, batch)
		require.ErrorIs(t, err, errBatchReaderNotOpen)
	})

	t.Run("returns io.EOF for a reader without a section", func(t *testing.T) {
		reader := NewBatchReader(nil)
		require.NoError(t, reader.Open(t.Context()))

		var alloc memory.Allocator
		batch, err := reader.Read(t.Context(), &alloc, 10)
		require.Nil(t, batch)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("returns an error when the batch size is not positive", func(t *testing.T) {
		reader := NewBatchReader(buildTestSection(t))
		require.NoError(t, reader.Open(t.Context()))
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		var alloc memory.Allocator
		_, err := reader.Read(t.Context(), &alloc, 0)
		require.EqualError(t, err, "reading batch: batch size must be positive, got 0")
	})

	t.Run("returns the error of the context when it is done", func(t *testing.T) {
		reader := NewBatchReader(buildTestSection(t))
		require.NoError(t, reader.Open(t.Context()))
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		var alloc memory.Allocator
		_, err := reader.Read(ctx, &alloc, 10)
		require.ErrorIs(t, err, ctx.Err())
	})
}

func TestBatchReader_MatchStreams(t *testing.T) {
	t.Run("adds the streams of every call", func(t *testing.T) {
		reader := NewBatchReader(buildTestProjectionSection(t))
		require.NoError(t, reader.MatchStreams(slices.Values([]int64{1})))
		require.NoError(t, reader.MatchStreams(slices.Values([]int64{2})))
		require.NoError(t, reader.Open(t.Context()))
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		var alloc memory.Allocator
		batch, err := reader.Read(t.Context(), &alloc, 10)
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2, 2}, batch.StreamID.Values())
	})
}

func TestBatchReader_SetProjectedColumns(t *testing.T) {
	section := buildTestProjectionSection(t)

	t.Run("leaves the arrays of the columns that it does not project nil", func(t *testing.T) {
		reader := NewBatchReader(section)
		require.NoError(t, reader.SetProjectedColumns([]ColumnType{ColumnTypeTimestamp}, []string{"env"}))
		require.NoError(t, reader.Open(t.Context()))
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		var alloc memory.Allocator
		batch, err := reader.Read(t.Context(), &alloc, 10)
		require.NoError(t, err)

		require.Nil(t, batch.StreamID)
		require.Nil(t, batch.Message)
		require.NotNil(t, batch.Timestamp)
		require.Len(t, batch.Metadata, 1)
		require.Equal(t, "env", batch.Metadata[0].Name)
	})

	t.Run("reads the columns that a predicate needs without returning them", func(t *testing.T) {
		reader := NewBatchReader(section)
		require.NoError(t, reader.SetProjectedColumns([]ColumnType{ColumnTypeTimestamp}, nil))
		require.NoError(t, reader.SetPredicates([]RowPredicate{MetadataMatcherRowPredicate{Key: "env", Value: "dev"}}))
		require.NoError(t, reader.Open(t.Context()))
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		var alloc memory.Allocator
		batch, err := reader.Read(t.Context(), &alloc, 10)
		require.NoError(t, err)

		require.Equal(t, 1, batch.Rows)
		require.Equal(t, []int64{30e9}, batch.Timestamp.Values())
		require.Empty(t, batch.Metadata)
	})

	t.Run("returns an error when no column is selected", func(t *testing.T) {
		reader := NewBatchReader(section)
		require.EqualError(t, reader.SetProjectedColumns(nil, nil), "projection must select at least one column")
	})

	t.Run("returns an error from Open when the section holds none of the projected columns", func(t *testing.T) {
		reader := NewBatchReader(section)
		require.NoError(t, reader.SetProjectedColumns(nil, []string{"missing"}))
		require.EqualError(t, reader.Open(t.Context()), "initializing batch reader: none of the projected columns are present in the section")
	})

	t.Run("returns an error after the reader is opened", func(t *testing.T) {
		reader := NewBatchReader(section)
		require.NoError(t, reader.Open(t.Context()))
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		require.EqualError(t, reader.SetProjectedColumns([]ColumnType{ColumnTypeTimestamp}, nil), "cannot change projected columns after reading has started")
		require.EqualError(t, reader.SetPredicates(nil), "cannot change predicate after reading has started")
		require.EqualError(t, reader.MatchStreams(slices.Values([]int64{1})), "cannot change matched streams after reading has started")
	})
}

func TestBatchReader_Reset(t *testing.T) {
	section := buildTestProjectionSection(t)

	t.Run("clears the projection, the predicates and the stream match", func(t *testing.T) {
		reader := NewBatchReader(section)
		require.NoError(t, reader.SetProjectedColumns([]ColumnType{ColumnTypeStreamID}, nil))
		require.NoError(t, reader.SetPredicates([]RowPredicate{MetadataMatcherRowPredicate{Key: "env", Value: "dev"}}))
		require.NoError(t, reader.MatchStreams(slices.Values([]int64{1})))
		reader.Reset(section)
		require.NoError(t, reader.Open(t.Context()))
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		var alloc memory.Allocator
		batch, err := reader.Read(t.Context(), &alloc, 10)
		require.NoError(t, err)
		require.Equal(t, 3, batch.Rows)
		require.NotNil(t, batch.Message)
	})

	t.Run("lets a reset reader read another section with another projection", func(t *testing.T) {
		reader := NewBatchReader(buildTestProjectionSection(t))
		require.NoError(t, reader.Open(t.Context()))

		var alloc memory.Allocator
		batch, err := reader.Read(t.Context(), &alloc, 10)
		require.NoError(t, err)
		require.Equal(t, 3, batch.Rows)
		require.NoError(t, reader.Close())

		alloc.Reclaim()
		reader.Reset(buildTestSection(t))
		require.NoError(t, reader.SetProjectedColumns([]ColumnType{ColumnTypeTimestamp}, nil))
		require.NoError(t, reader.Open(t.Context()))
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		batch, err = reader.Read(t.Context(), &alloc, 10)
		require.NoError(t, err)
		require.Equal(t, 2, batch.Rows)
		require.NotNil(t, batch.Timestamp)
		require.Nil(t, batch.StreamID)
		require.Nil(t, batch.Message)
	})
}

func TestBatch_MetadataLabels(t *testing.T) {
	t.Run("returns no labels when the batch holds no metadata column", func(t *testing.T) {
		batch := &Batch{Rows: 1}
		require.True(t, batch.MetadataLabels(0).IsEmpty())
	})

	t.Run("skips a null element and keeps an empty one", func(t *testing.T) {
		alloc := memory.NewAllocator(nil)
		builder := columnar.NewUTF8Builder(alloc)
		builder.AppendNull()
		builder.AppendValue([]byte(""))
		builder.AppendValue([]byte("v"))
		values := builder.BuildArray().(*columnar.UTF8)

		batch := &Batch{Rows: 3, Metadata: []MetadataColumn{{Name: "key", Values: values}}}
		require.True(t, batch.MetadataLabels(0).IsEmpty())
		require.Equal(t, labels.FromStrings("key", ""), batch.MetadataLabels(1))
		require.Equal(t, labels.FromStrings("key", "v"), batch.MetadataLabels(2))
	})

	t.Run("sorts the labels when the columns are not sorted by name", func(t *testing.T) {
		alloc := memory.NewAllocator(nil)
		build := func(value string) *columnar.UTF8 {
			builder := columnar.NewUTF8Builder(alloc)
			builder.AppendValue([]byte(value))
			return builder.BuildArray().(*columnar.UTF8)
		}

		batch := &Batch{Rows: 1, Metadata: []MetadataColumn{{Name: "b", Values: build("2")}, {Name: "a", Values: build("1")}}}
		require.Equal(t, labels.FromStrings("a", "1", "b", "2"), batch.MetadataLabels(0))
	})
}

func TestBatchReader_Open(t *testing.T) {
	t.Run("keeps reading where it was when the reader is already open", func(t *testing.T) {
		reader := NewBatchReader(buildTestSection(t))
		require.NoError(t, reader.Open(t.Context()))
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		var alloc memory.Allocator
		batch, err := reader.Read(t.Context(), &alloc, 1)
		require.NoError(t, err)
		require.Equal(t, 1, batch.Rows)

		require.NoError(t, reader.Open(t.Context()))
		batch, err = reader.Read(t.Context(), &alloc, 1)
		require.NoError(t, err)
		require.Equal(t, 1, batch.Rows)

		_, err = reader.Read(t.Context(), &alloc, 1)
		require.ErrorIs(t, err, io.EOF)
	})
}

func TestBatchReader_newBatch(t *testing.T) {
	newReader := func() *BatchReader {
		return &BatchReader{
			projSectionColumns: []*Column{
				{Type: ColumnTypeStreamID},
				{Type: ColumnTypeTimestamp},
				{Type: ColumnTypeMessage},
				{Type: ColumnTypeMetadata, Name: "env"},
			},
			metadataColumns: 1,
		}
	}

	alloc := memory.NewAllocator(nil)
	numbers := func(values ...int64) columnar.Array {
		builder := columnar.NewNumberBuilder[int64](alloc)
		for _, value := range values {
			builder.AppendValue(value)
		}
		return builder.BuildArray()
	}
	strs := func(values ...string) columnar.Array {
		builder := columnar.NewUTF8Builder(alloc)
		for _, value := range values {
			builder.AppendValue([]byte(value))
		}
		return builder.BuildArray()
	}

	t.Run("sets the field of each column from the array at its position", func(t *testing.T) {
		rb := columnar.NewRecordBatch(nil, 2, []columnar.Array{numbers(1, 2), numbers(10, 20), strs("a", "b"), strs("x", "y")})

		batch, err := newReader().newBatch(rb)
		require.NoError(t, err)

		require.Equal(t, 2, batch.Rows)
		require.Equal(t, []int64{1, 2}, batch.StreamID.Values())
		require.Equal(t, []int64{10, 20}, batch.Timestamp.Values())
		require.Equal(t, "b", string(batch.Message.Get(1)))
		require.Len(t, batch.Metadata, 1)
		require.Equal(t, "env", batch.Metadata[0].Name)
		require.Equal(t, "y", string(batch.Metadata[0].Values.Get(1)))
	})

	t.Run("keeps a null message and a null metadata element", func(t *testing.T) {
		messages := columnar.NewUTF8Builder(alloc)
		messages.AppendNull()
		env := columnar.NewUTF8Builder(alloc)
		env.AppendNull()
		rb := columnar.NewRecordBatch(nil, 1, []columnar.Array{numbers(1), numbers(10), messages.BuildArray(), env.BuildArray()})

		batch, err := newReader().newBatch(rb)
		require.NoError(t, err)
		require.True(t, batch.Message.IsNull(0))
		require.True(t, batch.Metadata[0].Values.IsNull(0))
	})

	t.Run("returns an error when the record batch has no row", func(t *testing.T) {
		rb := columnar.NewRecordBatch(nil, 0, []columnar.Array{numbers(), numbers(), strs(), strs()})

		_, err := newReader().newBatch(rb)
		require.EqualError(t, err, "record batch has no row")
	})

	t.Run("returns an error when the record batch has another number of columns", func(t *testing.T) {
		rb := columnar.NewRecordBatch(nil, 1, []columnar.Array{numbers(1), numbers(10)})

		_, err := newReader().newBatch(rb)
		require.EqualError(t, err, "record batch has 2 columns, want 4")
	})

	t.Run("returns an error when an array has another number of elements than the record batch", func(t *testing.T) {
		rb := columnar.NewRecordBatch(nil, 2, []columnar.Array{numbers(1, 2), numbers(10), strs("a", "b"), strs("x", "y")})

		_, err := newReader().newBatch(rb)
		require.EqualError(t, err, "array of timestamp has 1 elements, want 2")
	})

	t.Run("returns an error when an array has another type than its column", func(t *testing.T) {
		rb := columnar.NewRecordBatch(nil, 1, []columnar.Array{strs("1"), numbers(10), strs("a"), strs("x")})

		_, err := newReader().newBatch(rb)
		require.EqualError(t, err, "invalid array type *columnar.UTF8 for stream_id")
	})

	t.Run("returns an error when the message array is not a UTF8 array", func(t *testing.T) {
		rb := columnar.NewRecordBatch(nil, 1, []columnar.Array{numbers(1), numbers(10), numbers(5), strs("x")})

		_, err := newReader().newBatch(rb)
		require.EqualError(t, err, "invalid array type *columnar.Number[int64] for message")
	})

	t.Run("returns an error when the stream ID array or the timestamp array holds a null", func(t *testing.T) {
		withNull := func() columnar.Array {
			builder := columnar.NewNumberBuilder[int64](alloc)
			builder.AppendNull()
			return builder.BuildArray()
		}

		rb := columnar.NewRecordBatch(nil, 1, []columnar.Array{withNull(), numbers(10), strs("a"), strs("x")})
		_, err := newReader().newBatch(rb)
		require.EqualError(t, err, "array of stream_id has 1 null elements")

		rb = columnar.NewRecordBatch(nil, 1, []columnar.Array{numbers(1), withNull(), strs("a"), strs("x")})
		_, err = newReader().newBatch(rb)
		require.EqualError(t, err, "array of timestamp has 1 null elements")
	})
}

// randomRecords returns n records across six streams, with a time range of about an hour, lines
// that mention a level, empty lines, and metadata that some records lack.
func randomRecords(rng *rand.Rand, n int) []Record {
	var (
		levels  = []string{"info", "warn", "error", "debug"}
		envs    = []string{"prod", "dev", ""}
		regions = []string{"us-east", "eu-west", "eu-north", "ap-south"}
	)

	records := make([]Record, 0, n)
	for i := range n {
		record := Record{
			StreamID:  int64(1 + rng.Intn(6)),
			Timestamp: time.Unix(int64(i)*3600/int64(n), int64(rng.Intn(1_000_000))).UTC(),
			Line:      fmt.Appendf(nil, "level=%s msg=request id=%d", levels[rng.Intn(len(levels))], rng.Intn(1000)),
		}
		if rng.Intn(20) == 0 {
			record.Line = nil
		}

		var pairs []string
		if rng.Intn(4) != 0 {
			pairs = append(pairs, "env", envs[rng.Intn(len(envs))])
		}
		if rng.Intn(3) != 0 {
			pairs = append(pairs, "region", regions[rng.Intn(len(regions))])
		}
		if rng.Intn(10) == 0 {
			pairs = append(pairs, "trace_id", fmt.Sprintf("%016x", rng.Uint64()))
		}
		record.Metadata = labels.FromStrings(pairs...)

		records = append(records, record)
	}
	return records
}

// readAllBatchRecords drains reader into records. It decodes each batch into records and then
// reclaims the memory of the batch, as a caller of the reader does.
func readAllBatchRecords(t *testing.T, reader *BatchReader, batchSize int) []Record {
	t.Helper()

	var (
		out   []Record
		alloc memory.Allocator
	)
	for {
		alloc.Reclaim()
		batch, err := reader.Read(t.Context(), &alloc, batchSize)
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		require.NotZero(t, batch.Rows)
		require.LessOrEqual(t, batch.Rows, batchSize)

		for i := range batch.Rows {
			var record Record
			if batch.StreamID != nil {
				record.StreamID = batch.StreamID.Get(i)
			}
			if batch.Timestamp != nil {
				record.Timestamp = time.Unix(0, batch.Timestamp.Get(i)).UTC()
			}
			if batch.Message != nil {
				record.Line = bytes.Clone(batch.Message.Get(i))
			}
			record.Metadata = batch.MetadataLabels(i)
			out = append(out, record)
		}
	}
}

// requireSameRecords fails the test unless got holds the records of want, in the same order. A
// nil line and an empty line are the same.
func requireSameRecords(t *testing.T, want, got []Record) {
	t.Helper()

	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].StreamID, got[i].StreamID, "record %d: stream ID", i)
		require.Equal(t, want[i].Timestamp.UnixNano(), got[i].Timestamp.UnixNano(), "record %d: timestamp", i)
		require.Equal(t, string(want[i].Line), string(got[i].Line), "record %d: line", i)
		require.True(t, labels.Equal(want[i].Metadata, got[i].Metadata), "record %d: metadata %s and %s", i, want[i].Metadata, got[i].Metadata)
	}
}
