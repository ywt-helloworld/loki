package logs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/columnar"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/memory"
)

var errBatchReaderNotOpen = errors.New("batch reader not opened")

// A Batch holds the records that [BatchReader.Read] read from a logs section, as one array per
// column.
//
// The arrays use memory from the allocator that the caller passed to Read. They stay valid until
// the caller reclaims that allocator. The Batch and its Metadata slice are Go values, so they do
// not use that memory.
type Batch struct {
	// Rows is the number of records in the batch. It is at least one, and every array has Rows
	// elements.
	Rows int

	// StreamID, Timestamp and Message hold the columns of type [ColumnTypeStreamID],
	// [ColumnTypeTimestamp] and [ColumnTypeMessage]. A field is nil when the reader did not
	// project its column, or when the section has no such column.
	//
	// Timestamp holds nanoseconds since the Unix epoch. StreamID and Timestamp have no null
	// element. A null element of Message means that the record has no message. It reads as an
	// empty line, as in [DecodeRow].
	StreamID  *columnar.Number[int64]
	Timestamp *columnar.Number[int64]
	Message   *columnar.UTF8

	// Metadata holds the projected metadata columns, in the order of the section.
	Metadata []MetadataColumn
}

// A MetadataColumn holds the values that one structured metadata key has in a [Batch]. Values is
// never nil. A null element means that the record has no value for the key. An empty element
// means that the record has the key with an empty value.
type MetadataColumn struct {
	Name   string
	Values *columnar.UTF8
}

// MetadataLabels returns the structured metadata of record i as labels. It skips a null element
// and keeps an empty one, as [DecodeRow] does.
//
// The labels own their strings, so they stay valid after the caller reclaims the memory of the
// batch. The index i must be in the range [0, Rows).
func (b *Batch) MetadataLabels(i int) labels.Labels {
	if len(b.Metadata) == 0 {
		return labels.EmptyLabels()
	}

	metadata := newMetadataBuilder()
	defer metadata.release()

	for _, column := range b.Metadata {
		if !column.Values.IsNull(i) {
			metadata.add(column.Name, column.Values.Get(i))
		}
	}
	return metadata.labels()
}

// A BatchReader reads the records of a [Section] as [Batch] values.
//
// With the same stream match, predicates and projection, it returns the same records as a
// [RowReader], in the same order. It returns the values of a column as one array, where a
// RowReader returns one [Record] for each row.
type BatchReader struct {
	sec   *Section
	ready bool

	readConfig

	reader *dataset.RowReader

	// projSectionColumns are the projected columns, in the same order as the arrays of each record
	// batch that the dataset reader returns. It holds every recognized column when no projection
	// is set.
	projSectionColumns []*Column

	// metadataColumns is the number of projected metadata columns. newBatch uses it to size
	// Batch.Metadata.
	metadataColumns int
}

// NewBatchReader creates a new BatchReader that reads from the provided [Section].
//
// Call [BatchReader.Open] before calling [BatchReader.Read].
func NewBatchReader(sec *Section) *BatchReader {
	var r BatchReader
	r.Reset(sec)
	return &r
}

// Open initializes BatchReader resources.
//
// Open must be called before [BatchReader.Read]. Open is safe to call multiple times. Open is a
// no-op when the reader has no section.
func (r *BatchReader) Open(ctx context.Context) error {
	if r.sec == nil || r.ready {
		return nil
	}

	if err := r.init(ctx); err != nil {
		return errors.Join(fmt.Errorf("initializing batch reader: %w", err), r.Close())
	}
	return nil
}

// MatchStreams adds the stream IDs of ids to the streams to match. [BatchReader.Read] only
// returns records of the matched streams. When no stream ID was added, the reader matches every
// stream.
//
// MatchStreams may be called multiple times. It returns an error after a successful
// [BatchReader.Open], until [BatchReader.Reset] clears the reader.
func (r *BatchReader) MatchStreams(ids iter.Seq[int64]) error {
	if r.ready {
		return fmt.Errorf("cannot change matched streams after reading has started")
	}

	r.matchStreams(ids)
	return nil
}

// SetPredicates sets the predicates that filter the records. [BatchReader.Read] only returns the
// records that every predicate keeps.
//
// SetPredicates returns an error after a successful [BatchReader.Open], until [BatchReader.Reset]
// clears the reader.
func (r *BatchReader) SetPredicates(p []RowPredicate) error {
	if r.ready {
		return fmt.Errorf("cannot change predicate after reading has started")
	}

	r.predicates = p
	return nil
}

// SetProjectedColumns restricts the [Batch] to the given column types, plus the metadata columns
// named in metadataNames. A [ColumnTypeMetadata] entry in types selects all metadata columns
// instead. The field of a column that is not projected is nil.
//
// The reader reads the columns that a stream match or a predicate needs, even when the caller
// does not list them. It does not return them.
//
// When the section holds none of the projected columns, Open returns an error. When
// SetProjectedColumns is not called, the reader projects every column of the section.
//
// SetProjectedColumns returns an error after a successful [BatchReader.Open], until
// [BatchReader.Reset] clears the reader. It also returns an error when types and metadataNames
// are both empty.
func (r *BatchReader) SetProjectedColumns(types []ColumnType, metadataNames []string) error {
	if r.ready {
		return fmt.Errorf("cannot change projected columns after reading has started")
	}

	return r.project(types, metadataNames)
}

func (r *BatchReader) init(ctx context.Context) error {
	readerOpts, projectedSectionColumns, err := r.readerOptions(r.sec)
	if err != nil {
		return err
	}

	if r.reader == nil {
		r.reader = dataset.NewRowReader(readerOpts)
	} else {
		r.reader.Reset(readerOpts)
	}
	if err := r.reader.Open(ctx); err != nil {
		return fmt.Errorf("opening dataset reader: %w", err)
	}

	r.projSectionColumns = projectedSectionColumns
	r.metadataColumns = 0
	for _, column := range projectedSectionColumns {
		if column.Type == ColumnTypeMetadata {
			r.metadataColumns++
		}
	}
	r.ready = true
	return nil
}

// Read returns the next records of the section that pass the predicates, at most batchSize of
// them. It returns a batch of at least one record, or nil and [io.EOF] at the end of the section.
//
// Read applies the predicates to one run of at most batchSize consecutive records. It skips a run
// in which no record passes. A batch can hold fewer than batchSize records, so a short batch does
// not mean that the section ended.
//
// Read returns nil and the error on any other error. It fails when batchSize is not positive or
// alloc is nil.
//
// The arrays of the batch use memory from alloc, so they stay valid until the caller reclaims
// alloc. Read can also leave temporary memory in alloc, and so can a failed call. The caller should
// reclaim alloc after it consumes each batch.
func (r *BatchReader) Read(ctx context.Context, alloc *memory.Allocator, batchSize int) (*Batch, error) {
	if r.sec == nil {
		return nil, io.EOF
	}
	if !r.ready {
		return nil, errBatchReaderNotOpen
	}

	rb, err := r.reader.ReadRecordBatch(ctx, alloc, batchSize)
	if errors.Is(err, io.EOF) {
		return nil, io.EOF
	} else if err != nil {
		return nil, fmt.Errorf("reading batch: %w", err)
	}

	batch, err := r.newBatch(rb)
	if err != nil {
		return nil, fmt.Errorf("decoding batch: %w", err)
	}
	return batch, nil
}

// newBatch builds a [Batch] from the arrays of rb. It matches each array to its column by
// position, and returns an error when the arrays break the contract of [Batch].
func (r *BatchReader) newBatch(rb *columnar.RecordBatch) (*Batch, error) {
	rows := int(rb.NumRows())
	if rows == 0 {
		return nil, errors.New("record batch has no row")
	}
	if int(rb.NumCols()) != len(r.projSectionColumns) {
		return nil, fmt.Errorf("record batch has %d columns, want %d", rb.NumCols(), len(r.projSectionColumns))
	}

	batch := &Batch{Rows: rows}
	if r.metadataColumns > 0 {
		batch.Metadata = make([]MetadataColumn, 0, r.metadataColumns)
	}
	for i, column := range r.projSectionColumns {
		arr := rb.Column(int64(i))
		if arr.Len() != rows {
			return nil, fmt.Errorf("array of %s has %d elements, want %d", column.Type, arr.Len(), rows)
		}

		switch column.Type {
		case ColumnTypeStreamID, ColumnTypeTimestamp:
			numbers, ok := arr.(*columnar.Number[int64])
			if !ok {
				return nil, fmt.Errorf("invalid array type %T for %s", arr, column.Type)
			}
			if numbers.Nulls() > 0 {
				return nil, fmt.Errorf("array of %s has %d null elements", column.Type, numbers.Nulls())
			}
			if column.Type == ColumnTypeStreamID {
				batch.StreamID = numbers
			} else {
				batch.Timestamp = numbers
			}

		case ColumnTypeMessage, ColumnTypeMetadata:
			values, ok := arr.(*columnar.UTF8)
			if !ok {
				return nil, fmt.Errorf("invalid array type %T for %s", arr, column.Type)
			}
			if column.Type == ColumnTypeMessage {
				batch.Message = values
			} else {
				batch.Metadata = append(batch.Metadata, MetadataColumn{Name: column.Name, Values: values})
			}

		default:
			return nil, fmt.Errorf("unsupported column type %s", column.Type)
		}
	}
	return batch, nil
}

// Reset makes the BatchReader read sec. It clears the predicates, the stream match and the column
// projection. Call [BatchReader.Open] after Reset.
//
// Reset accepts a nil section, which clears the BatchReader.
func (r *BatchReader) Reset(sec *Section) {
	r.sec = sec
	r.ready = false

	r.clear()
	r.projSectionColumns = nil
	r.metadataColumns = 0

	// Keep r.reader. The next Open resets it, so Reset does not allocate a dataset reader.
}

// Close closes the BatchReader and releases any resources it holds. After Close, call
// [BatchReader.Reset] to reuse the BatchReader.
func (r *BatchReader) Close() error {
	if r.reader != nil {
		return r.reader.Close()
	}
	return nil
}
