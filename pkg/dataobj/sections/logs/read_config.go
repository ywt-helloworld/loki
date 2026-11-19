package logs

import (
	"fmt"
	"iter"
	"maps"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
)

// readConfig holds the request that a section reader serves. It lists the streams to match, the
// predicates to apply and the columns to return.
//
// [RowReader] and [BatchReader] embed it, so both read the same rows for the same request.
type readConfig struct {
	matchIDs   map[int64]struct{}
	predicates []RowPredicate

	// Column projection. When projected is false, every recognized column is read.
	projected    bool
	projTypes    map[ColumnType]struct{}
	projMetadata map[string]struct{}
}

// clear clears the stream match, the predicates and the projection.
func (c *readConfig) clear() {
	clear(c.matchIDs)
	c.predicates = nil

	c.projected = false
	clear(c.projTypes)
	clear(c.projMetadata)
}

// matchStreams adds ids to the streams to match.
func (c *readConfig) matchStreams(ids iter.Seq[int64]) {
	if c.matchIDs == nil {
		c.matchIDs = make(map[int64]struct{})
	}
	for id := range ids {
		c.matchIDs[id] = struct{}{}
	}
}

// project selects the columns to return. It returns an error when the selection is empty.
func (c *readConfig) project(types []ColumnType, metadataNames []string) error {
	if len(types) == 0 && len(metadataNames) == 0 {
		return fmt.Errorf("projection must select at least one column")
	}

	c.projTypes = make(map[ColumnType]struct{}, len(types))
	for _, t := range types {
		c.projTypes[t] = struct{}{}
	}
	c.projMetadata = make(map[string]struct{}, len(metadataNames))
	for _, name := range metadataNames {
		c.projMetadata[name] = struct{}{}
	}
	c.projected = true
	return nil
}

// readerOptions returns the options of the dataset reader for sec, built from the config. It also
// returns the section columns that the options project, in the same order. A reader uses this
// order. A reader matches the values of a row, or the arrays of a record batch, to these columns by
// position.
func (c *readConfig) readerOptions(sec *Section) (dataset.RowReaderOptions, []*Column, error) {
	dset, err := sec.makeDataset()
	if err != nil {
		return dataset.RowReaderOptions{}, nil, fmt.Errorf("creating section dataset: %w", err)
	}

	// datasetColumns and sectionColumns are parallel: datasetColumns[i] describes the same
	// column as sectionColumns[i]. The projection below filters both together, so they stay
	// parallel. A reader finds the value of a column by its position.
	datasetColumns, sectionColumns := dset.Columns(), sec.Columns()

	// The stream match and the predicates look their columns up among all the columns. They work
	// whether or not the projection lists those columns. The reader returns only the projected
	// columns.
	projectedDatasetColumns, projectedSectionColumns := datasetColumns, sectionColumns
	if c.projected {
		projectedDatasetColumns, projectedSectionColumns = projectColumns(datasetColumns, sectionColumns, c.projTypes, c.projMetadata)

		// The section holds none of the projected columns, so every record would be empty. Report
		// it here instead of reading rows that hold nothing.
		if len(projectedDatasetColumns) == 0 {
			return dataset.RowReaderOptions{}, nil, fmt.Errorf("none of the projected columns are present in the section")
		}
	}

	// The matched stream IDs are not part of c.predicates, so build them as a separate
	// predicate. RowReaderOptions.Predicates are ANDed together.
	var predicates []dataset.Predicate
	if p := streamIDPredicate(maps.Keys(c.matchIDs), datasetColumns, sectionColumns); p != nil {
		predicates = append(predicates, p)
	}

	for _, predicate := range c.predicates {
		p := translateLogsPredicate(predicate, datasetColumns, sectionColumns)

		// A constant predicate that keeps every row constrains nothing, so drop it. One that
		// drops every row stays. The dataset reader prunes it to an empty row range, so the
		// read ends without touching object storage.
		if keep, ok := dataset.IsConstPredicate(p); ok && keep {
			continue
		}

		predicates = append(predicates, p)
	}

	return dataset.RowReaderOptions{
		Dataset:           dset,
		ProjectedColumns:  projectedDatasetColumns,
		Predicates:        predicates,
		PrefetchAllOnOpen: true,
	}, projectedSectionColumns, nil
}
