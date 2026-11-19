package logs

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/util/labelpool"
)

// metadataBuilder builds the labels of the structured metadata of one record. [DecodeRow] and
// [Batch.MetadataLabels] share it, so both give a record the same labels.
//
// A metadataBuilder holds a pooled label builder. The caller must call release when it is done.
type metadataBuilder struct {
	builder *labels.ScratchBuilder

	// Sections store metadata columns sorted by name, so the labels usually arrive sorted. The
	// builder tracks the order to skip the sort in that case.
	sorted bool
	last   string
}

func newMetadataBuilder() metadataBuilder {
	return metadataBuilder{builder: labelpool.Get(), sorted: true}
}

// add adds the label name with the given value. An empty value is a label too.
func (b *metadataBuilder) add(name string, value []byte) {
	if name < b.last {
		b.sorted = false
	}
	b.last = name
	b.builder.Add(name, metadataValue(value))
}

// labels returns the labels that add received, sorted by name.
func (b *metadataBuilder) labels() labels.Labels {
	if !b.sorted {
		b.builder.Sort()
	}
	return b.builder.Labels()
}

// release returns the label builder to the pool. The caller must not use the builder after release.
func (b *metadataBuilder) release() { labelpool.Put(b.builder) }
