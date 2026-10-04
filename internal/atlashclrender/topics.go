package atlashclrender

import (
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
)

// reportTopics names every YDB topic the document leaves out. Atlas HCL has
// no block for a topic, and Ptah does not invent one, so each is a loss the
// export says out loud: `ptah schema export --cleanup-go-annotations` refuses
// to delete annotations a loss diagnostic names.
func (r *renderer) reportTopics() {
	for _, topic := range r.db.Topics {
		r.warn("topics."+topic.QualifiedName(), "a YDB topic is not represented in HCL")
	}
}

// topicsNotDescribed records that the document does not describe topics, so
// applying it back does not read their absence as a request to drop them.
//
// The record is made for every YDB render, whether or not the schema holds a
// topic, for the reason the omitted Atlas blocks are recorded: the document
// read against another YDB database cannot say that one holds none. A render
// for another dialect records it only when the schema holds a topic, which a
// declaration written for YDB can and a read of another engine cannot.
func (r *renderer) topicsNotDescribed() coverage.Set {
	if platform.NormalizeDialect(r.dialect) != platform.YDB && (r.db == nil || len(r.db.Topics) == 0) {
		return coverage.Set{}
	}
	return coverage.Set{}.With(coverage.Object{
		Kind:       coverage.Topic,
		Reason:     coverage.Unsupported,
		Provenance: coverage.Defaulted,
	})
}
