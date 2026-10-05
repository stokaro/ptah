package atlashclrender

import (
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
)

// reportReplications names every YDB async replication and transfer the
// document leaves out. Atlas HCL has no block for either, and Ptah does not
// invent one, so each is a loss the export says out loud: `ptah schema export
// --cleanup-go-annotations` refuses to delete annotations a loss diagnostic
// names.
func (r *renderer) reportReplications() {
	for _, replication := range r.db.AsyncReplications {
		r.warn("async_replications."+replication.QualifiedName(), "a YDB async replication is not represented in HCL")
	}
	for _, transfer := range r.db.Transfers {
		r.warn("transfers."+transfer.QualifiedName(), "a YDB transfer is not represented in HCL")
	}
}

// replicationsNotDescribed records that the document does not describe async
// replications and transfers, so applying it back does not read their absence
// as a request to drop them -- for a replication, with its replica tables.
//
// The record is made for every YDB render, whether or not the schema holds
// one, for the reason the omitted Atlas blocks are recorded: the document
// read against another YDB database cannot say that one holds none. A render
// for another dialect records it only when the schema holds one, which a
// declaration written for YDB can and a read of another engine cannot.
func (r *renderer) replicationsNotDescribed() coverage.Set {
	holds := r.db != nil && (len(r.db.AsyncReplications) > 0 || len(r.db.Transfers) > 0)
	if platform.NormalizeDialect(r.dialect) != platform.YDB && !holds {
		return coverage.Set{}
	}
	return coverage.Set{}.With(
		coverage.Object{Kind: coverage.Replication, Reason: coverage.Unsupported, Provenance: coverage.Defaulted},
		coverage.Object{Kind: coverage.Transfer, Reason: coverage.Unsupported, Provenance: coverage.Defaulted},
	)
}
