package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// mirrorDeclared is a replication as a declaration states it, and
// mirrorRead the same one as the YDB reader reports it: the level in its
// default left out, and the source by the path relative to its database.
var (
	mirrorDeclared = schemamodel.AsyncReplication{Name: "mirror", Spec: ast.AsyncReplicationSpec{
		Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136?database=/prod",
			TokenSecretName: "token"},
		Items:            []ast.AsyncReplicationItem{{Source: "/prod/accounts", Target: "replica/accounts"}},
		ConsistencyLevel: "ROW",
	}}
	mirrorRead = catalog.AsyncReplication{Name: "mirror", State: catalog.ReplicationRunning,
		Spec: ast.AsyncReplicationSpec{
			Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod",
				TokenSecretName: "token"},
			Items: []ast.AsyncReplicationItem{{Source: "accounts", Target: "replica/accounts"}},
		}}
	ingestDeclared = schemamodel.Transfer{Name: "ingest", Spec: ast.TransferSpec{Source: "events/feed",
		Target: "event_log", Lambda: "($m) -> { return []; }\n", FlushInterval: "PT1M"}}
	ingestRead = catalog.Transfer{Name: "ingest", State: catalog.ReplicationRunning, Spec: ast.TransferSpec{
		Source: "events/feed", Target: "event_log", Lambda: "($m) -> { return []; }",
		Consumer: "fbc17198-8229c5ec-37e45ba-fe47b6c1"}}
)

// replicationDeclaration declares the given replications and transfers.
func replicationDeclaration(replications []schemamodel.AsyncReplication, transfers ...schemamodel.Transfer) *schemamodel.Database {
	return &schemamodel.Database{AsyncReplications: replications, Transfers: transfers}
}

// replicationCatalog is a database holding the given replications and
// transfers.
func replicationCatalog(replications []catalog.AsyncReplication, transfers ...catalog.Transfer) *catalog.Database {
	return &catalog.Database{AsyncReplications: replications, Transfers: transfers}
}

// TestCompare_YDBReplicationReadsBackAsDeclared holds a replication and a
// transfer the database holds as declared equal to their declarations, and a
// document equal to itself: neither plans anything.
func TestCompare_YDBReplicationReadsBackAsDeclared(t *testing.T) {
	declaration := replicationDeclaration([]schemamodel.AsyncReplication{mirrorDeclared}, ingestDeclared)
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "against the database", diff: must.Must(schemadiff.CompareWithDialect(t.Context(), declaration,
			replicationCatalog([]catalog.AsyncReplication{mirrorRead}, ingestRead), platform.YDB, must.Must(builtin.New())))},
		{name: "against the same document", diff: must.Must(schemadiff.CompareSchemas(t.Context(), declaration, declaration, platform.YDB, must.Must(builtin.New())))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.diff.HasChanges(), qt.IsFalse)
		})
	}
}

// TestCompare_YDBReplicationChange lists what is added, removed and changed,
// each change with both operands and the state the database reports, and
// carries every replication and transfer of each side for the plan.
func TestCompare_YDBReplicationChange(t *testing.T) {
	c := qt.New(t)
	moved := mirrorDeclared
	moved.Spec = mirrorDeclared.Spec.Clone()
	moved.Spec.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	added := schemamodel.Transfer{Name: "archive", Schema: "dr", Spec: ast.TransferSpec{Source: "t/f", Target: "a",
		Lambda: "($m) -> { return []; }"}}
	stale := catalog.AsyncReplication{Name: "old", Schema: "dr", State: catalog.ReplicationDone,
		Spec: mirrorRead.Spec.Clone()}
	desired := replicationDeclaration([]schemamodel.AsyncReplication{moved}, added)
	current := replicationCatalog([]catalog.AsyncReplication{mirrorRead, stale}, ingestRead)

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.AsyncReplicationsAdded, qt.HasLen, 0)
	c.Assert(diff.AsyncReplicationsRemoved.Names(), qt.DeepEquals, []string{"dr.old"})
	c.Assert(diff.AsyncReplicationsModified, qt.DeepEquals, []difftypes.AsyncReplicationDiff{{
		Name: "mirror", ConnectionChanged: true, Desired: moved.Spec, Current: mirrorRead.Spec,
		State: catalog.ReplicationRunning,
	}})
	c.Assert(diff.TransfersAdded.Names(), qt.DeepEquals, []string{"dr.archive"})
	c.Assert(diff.TransfersRemoved.Names(), qt.DeepEquals, []string{"ingest"})
	c.Assert(diff.TransfersModified, qt.HasLen, 0)
	c.Assert(diff.Replications, qt.DeepEquals, difftypes.ReplicationContext{
		CurrentReplications:  []catalog.AsyncReplication{mirrorRead, stale},
		CurrentTransfers:     []catalog.Transfer{ingestRead},
		DeclaredReplications: []schemamodel.AsyncReplication{moved},
		DeclaredTransfers:    []schemamodel.Transfer{added},
	})
}

// TestCompare_YDBReplicationCoverage reads each side's silence through what it
// says it does not describe: a replication the read could not describe, as
// on a cluster serving no replication API, is neither created nor dropped,
// and one the declaration does not describe is kept.
func TestCompare_YDBReplicationCoverage(t *testing.T) {
	c := qt.New(t)
	unread := replicationCatalog(nil)
	unread.NotDescribed = coverage.Set{}.With(coverage.Object{Kind: coverage.Replication, Name: "mirror",
		Reason: coverage.Unsupported, Provenance: coverage.Observed})
	undeclared := replicationDeclaration(nil)
	undeclared.NotDescribed = coverage.Set{}.WithKind(coverage.Replication).WithKind(coverage.Transfer)

	opts := config.DefaultCompareOptions()
	opts.Dialect = platform.YDB

	withheld, undecided, err := schemadiff.CompareReportingUndecidedAdditions(
		t.Context(), replicationDeclaration([]schemamodel.AsyncReplication{mirrorDeclared}), unread, opts, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	kept := must.Must(schemadiff.CompareWithDialect(t.Context(), undeclared,
		replicationCatalog([]catalog.AsyncReplication{mirrorRead}, ingestRead), platform.YDB, must.Must(builtin.New())))

	c.Assert(withheld.HasChanges(), qt.IsFalse)
	c.Assert(undecided.Common, qt.DeepEquals, []coverage.Object{{Kind: coverage.Replication, Name: "mirror",
		Reason: coverage.Unsupported, Provenance: coverage.Observed}})
	c.Assert(kept.HasChanges(), qt.IsFalse)
}

// transferFeed is the changefeed a transfer reads, as declared.
var transferFeed = ydbschema.ChangefeedSpec{Name: "feed", Mode: "UPDATES", Format: "JSON"}

// transferDeclaration declares table events with transferFeed and a transfer
// that reads it through no consumer it names.
func transferDeclaration() *schemamodel.Database {
	db := changefeedDeclaration(transferFeed)
	db.Transfers = []schemamodel.Transfer{{Name: "ingest", Spec: ast.TransferSpec{Source: "events/feed",
		Target: "events", Lambda: "($m) -> { return []; }"}}}
	return db
}

// transferCatalog is the database the declaration made, the changefeed
// holding the given consumers, the transfer reading it through the consumer
// YDB created for it.
func transferCatalog(consumers ...ydbtopic.ConsumerSpec) *catalog.Database {
	read := transferFeed
	read.Consumers = consumers
	db := changefeedCatalog(read)
	db.Transfers = []catalog.Transfer{{Name: "ingest", State: catalog.ReplicationRunning,
		Spec: ast.TransferSpec{Source: "events/feed", Target: "events", Lambda: "($m) -> { return []; }",
			Consumer: "fbc17198"}}}
	return db
}

// generated is the consumer YDB created for the transfer, and audit one
// nothing reads through.
var (
	generated = ydbtopic.ConsumerSpec{Name: "fbc17198", ReadFrom: "1970-01-01T00:00:00Z"}
	audit     = ydbtopic.ConsumerSpec{Name: "audit", ReadFrom: "1970-01-01T00:00:00Z"}
)

// TestCompare_YDBTransferConsumerIsAdopted keeps the consumer YDB created for
// a transfer on the changefeed the transfer reads: no declaration can name
// it, and dropping it would stop the transfer. The declaration passed in is
// not modified.
func TestCompare_YDBTransferConsumerIsAdopted(t *testing.T) {
	c := qt.New(t)
	desired := transferDeclaration()

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, transferCatalog(generated), platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(must.Must(ydbschema.DesiredChangefeeds(desired.FeatureObjects, "", "events")), qt.DeepEquals, []ydbschema.ChangefeedSpec{transferFeed})
}

func TestTransferConsumerAdoptionPreservesReplicationBinding(t *testing.T) {
	c := qt.New(t)
	desired, current := transferDeclaration(), transferCatalog(generated)
	ref := ydbschema.ChangefeedRef("", "events", transferFeed.Name)
	observed := &ydbschema.ObservedChangefeed{Spec: transferFeed.Clone(),
		Replication: &ydbschema.ReplicationBinding{DestinationPath: "/remote/replica", ItemID: "1"}}
	retained := observed.Desired()
	observed.Spec.Consumers = []ydbtopic.ConsumerSpec{generated}
	var err error
	desired.FeatureObjects, err = schemaext.NewObjects(schemaext.Object{Ref: ref, Value: retained})
	c.Assert(err, qt.IsNil)
	current.FeatureObjects, err = schemaext.NewObjects(schemaext.Object{Ref: ref, Value: observed})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, platform.YDB, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	unchanged, _, err := desired.FeatureObjects.Get(ref)
	c.Assert(err, qt.IsNil)
	c.Assert(unchanged.Value.Equal(retained), qt.IsTrue)
}

func TestTableChangeCapturesRetainedReplicationState(t *testing.T) {
	c := qt.New(t)
	desired, current := changefeedDeclaration(), changefeedCatalog()
	desired.Fields = append(desired.Fields, schemamodel.Field{StructName: "Event", Name: "extra", Type: "TEXT", Nullable: true})
	observed := &ydbschema.ObservedChangefeed{Spec: transferFeed.Clone(),
		Replication: &ydbschema.ReplicationBinding{DestinationPath: "/remote/replica", ItemID: "1"}}
	ref := ydbschema.ChangefeedRef("", "events", transferFeed.Name)
	var err error
	current.FeatureObjects, err = schemaext.NewObjects(schemaext.Object{Ref: ref, Value: observed})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, platform.YDB, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, 0)
	captured, found, err := diff.TablesModified[0].Desired.OwnedObjects.Get(ref)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(captured.Value.Equal(observed.Desired()), qt.IsTrue)
}

// TestCompare_YDBTransferConsumerAdoptionKeepsOthersCompared still compares a
// consumer of the same changefeed no transfer reads through: the plan drops
// it, and keeps the transfer's.
func TestCompare_YDBTransferConsumerAdoptionKeepsOthersCompared(t *testing.T) {
	c := qt.New(t)
	adopted := transferFeed
	adopted.Consumers = []ydbtopic.ConsumerSpec{generated}
	held := transferFeed
	held.Consumers = []ydbtopic.ConsumerSpec{generated, audit}

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), transferDeclaration(), transferCatalog(generated, audit), platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{{Subject: ydbschema.ChangefeedRef("", "events", held.Name), Value: &ydbdiff.Changefeed{Before: &ydbschema.ObservedChangefeed{Spec: held}, After: &ydbschema.DesiredChangefeed{Spec: adopted}}}})
}

// TestCompare_YDBTransferConsumerIsAdoptedOnATopic keeps the consumer YDB
// created for a transfer of a standalone topic on the declared topic, as it
// does on a changefeed.
func TestCompare_YDBTransferConsumerIsAdoptedOnATopic(t *testing.T) {
	c := qt.New(t)
	transfer := ast.TransferSpec{Source: "app/events", Target: "event_log", Lambda: "($m) -> { return []; }"}
	desired := &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{}))),
		FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
		Transfers:       []schemamodel.Transfer{{Name: "ingest", Spec: transfer}},
	}
	held := transfer
	held.Consumer = "fbc17198"
	current := &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(ydbtopic.ObservedObject("app", "events", ydbtopic.Spec{
			Consumers: []ydbtopic.ConsumerSpec{generated}}))),
		FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
		Transfers:       []catalog.Transfer{{Name: "ingest", State: catalog.ReplicationRunning, Spec: held}},
	}

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	declared, _, err := desired.FeatureObjects.Get(ydbtopic.Ref("app", "events"))
	c.Assert(err, qt.IsNil)
	c.Assert(declared.Value, qt.DeepEquals, &ydbtopic.Desired{}, qt.Commentf("the adoption must not write through to the declaration"))
	c.Assert(diff.Replications.CurrentTopics, qt.DeepEquals, []string{"app/events"})
	c.Assert(diff.Replications.DeclaredTopics, qt.DeepEquals, []string{"app/events"})
}
