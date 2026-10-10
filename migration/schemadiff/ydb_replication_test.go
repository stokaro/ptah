package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
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
	mirrorDeclared = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136?database=/prod",
			TokenSecretName: "token"},
		Items:            []ydbreplication.Item{{Source: "/prod/accounts", Target: "replica/accounts"}},
		ConsistencyLevel: "ROW",
	}
	mirrorRead = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod",
			TokenSecretName: "token"},
		Items: []ydbreplication.Item{{Source: "accounts", Target: "replica/accounts"}},
	}
	ingestDeclared = ydbreplication.TransferSpec{Source: "events/feed",
		Target: "event_log", Lambda: "($m) -> { return []; }\n", FlushInterval: "PT1M"}
	ingestRead = ydbreplication.TransferSpec{
		Source: "events/feed", Target: "event_log", Lambda: "($m) -> { return []; }",
		Consumer: "fbc17198-8229c5ec-37e45ba-fe47b6c1"}
)

// replicationCoverage claims both namespaces in full.
func replicationCoverage(representation schemaext.Representation) schemaext.Coverage {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	replications := must.Must(ydbreplication.ReplicationCoverage(representation, complete, nil))
	return must.Must(replications.Combine(must.Must(ydbreplication.TransferCoverage(representation, complete, nil))))
}

// replicationDeclaration declares the given replications and transfers and
// claims both namespaces.
func replicationDeclaration(objects ...schemaext.Object) *schemamodel.Database {
	return &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: replicationCoverage(schemaext.Desired)}
}

// replicationCatalog is a database holding the given replications and
// transfers, read in full.
func replicationCatalog(objects ...schemaext.Object) *catalog.Database {
	return &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: replicationCoverage(schemaext.Observed)}
}

// TestCompare_YDBReplicationReadsBackAsDeclared holds a replication and a
// transfer the database holds as declared equal to their declarations, and a
// document equal to itself: neither plans anything.
func TestCompare_YDBReplicationReadsBackAsDeclared(t *testing.T) {
	declaration := replicationDeclaration(ydbreplication.DesiredReplicationObject("", "mirror", "", mirrorDeclared),
		ydbreplication.DesiredTransferObject("", "ingest", "", ingestDeclared))
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "against the database", diff: must.Must(schemadiff.CompareWithDialect(t.Context(), declaration,
			replicationCatalog(ydbreplication.ObservedReplicationObject("", "mirror", mirrorRead, ydbreplication.StateRunning),
				ydbreplication.ObservedTransferObject("", "ingest", ingestRead, ydbreplication.StateRunning)),
			platform.YDB, must.Must(builtin.New())))},
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
// carries every feature object of each side for the plan.
func TestCompare_YDBReplicationChange(t *testing.T) {
	c := qt.New(t)
	moved := mirrorDeclared.Clone()
	moved.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	added := ydbreplication.TransferSpec{Source: "t/f", Target: "a", Lambda: "($m) -> { return []; }"}
	desired := replicationDeclaration(ydbreplication.DesiredReplicationObject("", "mirror", "", moved),
		ydbreplication.DesiredTransferObject("dr", "archive", "", added))
	current := replicationCatalog(ydbreplication.ObservedReplicationObject("", "mirror", mirrorRead, ydbreplication.StateRunning),
		ydbreplication.ObservedReplicationObject("dr", "old", mirrorRead.Clone(), ydbreplication.StateDone),
		ydbreplication.ObservedTransferObject("", "ingest", ingestRead, ydbreplication.StateRunning))

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{
		{Subject: ydbreplication.ReplicationRef("", "mirror"), Value: ydbdiff.NewAsyncReplication(
			&ydbreplication.ObservedReplication{Spec: mirrorRead, State: ydbreplication.StateRunning},
			&ydbreplication.DesiredReplication{Spec: moved})},
		{Subject: ydbreplication.ReplicationRef("dr", "old"), Value: ydbdiff.NewAsyncReplication(
			&ydbreplication.ObservedReplication{Spec: mirrorRead, State: ydbreplication.StateDone}, nil)},
		{Subject: ydbreplication.TransferRef("", "ingest"), Value: ydbdiff.NewTransfer(
			&ydbreplication.ObservedTransfer{Spec: ingestRead, State: ydbreplication.StateRunning}, nil)},
		{Subject: ydbreplication.TransferRef("dr", "archive"), Value: ydbdiff.NewTransfer(
			nil, &ydbreplication.DesiredTransfer{Spec: added})},
	})
	c.Assert(diff.Features.DesiredObjects, qt.DeepEquals, desired.FeatureObjects)
	c.Assert(diff.Features.CurrentObjects, qt.DeepEquals, current.FeatureObjects)
}

// TestCompare_YDBReplicationCoverage reads each side's silence through what it
// says it describes: a replication the read could not describe, as on a
// cluster serving no replication API, is neither created nor dropped, and one
// the declaration does not describe is kept.
func TestCompare_YDBReplicationCoverage(t *testing.T) {
	c := qt.New(t)
	unread := &catalog.Database{FeatureCoverage: must.Must(ydbreplication.ReplicationCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{
			Kind:      ydbreplication.ReplicationKind,
			Subject:   ydbreplication.ReplicationRef("", "mirror"),
			Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbreplication.UnsupportedReplicationReason},
		}}))}
	undeclared := &schemamodel.Database{}

	opts := config.DefaultCompareOptions()
	opts.Dialect = platform.YDB

	withheld, _, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(),
		replicationDeclaration(ydbreplication.DesiredReplicationObject("", "mirror", "", mirrorDeclared)), unread, opts, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	kept := must.Must(schemadiff.CompareWithDialect(t.Context(), undeclared,
		replicationCatalog(ydbreplication.ObservedReplicationObject("", "mirror", mirrorRead, ydbreplication.StateRunning),
			ydbreplication.ObservedTransferObject("", "ingest", ingestRead, ydbreplication.StateRunning)),
		platform.YDB, must.Must(builtin.New())))

	c.Assert(withheld.HasChanges(), qt.IsFalse)
	c.Assert(kept.HasChanges(), qt.IsFalse)
}

// transferFeed is the changefeed a transfer reads, as declared.
var transferFeed = ydbschema.ChangefeedSpec{Name: "feed", Mode: "UPDATES", Format: "JSON"}

// transferDeclaration declares table events with transferFeed and a transfer
// that reads it through no consumer it names.
func transferDeclaration() *schemamodel.Database {
	db := changefeedDeclaration(transferFeed)
	db.FeatureObjects = must.Must(db.FeatureObjects.With(ydbreplication.DesiredTransferObject("", "ingest", "",
		ydbreplication.TransferSpec{Source: "events/feed", Target: "events", Lambda: "($m) -> { return []; }"})))
	db.FeatureCoverage = must.Must(db.FeatureCoverage.Combine(replicationCoverage(schemaext.Desired)))
	return db
}

// transferCatalog is the database the declaration made, the changefeed
// holding the given consumers, the transfer reading it through the consumer
// YDB created for it.
func transferCatalog(consumers ...ydbtopic.ConsumerSpec) *catalog.Database {
	read := transferFeed
	read.Consumers = consumers
	db := changefeedCatalog(read)
	db.FeatureObjects = must.Must(db.FeatureObjects.With(ydbreplication.ObservedTransferObject("", "ingest",
		ydbreplication.TransferSpec{Source: "events/feed", Target: "events", Lambda: "($m) -> { return []; }",
			Consumer: "fbc17198"}, ydbreplication.StateRunning)))
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
	desired.FeatureObjects, err = desired.FeatureObjects.Replace(schemaext.Object{Ref: ref, Value: retained})
	c.Assert(err, qt.IsNil)
	current.FeatureObjects, err = current.FeatureObjects.Replace(schemaext.Object{Ref: ref, Value: observed})
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
	transfer := ydbreplication.TransferSpec{Source: "app/events", Target: "event_log", Lambda: "($m) -> { return []; }"}
	held := transfer
	held.Consumer = "fbc17198"
	desired := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{}),
			ydbreplication.DesiredTransferObject("", "ingest", "", transfer))),
		FeatureCoverage: must.Must(must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)).
			Combine(replicationCoverage(schemaext.Desired))),
	}
	current := &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(ydbtopic.ObservedObject("app", "events", ydbtopic.Spec{
			Consumers: []ydbtopic.ConsumerSpec{generated}}),
			ydbreplication.ObservedTransferObject("", "ingest", held, ydbreplication.StateRunning))),
		FeatureCoverage: must.Must(must.Must(ydbtopic.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)).
			Combine(replicationCoverage(schemaext.Observed))),
	}

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	declared, _, err := desired.FeatureObjects.Get(ydbtopic.Ref("app", "events"))
	c.Assert(err, qt.IsNil)
	c.Assert(declared.Value, qt.DeepEquals, &ydbtopic.Desired{}, qt.Commentf("the adoption must not write through to the declaration"))
}
