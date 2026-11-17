package pgpublication_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
)

var orders = pgpublication.Member{Schema: "public", Table: "orders"}

// TestCodecs_HappyPath pins each wire the codecs write: members, schemas and
// operations sorted once each, a declaration's publish set nil or empty as
// written, and an observation's publish set as the server applies it.
func TestCodecs_HappyPath(t *testing.T) {
	desired, observed := pgpublication.Codecs()[0], pgpublication.Codecs()[1]
	tests := []struct {
		name  string
		codec schemaext.Codec
		value schemaext.Payload
		wire  string
	}{
		{name: "an empty declaration", codec: desired, value: &pgpublication.DesiredPublication{}, wire: `{"publish":null}`},
		{name: "a declaration publishing nothing", codec: desired, value: &pgpublication.DesiredPublication{Publish: make([]string, 0)},
			wire: `{"publish":[]}`},
		{name: "a full declaration", codec: desired, value: &pgpublication.DesiredPublication{
			Tables: []pgpublication.Member{{Table: "items"}, orders}, Schemas: []string{"b", "a"}, Publish: []string{"update", "insert"},
			ViaPartitionRoot: true, GeneratedColumns: pgpublication.GeneratedColumnsStored, Owner: "Replicator"},
			wire: `{"tables":[{"table":"items"},{"schema":"public","table":"orders"}],"schemas":["a","b"],"publish":["insert","update"],` +
				`"via_partition_root":true,"generated_columns":"stored","owner":"Replicator"}`},
		{name: "an observation publishing nothing", codec: observed, value: &pgpublication.ObservedPublication{AllTables: true},
			wire: `{"all_tables":true,"publish":[],"via_partition_root":false}`},
		{name: "an all-tables declaration", codec: desired, value: &pgpublication.DesiredPublication{AllTables: true},
			wire: `{"all_tables":true,"publish":null}`},
		{name: "an observation", codec: observed, value: &pgpublication.ObservedPublication{
			Tables:  []pgpublication.Member{{Schema: "public", Table: "orders", RowFilter: "(id > 0)", Columns: []string{"id"}}},
			Publish: []string{"truncate", "insert", "update", "delete"}, Owner: "postgres"},
			wire: `{"all_tables":false,"tables":[{"schema":"public","table":"orders","row_filter":"(id \u003e 0)","columns":["id"]}],` +
				`"publish":["insert","update","delete","truncate"],"via_partition_root":false,"owner":"postgres"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded := must.Must(test.codec.Encode(test.value))
			decoded := must.Must(test.codec.Decode(encoded))

			c.Assert(string(encoded), qt.Equals, test.wire)
			c.Assert(decoded.(schemaext.Value).Equal(test.value.(schemaext.Value)), qt.IsTrue)
		})
	}
}

// TestCodecs_FailurePath pins the wires the codecs refuse: one spelling per
// value, known keys in their own case, and only a shape a statement can
// write.
func TestCodecs_FailurePath(t *testing.T) {
	desired, observed := pgpublication.Codecs()[0], pgpublication.Codecs()[1]
	tests := []struct {
		name  string
		codec schemaext.Codec
		wire  string
	}{
		{name: "no publish key", codec: desired, wire: `{}`},
		{name: "all tables spelled false", codec: desired, wire: `{"all_tables":false,"publish":null}`},
		{name: "an empty owner spelled out", codec: desired, wire: `{"publish":null,"owner":""}`},
		{name: "a key in another case", codec: desired, wire: `{"Publish":null}`},
		{name: "a member key in another case", codec: desired, wire: `{"tables":[{"Table":"orders"}],"publish":null}`},
		{name: "an unknown operation", codec: desired, wire: `{"publish":["merge"]}`},
		{name: "an unknown generated columns value", codec: desired, wire: `{"publish":null,"generated_columns":"virtual"}`},
		{name: "all tables beside a table", codec: desired, wire: `{"all_tables":true,"tables":[{"table":"orders"}],"publish":null}`},
		{name: "a table named twice", codec: desired, wire: `{"tables":[{"table":"orders"},{"table":"orders"}],"publish":null}`},
		{name: "an observed member without its schema", codec: observed,
			wire: `{"all_tables":false,"tables":[{"table":"orders"}],"publish":[],"via_partition_root":false}`},
		{name: "an observation without its publish set", codec: observed, wire: `{"all_tables":false,"via_partition_root":false}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := test.codec.Decode(json.RawMessage(test.wire))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestPublication_Projections pins the projections between the two
// representations as the server applies a declaration: a nil publish set is
// all four operations, generated columns none is the default a declaration
// leaves out, and members compare in any order.
func TestPublication_Projections(t *testing.T) {
	c := qt.New(t)
	declared := &pgpublication.DesiredPublication{Tables: []pgpublication.Member{orders, {Schema: "public", Table: "items"}}}
	predicted := &pgpublication.ObservedPublication{Tables: []pgpublication.Member{{Schema: "public", Table: "items"}, orders},
		Publish: pgpublication.PublishOperations()}
	read := &pgpublication.ObservedPublication{Tables: []pgpublication.Member{{Schema: "public", Table: "items"}, orders},
		Publish: pgpublication.PublishOperations(), GeneratedColumns: pgpublication.GeneratedColumnsNone}

	c.Assert(declared.Observed().Equal(predicted), qt.IsTrue)
	c.Assert(read.Desired().Equal(declared), qt.IsTrue)
	c.Assert(declared.EffectiveGeneratedColumns(), qt.Equals, read.EffectiveGeneratedColumns())
	c.Assert((&pgpublication.DesiredPublication{Publish: make([]string, 0)}).Equal(&pgpublication.DesiredPublication{}), qt.IsFalse)
}

// TestPublication_ObservationKeepsNoOwner pins that a declaration made from a
// read leaves the owner unmanaged, so no role is inferred from what the
// catalog reports.
func TestPublication_ObservationKeepsNoOwner(t *testing.T) {
	c := qt.New(t)
	observed := &pgpublication.ObservedPublication{AllTables: true, Publish: []string{"insert"}, Owner: "postgres"}

	c.Assert(observed.Desired(), qt.DeepEquals, &pgpublication.DesiredPublication{AllTables: true, Publish: []string{"insert"}})
}

// TestChange_HappyPath pins the effect each change records and that its codec
// reads back what it writes.
func TestChange_HappyPath(t *testing.T) {
	held := &pgpublication.ObservedPublication{Tables: []pgpublication.Member{orders}, Publish: pgpublication.PublishOperations(), Owner: "postgres"}
	tests := []struct {
		name   string
		change *pgpublication.Change
		effect schemaext.Effect
	}{
		{name: "a creation", change: &pgpublication.Change{After: &pgpublication.DesiredPublication{}},
			effect: schemaext.Effect{Impact: schemaext.Additive, Reason: "creates a publication; no consumer reads it until a subscription does"}},
		{name: "a drop", change: &pgpublication.Change{Before: held},
			effect: schemaext.Effect{Impact: schemaext.Destructive, Reason: "DROP PUBLICATION stops logical replication to every subscriber that reads the publication"}},
		{name: "a member added and one dropped", change: &pgpublication.Change{Before: held,
			After:     &pgpublication.DesiredPublication{Tables: []pgpublication.Member{{Schema: "public", Table: "items"}}},
			AddTables: []pgpublication.Member{{Schema: "public", Table: "items"}}, DropTables: []pgpublication.Member{orders}},
			effect: schemaext.Effect{Impact: schemaext.Behavioral,
				Reason: "publishes more tables to the publication's subscribers; stops publishing tables to the publication's subscribers"}},
		{name: "a rebuild and an owner", change: &pgpublication.Change{Before: held,
			After: &pgpublication.DesiredPublication{AllTables: true, Owner: "replicator"}, Rebuild: true, Owner: "replicator"},
			effect: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "drops and creates the publication again to change its membership form; " +
				"gives the publication to another role, which the publication's subscribers then depend on"}},
		{name: "options", change: &pgpublication.Change{Before: held,
			After: &pgpublication.DesiredPublication{Tables: []pgpublication.Member{orders}, Publish: []string{"insert"}}, Options: true},
			effect: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes which changes the publication publishes"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.change.Validate(), qt.IsNil)
			c.Assert(test.change.Effect(), qt.Equals, test.effect)
			codec := pgpublication.ChangeCodec()
			decoded := must.Must(codec.Decode(must.Must(codec.Encode(test.change))))
			c.Assert(decoded, qt.DeepEquals, test.change)
		})
	}
}

// TestChange_FailurePath pins the changes no comparison makes.
func TestChange_FailurePath(t *testing.T) {
	held := &pgpublication.ObservedPublication{Tables: []pgpublication.Member{orders}, Publish: pgpublication.PublishOperations()}
	tests := []struct {
		name   string
		change *pgpublication.Change
		want   string
	}{
		{name: "no operand", change: &pgpublication.Change{}, want: `.*requires a before or after operand`},
		{name: "options of a creation", change: &pgpublication.Change{After: &pgpublication.DesiredPublication{}, Options: true},
			want: `.*only a surviving publication changes its members, its options or its owner`},
		{name: "a rebuild with members", change: &pgpublication.Change{Before: held, After: &pgpublication.DesiredPublication{AllTables: true},
			Rebuild: true, AddTables: []pgpublication.Member{orders}}, want: `.*a rebuilt publication is created with its members`},
		{name: "a survivor that changes nothing", change: &pgpublication.Change{Before: held, After: &pgpublication.DesiredPublication{}},
			want: `.*changes its members, its options or its owner`},
		{name: "a member without its schema", change: &pgpublication.Change{Before: held, After: &pgpublication.DesiredPublication{},
			DropTables: []pgpublication.Member{{Table: "orders"}}}, want: `.*names each table by schema and name alone`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.change.Validate(), qt.ErrorMatches, test.want)
			c.Assert(test.change.Effect(), qt.Equals, schemaext.Effect{})
		})
	}
}

// TestOperation_HappyPath pins that each action's operation is valid and that
// its codec reads back what it writes.
func TestOperation_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		operation *pgpublication.Operation
		impact    schemaext.Impact
	}{
		{name: "create", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.Create, Tables: []pgpublication.Member{orders},
			Publish: []string{"insert"}}, impact: schemaext.Additive},
		{name: "drop", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.Drop, IfExists: true}, impact: schemaext.Destructive},
		{name: "add a table without a schema", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.AddMembers,
			Tables: []pgpublication.Member{{Table: "items"}}}, impact: schemaext.Behavioral},
		{name: "set members", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.SetMembers, Schemas: []string{"audit"}},
			impact: schemaext.Behavioral},
		{name: "set options", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.SetOptions, Publish: make([]string, 0)},
			impact: schemaext.Behavioral},
		{name: "set owner", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.SetOwner, Owner: "replicator"},
			impact: schemaext.Behavioral},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.operation.Validate(), qt.IsNil)
			c.Assert(test.operation.Effect().Impact, qt.Equals, test.impact)
			codec := pgpublication.OperationCodecs()[0]
			decoded := must.Must(codec.Decode(must.Must(codec.Encode(test.operation))))
			c.Assert(decoded, qt.DeepEquals, test.operation)
		})
	}
}

// TestOperation_FailurePath pins the fields each action refuses.
func TestOperation_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		operation *pgpublication.Operation
		want      string
	}{
		{name: "a drop with members", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.Drop, Tables: []pgpublication.Member{orders}},
			want: `.*a publication drop names its publication and nothing else`},
		{name: "IF EXISTS on a creation", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.Create, IfExists: true},
			want: `.*only a publication drop names IF EXISTS`},
		{name: "a membership change with options", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.AddMembers,
			Tables: []pgpublication.Member{orders}, ViaPartitionRoot: true}, want: `.*names its tables or schemas and nothing else`},
		{name: "an option change without its publish set", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.SetOptions,
			ViaPartitionRoot: true}, want: `.*names its publish set and its settings and nothing else`},
		{name: "an owner on a creation", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.Create, Owner: "replicator"},
			want: `.*only a publication owner change names an owner`},
		{name: "a filtered member", operation: &pgpublication.Operation{Name: "feed", Action: pgpublication.Create,
			Tables: []pgpublication.Member{{Schema: "public", Table: "orders", RowFilter: "(id > 0)"}}}, want: `.*names each table by its name alone`},
		{name: "an unknown action", operation: &pgpublication.Operation{Name: "feed", Action: "rename"}, want: `.*unknown publication action "rename"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.operation.Validate(), qt.ErrorMatches, test.want)
			c.Assert(test.operation.Effect(), qt.Equals, schemaext.Effect{})
		})
	}
}

// TestCoverage_HappyPath pins the claim of a complete read.
func TestCoverage_HappyPath(t *testing.T) {
	c := qt.New(t)

	complete := must.Must(pgpublication.CompleteCoverage(schemaext.Observed))

	c.Assert(complete.Lookup(pgpublication.PublicationKind, pgpublication.Ref("feed")).State, qt.Equals, schemaext.Complete)
	c.Assert(complete.KindRecords(), qt.HasLen, 1)
	c.Assert(complete.KindRecords()[0].Model.Owner, qt.Equals, pgpublication.Owner)
}
