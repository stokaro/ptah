package difftypes_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestSequenceChanges_TheWireShapeIsUnchanged is the same promise for the
// sequence family off `[]string`.
//
// One test per family rather than one shared table: the promise is per FIELD,
// and a table that iterated over an interface would pass for a family whose
// marshaller had never been written.
func TestSequenceChanges_TheWireShapeIsUnchanged(t *testing.T) {
	increment := int64(5)
	tests := []struct {
		name    string
		changes difftypes.SequenceChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.SequenceChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "the definition does not reach the wire",
			changes: difftypes.SequenceChanges{
				{Name: "s", AsType: "bigint", Increment: &increment, OwnedBy: "orders.id"},
			},
			want: `["s"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
		{
			name:    "a schema-qualified sequence keeps its qualified spelling",
			changes: difftypes.SequenceChanges{{Name: "s", Schema: "app"}},
			want:    `["app.s"]`,
			why:     "the identity a consumer keys on is unchanged",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestSequenceChanges_TheDefinitionSurvivesInMemory is the other half.
//
// Ownership is the field that makes it matter: `OWNED BY` is planned from the
// operand, and a carry that drops it plans a sequence whose lifetime is not
// tied to its column.
func TestSequenceChanges_TheDefinitionSurvivesInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.SequenceChanges{{Name: "s", Schema: "app", AsType: "bigint", OwnedBy: "orders.id"}}

	c.Assert(changes[0].OwnedBy, qt.Equals, "orders.id")
	c.Assert(changes[0].AsType, qt.Equals, "bigint")
	c.Assert(changes.Names(), qt.DeepEquals, []string{"app.s"})
}

// TestEnumChanges_TheWireShapeIsUnchanged is the same promise for the fifth
// family off `[]string`.
func TestEnumChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.EnumChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.EnumChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name:    "the values do not reach the wire",
			changes: difftypes.EnumChanges{{Name: "status", Values: []string{"draft", "live"}}},
			want:    `["status"]`,
			why:     "a name list is what format_version 1 has always carried here",
		},
		{
			name:    "a schema-qualified enum keeps its qualified spelling",
			changes: difftypes.EnumChanges{{Name: "status", Schema: "app"}},
			want:    `["app.status"]`,
			why:     "the identity a consumer keys on is unchanged",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestEnumChanges_TheValuesSurviveInMemory is the other half.
//
// The values are the whole point for an enum: CREATE TYPE ... AS ENUM is
// planned from them, so a carry that dropped them would plan an empty type.
func TestEnumChanges_TheValuesSurviveInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.EnumChanges{{Name: "status", Schema: "app", Values: []string{"draft", "live"}}}

	c.Assert(changes[0].Values, qt.DeepEquals, []string{"draft", "live"})
	c.Assert(changes.Names(), qt.DeepEquals, []string{"app.status"})
}

// TestViewChanges_TheWireShapeIsUnchanged is the same promise for the seventh
// family off `[]string`.
func TestViewChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.ViewChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.ViewChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "the body, the check option and the attributes do not reach the wire",
			changes: difftypes.ViewChanges{{
				Name:       "active_users",
				Body:       "SELECT id FROM users",
				WithCheck:  true,
				Attributes: []string{"SCHEMABINDING"},
			}},
			want: `["active_users"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
		{
			name:    "a schema-qualified view keeps its qualified spelling",
			changes: difftypes.ViewChanges{{Name: "reporting.active_users"}},
			want:    `["reporting.active_users"]`,
			why:     "the schema is folded into the name, so the name IS the identity a consumer keys on",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestViewChanges_TheBodySurvivesInMemory is the other half: the field the wire
// drops is the field the planner reads.
//
// A view planned from its name alone renders as an empty SELECT, which is the
// failure this family exists to remove, so the carry is asserted where it is
// used rather than only where it is written.
func TestViewChanges_TheBodySurvivesInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.ViewChanges{{Name: "active_users", Body: "SELECT id FROM users", WithCheck: true}}

	c.Assert(changes[0].Body, qt.Equals, "SELECT id FROM users",
		qt.Commentf("the planner renders this rather than looking the name back up"))
	c.Assert(changes[0].WithCheck, qt.IsTrue,
		qt.Commentf("WITH CHECK OPTION is the view's, and a name cannot carry it"))
	c.Assert(changes.Names(), qt.DeepEquals, []string{"active_users"},
		qt.Commentf("and the name list a consumer reads is unchanged"))
}

// TestMaterializedViewChanges_TheWireShapeIsUnchanged is the same promise for
// the eighth family off `[]string`.
func TestMaterializedViewChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.MaterializedViewChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.MaterializedViewChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "the body and the refresh schedule do not reach the wire",
			changes: difftypes.MaterializedViewChanges{{
				Name:   "reporting.user_counts",
				Body:   "SELECT count() FROM users",
				Facets: hourlyRefresh(),
			}},
			want: `["reporting.user_counts"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// hourlyRefresh is a ClickHouse refresh schedule as source decoding attaches
// it to a materialized view.
func hourlyRefresh() schemaext.Facets {
	schedule := &chschema.DesiredRefresh{Schedule: chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR"}}
	return must.Must(must.Must(schemaext.NewFacets(schedule)).WithTargetScope(chschema.RefreshKind, "clickhouse"))
}

// TestMaterializedViewChanges_TheRefreshScheduleSurvivesInMemory is the other
// half, on the settings this family carries that its plain twin does not.
//
// A ClickHouse refreshable view whose schedule did not reach the planner would
// be created as an ordinary one: the rows would be right once and never again.
func TestMaterializedViewChanges_TheRefreshScheduleSurvivesInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.MaterializedViewChanges{{
		Name:   "reporting.user_counts",
		Body:   "SELECT count() FROM users",
		Facets: hourlyRefresh(),
	}}

	schedule, found, err := schemaext.FacetAs[*chschema.DesiredRefresh](changes[0].Facets, chschema.RefreshKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue, qt.Commentf("a view created without its schedule refreshes once and never again"))
	c.Assert(schedule.Mode, qt.Equals, "EVERY")
	c.Assert(schedule.Interval, qt.Equals, "1 HOUR")
	c.Assert(changes.Names(), qt.DeepEquals, []string{"reporting.user_counts"},
		qt.Commentf("and the name list a consumer reads is unchanged"))
}

// TestColumnChanges_TheWireShapeIsUnchanged is the same promise for the
// twelfth family off `[]string`. Hypertables and continuous aggregates are
// owner changes and carry their operands in the owner's codec.
func TestColumnChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.ColumnChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.ColumnChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "the definition does not reach the wire",
			changes: difftypes.ColumnChanges{{
				StructName: "User",
				Name:       "email",
				Type:       "VARCHAR(255)",
				Nullable:   true,
				Foreign:    "accounts(id)",
			}},
			want: `["email"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestColumnChanges_TheDefinitionSurvivesInMemory is the other half, on the
// fields `ALTER TABLE ... ADD COLUMN` is written from.
//
// A column planned from a name alone renders as a name and nothing else, which
// is not a statement any engine accepts.
func TestColumnChanges_TheDefinitionSurvivesInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.ColumnChanges{{
		Name:     "email",
		Type:     "VARCHAR(255)",
		Nullable: true,
		Default:  "'unknown'",
	}}

	c.Assert(changes[0].Type, qt.Equals, "VARCHAR(255)",
		qt.Commentf("ADD COLUMN is written from this, and a name cannot carry it"))
	c.Assert(changes[0].Nullable, qt.IsTrue)
	c.Assert(changes[0].Default, qt.Equals, "'unknown'")
	c.Assert(changes.Names(), qt.DeepEquals, []string{"email"},
		qt.Commentf("and the name list a consumer reads is unchanged"))
}

// TestRoleChanges_TheWireShapeIsUnchanged is the same promise for the
// thirteenth family off `[]string`, and the one where it is a security
// property rather than only a compatibility one.
func TestRoleChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.RoleChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.RoleChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "the attributes do not reach the wire, and neither does the password",
			changes: difftypes.RoleChanges{{
				Name:      "app_user",
				Login:     true,
				Superuser: true,
				Password:  "hunter2",
			}},
			want: `["app_user"]`,
			why:  "a role's password must not appear in a migration document",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
			c.Assert(string(encoded), qt.Not(qt.Contains), "hunter2",
				qt.Commentf("asserted separately from the shape, because this is the half that matters"))
		})
	}
}

// TestRoleChanges_TheAttributesSurviveInMemory is the other half, on the fields
// `CREATE ROLE` is written from.
//
// A role planned from a name alone is created with every attribute defaulted,
// which for LOGIN and SUPERUSER is a different account from the one declared.
func TestRoleChanges_TheAttributesSurviveInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.RoleChanges{{Name: "app_user", Login: true, CreateDB: true, Inherit: true}}

	c.Assert(changes[0].Login, qt.IsTrue,
		qt.Commentf("CREATE ROLE is written from this, and a name cannot carry it"))
	c.Assert(changes[0].CreateDB, qt.IsTrue)
	c.Assert(changes.Names(), qt.DeepEquals, []string{"app_user"},
		qt.Commentf("and the name list a consumer reads is unchanged"))
}

// TestFunctionChanges_TheWireShapeIsUnchanged is the same promise for the
// fourteenth family off `[]string`, the one that retires a parallel field.
func TestFunctionChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.FunctionChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.FunctionChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "neither the body nor the drop signature reaches the wire",
			changes: difftypes.FunctionChanges{{
				Function: schemamodel.Function{
					Name: "bump", Parameters: "a integer", Returns: "integer",
					Language: "sql", Body: "SELECT a + 1",
				},
				Signature: new("integer"),
			}},
			want: `["bump"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestFunctionChanges_TheSignatureIsNotTheParameters is the distinction this
// family exists to keep.
//
// `Parameters` is what the author wrote -- names, defaults, OUT arguments and
// all -- and `Signature` is what identifies an overload to `DROP FUNCTION`.
// PostgreSQL answers a bare name with `function name "f" is not unique`
// whenever a schema holds more than one, and IF EXISTS does not help, because
// the refusal is about ambiguity rather than existence (stokaro/ptah#2296).
func TestFunctionChanges_TheSignatureIsNotTheParameters(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.FunctionChanges{{
		Function:  schemamodel.Function{Name: "bump", Parameters: "a integer DEFAULT 1"},
		Signature: new("integer"),
	}}

	c.Assert(changes[0].Parameters, qt.Equals, "a integer DEFAULT 1",
		qt.Commentf("what the author wrote, which a CREATE renders"))
	c.Assert(changes[0].Signature, qt.DeepEquals, new("integer"),
		qt.Commentf("what identifies the overload, which a DROP addresses"))
	c.Assert(changes.Removals(), qt.DeepEquals,
		[]difftypes.RoutineRemoval{{Name: "bump", Signature: new("integer")}},
		qt.Commentf("and this is the shape the parallel list used to hold"))
}
