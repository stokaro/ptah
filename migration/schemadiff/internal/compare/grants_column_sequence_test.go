package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// The database holds products, created as items and renamed: its id column
// owns items_id_seq, and its identity column code owns items_code_seq. The
// desired schema declares the same columns, which PostgreSQL would create
// with products_id_seq and products_code_seq. A grant on either sequence
// belongs to its column, whichever of the two names it is written with.

func renamedSerialDatabase(granted ...string) *catalog.Database {
	database := &catalog.Database{
		Tables: []catalog.Table{{Name: "products", Columns: []catalog.Column{
			{Name: "id", DataType: "bigint", OwnedSequence: "items_id_seq"},
			{Name: "code", DataType: "bigint", OwnedSequence: "items_code_seq", IdentityGeneration: "BY_DEFAULT"},
		}}},
	}
	for _, sequence := range granted {
		database.Grants = append(database.Grants, catalog.Grant{
			Role: "app", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: sequence,
		})
	}
	return database
}

// renamedSerialDesired declares products as it was created, the roles in
// managed, and a USAGE grant to app on each granted sequence.
func renamedSerialDesired(managed []string, granted ...string) *schemamodel.Database {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Products", Name: "products"}},
		Fields: []schemamodel.Field{
			{StructName: "Products", Name: "id", Type: "BIGSERIAL", Primary: true},
			{StructName: "Products", Name: "code", Type: "bigint", IdentityGeneration: "BY_DEFAULT"},
		},
	}
	for _, role := range managed {
		desired.Roles = append(desired.Roles, schemamodel.Role{Name: role})
	}
	for _, sequence := range granted {
		desired.Grants = append(desired.Grants, schemamodel.Grant{
			Role: "app", Privileges: []string{"USAGE"}, OnSequence: sequence,
		})
	}
	return desired
}

// sequenceGrantTargets lists the targets of refs, in order.
func sequenceGrantTargets(refs []difftypes.GrantRef) []string {
	var targets []string
	for _, ref := range refs {
		targets = append(targets, ref.ObjectType+" "+ref.ObjectName)
	}
	return targets
}

// TestGrants_ColumnSequenceMatchesUnderEitherName compares a grant on each
// column's sequence written with the name a replay creates, which is what a
// description writes, and with the name the database holds. Nothing is
// planned either way.
func TestGrants_ColumnSequenceMatchesUnderEitherName(t *testing.T) {
	tests := []struct {
		name     string
		declared []string
	}{
		{name: "the name a replay creates", declared: []string{"products_id_seq", "products_code_seq"}},
		{name: "the name the database holds", declared: []string{"items_id_seq", "items_code_seq"}},
		{name: "qualified", declared: []string{"public.products_id_seq", "public.items_code_seq"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithSemantics(renamedSerialDesired([]string{"app"}, test.declared...),
				renamedSerialDatabase("items_id_seq", "items_code_seq"), diff, postgresSemantics())

			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
		})
	}
}

// TestGrants_ColumnSequencePlansUnderTheDatabaseName plans a grant the
// database lacks and revokes one it holds beyond a declaration that manages
// the role. Each statement names the sequence the database holds, since a
// statement naming the replay's name would name no relation.
func TestGrants_ColumnSequencePlansUnderTheDatabaseName(t *testing.T) {
	t.Run("a declared grant the database lacks", func(t *testing.T) {
		c := qt.New(t)
		diff := &difftypes.SchemaDiff{}

		compare.GrantsWithSemantics(renamedSerialDesired(nil, "products_id_seq"), renamedSerialDatabase(), diff, postgresSemantics())

		c.Assert(sequenceGrantTargets(diff.GrantsAdded), qt.DeepEquals, []string{"SEQUENCE items_id_seq"})
		c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
	})
	t.Run("a database grant the declaration leaves out", func(t *testing.T) {
		c := qt.New(t)
		diff := &difftypes.SchemaDiff{}

		compare.GrantsWithSemantics(renamedSerialDesired([]string{"app"}), renamedSerialDatabase("items_code_seq"), diff, postgresSemantics())

		c.Assert(diff.GrantsAdded, qt.HasLen, 0)
		c.Assert(sequenceGrantTargets(diff.GrantsRemoved), qt.DeepEquals, []string{"SEQUENCE items_code_seq"})
	})
	t.Run("a column the database does not have yet", func(t *testing.T) {
		c := qt.New(t)
		diff := &difftypes.SchemaDiff{}

		compare.GrantsWithSemantics(renamedSerialDesired(nil, "products_id_seq"), &catalog.Database{}, diff, postgresSemantics())

		c.Assert(sequenceGrantTargets(diff.GrantsAdded), qt.DeepEquals, []string{"SEQUENCE products_id_seq"})
	})
}

// TestGrants_StandaloneSequenceKeepsItsName compares a grant on a sequence no
// column owns. It is keyed by its name as before: a grant on the sequence a
// column would have owned under that name is not this one.
func TestGrants_StandaloneSequenceKeepsItsName(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	database := &catalog.Database{
		Sequences: []catalog.Sequence{{Name: "order_seq"}},
		Grants:    []catalog.Grant{{Role: "app", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "order_seq"}},
	}
	desired := &schemamodel.Database{
		Sequences: []schemamodel.Sequence{{StructName: "Seq", Name: "order_seq"}},
		Grants:    []schemamodel.Grant{{Role: "app", Privileges: []string{"USAGE"}, OnSequence: "invoice_seq"}},
	}

	compare.GrantsWithSemantics(desired, database, diff, postgresSemantics())

	c.Assert(sequenceGrantTargets(diff.GrantsAdded), qt.DeepEquals, []string{"SEQUENCE invoice_seq"})
}
