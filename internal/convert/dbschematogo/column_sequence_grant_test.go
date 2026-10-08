package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvertDBSchemaToGoSchema_ColumnSequenceGrantNamesTheReplayedSequence
// describes a grant on the sequences of products, created as items and
// renamed. The columns are described as their serial type and identity
// clause, which create products_id_seq and products_code_seq when the
// description is replayed, so the grants name those. A grant on a standalone
// sequence keeps its name, and a column of another schema is the control for
// the qualification.
func TestConvertDBSchemaToGoSchema_ColumnSequenceGrantNamesTheReplayedSequence(t *testing.T) {
	c := qt.New(t)
	defaultValue := "nextval('items_id_seq'::regclass)"

	converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{
		Tables: []catalog.Table{
			{Name: "products", Columns: []catalog.Column{
				{
					Name: "id", DataType: "bigint", IsNullable: "NO", IsAutoIncrement: true,
					ColumnDefault: &defaultValue, OwnedSequence: "items_id_seq",
				},
				{
					Name: "code", DataType: "bigint", IsNullable: "NO",
					IdentityGeneration: "BY_DEFAULT", OwnedSequence: "items_code_seq",
				},
			}},
			{Schema: "app", Name: "orders", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IdentityGeneration: "ALWAYS", OwnedSequence: "orders_id_seq"},
			}},
		},
		Sequences: []catalog.Sequence{{Name: "order_seq"}},
		Grants: []catalog.Grant{
			{Role: "app", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "items_id_seq"},
			{Role: "app", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "items_code_seq"},
			{Role: "app", Privilege: "USAGE", ObjectType: "SEQUENCE", Schema: "app", ObjectName: "orders_id_seq"},
			{Role: "app", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "order_seq"},
		},
	}, "postgres", must.Must(builtin.New())))

	c.Assert(sequenceGrantTargets(converted.Grants), qt.DeepEquals, []string{
		"products_id_seq", "products_code_seq", "app.orders_id_seq", "order_seq",
	})
}

func sequenceGrantTargets(grants []schemamodel.Grant) []string {
	var targets []string
	for _, grant := range grants {
		if grant.OnSequence != "" {
			targets = append(targets, grant.OnSequence)
		}
	}
	return targets
}
