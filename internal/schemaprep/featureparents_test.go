package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/schemaprep"
)

// policyOn is a database that declares the table table and the row policy
// ref names.
func policyOn(c *qt.C, table string, ref objectidentity.ID) *schemamodel.Database {
	c.Helper()
	policy := must.Must(chschema.DesiredRowPolicyObject(ref, chschema.DesiredRowPolicy{}))
	return &schemamodel.Database{Tables: []schemamodel.Table{{Name: table}}, FeatureObjects: must.Must(schemaext.NewObjects(policy))}
}

// connection is ClickHouse's identifier semantics for a connection to d1,
// which names d1 as the schema an unqualified name is in.
func connection() identifier.Semantics {
	semantics := identifier.ForDialect("clickhouse")
	semantics.DefaultSchema = "d1"
	return semantics
}

// TestValidateFeatureParents_HappyPath accepts the row policy p on t beside
// the table t, whether its identity was built under the dialect's rules or
// under a connection's database, as a rollback captures it from a read.
func TestValidateFeatureParents_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		ref  objectidentity.ID
	}{
		{name: "built under the dialect's rules", ref: chschema.RowPolicyRefWith(identifier.ForDialect("clickhouse"), "", "t", "p")},
		{name: "built under a connection's database", ref: chschema.RowPolicyRefWith(connection(), "", "t", "p")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateFeatureParents(policyOn(c, "t", test.ref), "clickhouse")

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestValidateFeatureParents_FailurePath refuses a policy on t beside a
// database that declares only u, however its identity was built, and one
// whose schema the author wrote as d2 beside the table t, which names none.
func TestValidateFeatureParents_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name  string
		table string
		ref   objectidentity.ID
		want  string
	}{
		{name: "no table t under the dialect's rules", table: "u", ref: chschema.RowPolicyRefWith(identifier.ForDialect("clickhouse"), "", "t", "p"),
			want: `.*feature object ptah.run/clickhouse/row-policy t\.p has no declared parent table`},
		{name: "no table t under a connection's database", table: "u", ref: chschema.RowPolicyRefWith(connection(), "", "t", "p"),
			want: `.*feature object ptah.run/clickhouse/row-policy d1\.t\.p has no declared parent table`},
		{name: "a schema the author wrote", table: "t", ref: chschema.RowPolicyRef("d2", "t", "p"),
			want: `.*feature object ptah.run/clickhouse/row-policy d2\.t\.p has no declared parent table`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateFeatureParents(policyOn(c, test.table, test.ref), "clickhouse")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
