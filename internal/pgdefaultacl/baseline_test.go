package pgdefaultacl_test

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/pgdefaultacl"
)

// rowSpec is a pg_default_acl row as a fixture writes it: the ACLs as
// array_to_json renders them.
type rowSpec struct {
	schema, grantor, objectType, acl, builtin string
}

// rows parses specs into the rows [pgdefaultacl.ReadRows] returns.
func rows(c *qt.C, specs []rowSpec) []pgdefaultacl.Row {
	c.Helper()
	parsed := make([]pgdefaultacl.Row, 0, len(specs))
	for _, spec := range specs {
		row := pgdefaultacl.Row{Schema: spec.schema, Grantor: spec.grantor, ObjectType: spec.objectType,
			ACL: parseACL(c, spec.acl)}
		if spec.builtin != "" {
			row.Builtin = parseACL(c, spec.builtin)
		}
		parsed = append(parsed, row)
	}
	return parsed
}

// statements lists what each reconciliation runs, in order, and nil for
// none.
func statements(reconciled []pgdefaultacl.Revoke) []string {
	var spelled []string
	for _, statement := range reconciled {
		spelled = append(spelled, statement.Statement)
	}
	return spelled
}

// sequencesBuiltin is acldefault('s', o) for a grantor named o.
const sequencesBuiltin = `["o=rwU/o"]`

// TestReconcile_ReturnsTheRowsToTheBaseline compares what a dev database held
// when it was claimed with what a run left, and expects the statements that
// return one to the other. Each row is a change a migration can make; the ACLs
// are what PostgreSQL 18 stores for it.
func TestReconcile_ReturnsTheRowsToTheBaseline(t *testing.T) {
	tests := []struct {
		name     string
		baseline []rowSpec
		current  []rowSpec
		want     []string
	}{
		{
			name:     "a row the run left alone",
			baseline: []rowSpec{{schema: "public", grantor: "o", objectType: "TABLES", acl: `["app=r/o"]`}},
			current:  []rowSpec{{schema: "public", grantor: "o", objectType: "TABLES", acl: `["app=r/o"]`}},
		},
		{
			name:     "a privilege the run added to a kept grantee",
			baseline: []rowSpec{{schema: "public", grantor: "o", objectType: "TABLES", acl: `["app=r/o"]`}},
			current:  []rowSpec{{schema: "public", grantor: "o", objectType: "TABLES", acl: `["app=ar/o"]`}},
			want: []string{
				`ALTER DEFAULT PRIVILEGES FOR ROLE "o" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM "app"`,
				`ALTER DEFAULT PRIVILEGES FOR ROLE "o" IN SCHEMA "public" GRANT SELECT ON TABLES TO "app"`,
			},
		},
		{
			name:     "only the grantee the run changed",
			baseline: []rowSpec{{schema: "public", grantor: "o", objectType: "TABLES", acl: `["app=r/o","=r/o"]`}},
			current:  []rowSpec{{schema: "public", grantor: "o", objectType: "TABLES", acl: `["app=r/o"]`}},
			want: []string{
				`ALTER DEFAULT PRIVILEGES FOR ROLE "o" IN SCHEMA "public" GRANT SELECT ON TABLES TO PUBLIC`,
			},
		},
		{
			name:    "a row the run created in a kept schema",
			current: []rowSpec{{schema: "public", grantor: "o", objectType: "FUNCTIONS", acl: `["app=X/o"]`}},
			want: []string{
				`ALTER DEFAULT PRIVILEGES FOR ROLE "o" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON FUNCTIONS FROM "app"`,
			},
		},
		{
			name:     "a kept row the run revoked",
			baseline: []rowSpec{{schema: "public", grantor: "o", objectType: "SEQUENCES", acl: `["app=rU*/o"]`}},
			want: []string{
				`ALTER DEFAULT PRIVILEGES FOR ROLE "o" IN SCHEMA "public" GRANT SELECT ON SEQUENCES TO "app"`,
				`ALTER DEFAULT PRIVILEGES FOR ROLE "o" IN SCHEMA "public" GRANT USAGE ON SEQUENCES TO "app" WITH GRANT OPTION`,
			},
		},
		{
			name:    "a global row the run created",
			current: []rowSpec{{grantor: "o", objectType: "SEQUENCES", acl: `["o=rwU/o","app=U/o"]`, builtin: sequencesBuiltin}},
			want: []string{
				`ALTER DEFAULT PRIVILEGES FOR ROLE "o" REVOKE ALL PRIVILEGES ON SEQUENCES FROM "app"`,
			},
		},
		{
			name:     "a kept global row the run returned to the built-in default",
			baseline: []rowSpec{{grantor: "o", objectType: "SEQUENCES", acl: `["o=rwU/o","app=U/o"]`, builtin: sequencesBuiltin}},
			want: []string{
				`ALTER DEFAULT PRIVILEGES FOR ROLE "o" GRANT USAGE ON SEQUENCES TO "app"`,
			},
		},
		{
			name: "a kept global revoke the run granted back",
			baseline: []rowSpec{{grantor: "o", objectType: "FUNCTIONS", acl: `["o=X/o"]`,
				builtin: `["=X/o","o=X/o"]`}},
			want: []string{
				`ALTER DEFAULT PRIVILEGES FOR ROLE "o" REVOKE ALL PRIVILEGES ON FUNCTIONS FROM PUBLIC`,
			},
		},
		{
			name:    "an owner the run took a privilege from",
			current: []rowSpec{{grantor: "o", objectType: "SEQUENCES", acl: `["o=rU/o"]`, builtin: sequencesBuiltin}},
			want: []string{
				`ALTER DEFAULT PRIVILEGES FOR ROLE "o" REVOKE ALL PRIVILEGES ON SEQUENCES FROM "o"`,
				`ALTER DEFAULT PRIVILEGES FOR ROLE "o" GRANT SELECT, UPDATE, USAGE ON SEQUENCES TO "o"`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := pgdefaultacl.Reconcile(rows(c, test.baseline), rows(c, test.current))

			c.Assert(statements(got), qt.DeepEquals, test.want)
		})
	}
}

// TestReconcile_ReconcilingTwiceIsANoOp is the check a cleanup makes once its
// statements ran: the baseline reconciled against itself plans nothing, global
// rows included.
func TestReconcile_ReconcilingTwiceIsANoOp(t *testing.T) {
	c := qt.New(t)
	baseline := rows(c, []rowSpec{
		{schema: "public", grantor: "o", objectType: "TABLES", acl: `["app=arwd/o"]`},
		{grantor: "o", objectType: "SEQUENCES", acl: `["o=rwU/o","app=U/o"]`, builtin: sequencesBuiltin},
	})

	got := pgdefaultacl.Reconcile(baseline, baseline)

	c.Assert(got, qt.HasLen, 0)
}

// TestReadRows_ReadsTheScopedAndTheGlobalRows reads the two shapes a dev
// database's baseline holds through a fake server: a row set in a schema, with
// no built-in default, and a global row with one.
func TestReadRows_ReadsTheScopedAndTheGlobalRows(t *testing.T) {
	c := qt.New(t)
	columns := []string{"nspname", "grantor", "object_type", "acl", "builtin"}
	answers := []dbtest.QueryResult{
		{Columns: columns, Rows: [][]driver.Value{{"public", "o", "TABLES", `["app=r/o"]`, `[]`}}},
		{Columns: columns, Rows: [][]driver.Value{{"", "o", "SEQUENCES", `["o=rwU/o","app=U/o"]`, sequencesBuiltin}}},
	}
	var queries []string
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		return answers[len(queries)-1], nil
	})

	scoped, err := pgdefaultacl.ReadRows(c.Context(), db.SQL, []string{"public"})
	c.Assert(err, qt.IsNil)
	global, err := pgdefaultacl.ReadGlobalRows(c.Context(), db.SQL)

	c.Assert(err, qt.IsNil)
	c.Assert(scoped, qt.DeepEquals, rows(c, []rowSpec{
		{schema: "public", grantor: "o", objectType: "TABLES", acl: `["app=r/o"]`, builtin: `[]`},
	}))
	c.Assert(global, qt.DeepEquals, rows(c, []rowSpec{
		{grantor: "o", objectType: "SEQUENCES", acl: `["o=rwU/o","app=U/o"]`, builtin: sequencesBuiltin},
	}))
	c.Assert(queries, qt.HasLen, 2)
	c.Assert(queries[0], qt.Contains, "n.nspname = $1")
	c.Assert(queries[1], qt.Contains, "acldefault(")
}
