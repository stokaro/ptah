package objectowner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/objectowner"
)

// The PostgreSQL family manages the owner of both kinds, and a target whose
// descriptor lists neither, or no descriptor at all, manages none.
func TestManages_HappyPath(t *testing.T) {
	for _, test := range []struct {
		target string
		kind   objectowner.Kind
		want   bool
	}{
		{target: "postgres", kind: objectowner.View, want: true},
		{target: "postgres", kind: objectowner.MaterializedView, want: true},
		{target: "cockroachdb", kind: objectowner.View, want: true},
		{target: "yugabytedb", kind: objectowner.MaterializedView, want: true},
		{target: "mysql", kind: objectowner.View, want: false},
		{target: "sqlite", kind: objectowner.View, want: false},
		{target: "no-such-engine", kind: objectowner.View, want: false},
	} {
		t.Run(test.target+" "+test.kind.Label(), func(t *testing.T) {
			c := qt.New(t)

			c.Assert(objectowner.Manages(test.target, test.kind), qt.Equals, test.want)
		})
	}
}

func TestValidateRole_HappyPath(t *testing.T) {
	for _, role := range []string{"reporting", "Report Owner", `with"quote`, "public_reader"} {
		t.Run(role, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(objectowner.ValidateRole(role), qt.IsNil)
		})
	}
}

// A declaration names one role by its name: not every role, not the session
// that runs the plan, and not a name with white space the server would keep.
func TestValidateRole_FailurePath(t *testing.T) {
	for _, test := range []struct {
		role, wantErr string
	}{
		{role: "", wantErr: `invalid owner: the role's name is empty`},
		{role: "   ", wantErr: `invalid owner: the role's name is empty`},
		{role: " reporting", wantErr: `invalid owner " reporting": white space surrounds the role's name`},
		{role: "public", wantErr: `invalid owner "public": PUBLIC is every role, not one; name the role that owns the object`},
		{role: "current_user", wantErr: `invalid owner CURRENT_USER: it names the role of the session that runs the statement; name the role`},
		{role: "CURRENT_ROLE", wantErr: `invalid owner CURRENT_ROLE: it names .*`},
		{role: "Session_User", wantErr: `invalid owner SESSION_USER: it names .*`},
	} {
		t.Run(test.role, func(t *testing.T) {
			c := qt.New(t)

			err := objectowner.ValidateRole(test.role)

			c.Assert(err, qt.ErrorIs, objectowner.ErrInvalidOwner)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// A declaration that names no owner is not checked anywhere, and one that
// names a role is accepted where the target manages the kind.
func TestValidateDeclared_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, target string
		database     *schemamodel.Database
	}{
		{name: "no database", target: "sqlite"},
		{name: "no owner on a target that manages none", target: "sqlite", database: &schemamodel.Database{
			Views: []schemamodel.View{{Name: "v", Body: "SELECT 1"}}, MaterializedViews: []schemamodel.MaterializedView{{Name: "mv", Body: "SELECT 1"}},
		}},
		{name: "owners on postgres", target: "postgres", database: &schemamodel.Database{
			Views:             []schemamodel.View{{Name: "v", Body: "SELECT 1", Owner: "reporting"}},
			MaterializedViews: []schemamodel.MaterializedView{{Name: "mv", Body: "SELECT 1", Owner: "reporting"}},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(objectowner.ValidateDeclared(test.target, test.database), qt.IsNil)
		})
	}
}

// An owner declared where the target does not manage it is refused by
// capability rather than written nowhere, and a role no declaration may name
// is refused on every target.
func TestValidateDeclared_FailurePath(t *testing.T) {
	t.Run("a view owner on a target that manages none", func(t *testing.T) {
		c := qt.New(t)

		err := objectowner.ValidateDeclared("mysql", &schemamodel.Database{Views: []schemamodel.View{{Name: "v", Body: "SELECT 1", Owner: "reporting"}}})

		var capabilityErr *ptaherr.CapabilityError
		c.Assert(err, qt.ErrorAs, &capabilityErr)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(capabilityErr.Feature, qt.Equals, "view owner")
		c.Assert(err, qt.ErrorMatches, `.*view v declares owner "reporting", and Ptah does not manage the owner of a view on mysql.*`)
	})
	t.Run("a materialized view owner on a target that manages none", func(t *testing.T) {
		c := qt.New(t)

		err := objectowner.ValidateDeclared("sqlite", &schemamodel.Database{
			MaterializedViews: []schemamodel.MaterializedView{{Name: "mv", Body: "SELECT 1", Owner: "reporting"}},
		})

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `.*materialized view mv declares owner "reporting", and Ptah does not manage the owner of a materialized view on sqlite.*`)
	})
	t.Run("a session role on postgres", func(t *testing.T) {
		c := qt.New(t)

		err := objectowner.ValidateDeclared("postgres", &schemamodel.Database{Views: []schemamodel.View{{Name: "v", Body: "SELECT 1", Owner: "CURRENT_USER"}}})

		c.Assert(err, qt.ErrorIs, objectowner.ErrInvalidOwner)
		c.Assert(err, qt.ErrorMatches, `view v: invalid owner CURRENT_USER: .*`)
	})
}

// A read may leave the schema off the object, off its owner row or off both,
// and the lookup reads an empty schema and the default schema as one. Another
// schema, another kind and an unread object are unknown, and so is an owner
// row of a kind this package does not know.
func TestObservedOwners_HappyPath(t *testing.T) {
	observed := objectowner.ObservedOwners([]catalog.ObjectOwner{
		{Kind: "view", Schema: "public", Name: "v", Owner: "reporting"},
		{Kind: "view", Schema: "public", Name: "v", Owner: "second"},
		{Kind: "materialized view", Schema: "", Name: "mv", Owner: "Audit Team"},
		{Kind: "view", Schema: "app", Name: "v", Owner: "app_owner"},
		{Kind: "table", Schema: "public", Name: "t", Owner: "reporting"},
		{Kind: "view", Schema: "public", Name: "nobody", Owner: ""},
	})
	for _, test := range []struct {
		name          string
		kind          objectowner.Kind
		schema, table string
		want          string
		known         bool
	}{
		{name: "the first row of the default schema", kind: objectowner.View, schema: "public", table: "v", want: "reporting", known: true},
		{name: "no schema for the default one", kind: objectowner.View, schema: "", table: "v", want: "reporting", known: true},
		{name: "a row with no schema", kind: objectowner.MaterializedView, schema: "public", table: "mv", want: "Audit Team", known: true},
		{name: "another schema", kind: objectowner.View, schema: "app", table: "v", want: "app_owner", known: true},
		{name: "another kind", kind: objectowner.MaterializedView, schema: "public", table: "v"},
		{name: "a schema that holds none", kind: objectowner.View, schema: "other", table: "v"},
		{name: "a row of another kind", kind: objectowner.View, schema: "public", table: "t"},
		{name: "a row with no owner", kind: objectowner.View, schema: "public", table: "nobody"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, known := observed.Owner(test.kind, test.schema, test.table, "public")

			c.Assert(known, qt.Equals, test.known)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A quoted name keeps its spelling, an unquoted one folds as the dialect folds
// it, and CockroachDB folds a role name either way.
func TestStoredRole_HappyPath(t *testing.T) {
	for _, test := range []struct {
		dialect, written, want string
	}{
		{dialect: "postgres", written: `"Report Owner"`, want: "Report Owner"},
		{dialect: "postgres", written: `"with""quote"`, want: `with"quote`},
		{dialect: "postgres", written: "Reporting", want: "reporting"},
		{dialect: "postgres", written: " reporting ", want: "reporting"},
		{dialect: "cockroachdb", written: `"Reporting"`, want: "reporting"},
		{dialect: "no-such-engine", written: `"Reporting"`, want: "Reporting"},
	} {
		t.Run(test.dialect+" "+test.written, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(objectowner.StoredRole(test.dialect, test.written), qt.Equals, test.want)
		})
	}
}

func TestSameRole_HappyPath(t *testing.T) {
	for _, test := range []struct {
		dialect, a, b string
		want          bool
	}{
		{dialect: "postgres", a: "reporting", b: "reporting", want: true},
		{dialect: "postgres", a: "Reporting", b: "reporting", want: false},
		{dialect: "cockroachdb", a: "Reporting", b: "reporting", want: true},
	} {
		t.Run(test.dialect+" "+test.a, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(objectowner.SameRole(test.dialect, test.a, test.b), qt.Equals, test.want)
		})
	}
}

// TestTransfers_Refuses_HappyPath pins how an answer about owner transfers is
// read: a role the server reported is refused, compared as the dialect stores
// role names, and an answer whose privilege is empty refuses nothing, since
// the server was not asked.
func TestTransfers_Refuses_HappyPath(t *testing.T) {
	refused := objectowner.Transfers{Connected: "app", Privilege: "SET", Refused: []string{"Reporting", "rv_a"}}
	for _, test := range []struct {
		name          string
		transfers     objectowner.Transfers
		dialect, role string
		want          bool
	}{
		{name: "a refused role", transfers: refused, dialect: "postgres", role: "rv_a", want: true},
		{name: "a role the server did not report", transfers: refused, dialect: "postgres", role: "rv_b", want: false},
		{name: "another spelling on PostgreSQL", transfers: refused, dialect: "postgres", role: "reporting", want: false},
		{name: "another spelling on CockroachDB", transfers: refused, dialect: "cockroachdb", role: "reporting", want: true},
		{name: "a server not asked", transfers: objectowner.Transfers{Refused: []string{"rv_a"}}, dialect: "postgres", role: "rv_a", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.transfers.Refuses(test.dialect, test.role), qt.Equals, test.want)
		})
	}
}
