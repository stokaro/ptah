package schemaclean

// White-box testing required: the revision-table probe is only reachable
// through Inspect, which needs a live database of the right dialect. The
// exported API can therefore distinguish these cases on PostgreSQL alone — the
// one dialect with a live test — while the placeholder syntax that would break
// MySQL and SQL Server, and the coverage flag that keeps SQLite from planning a
// table it never destroys, are invisible to it. Every exported-API test on
// PostgreSQL passes whether or not the SQL Server placeholders are spelled for
// the right driver; only the unexported builder's output distinguishes them.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/revisiontable"
)

// TestRevisionTableProbeBindsNamesInTheDialectsPlaceholderSyntax pins the two
// things the probe cannot get wrong without failing at runtime: the placeholder
// syntax, which differs per driver, and the scope argument, which must be the
// one the writer cleans.
//
// The names are bound as arguments rather than pasted into the SQL, so a
// dialect whose placeholders were spelled for a different driver shows up here
// as a wrong token rather than as a query the database rejects only when a
// cleanup is already under way.
func TestRevisionTableProbeBindsNamesInTheDialectsPlaceholderSyntax(t *testing.T) {
	c := qt.New(t)
	names := revisiontable.DefaultNames()

	tests := []struct {
		name         string
		dialect      string
		schema       string
		wantScopeArg string
		wantTokens   []string
		wantCatalog  string
	}{
		{
			name:         "postgres numbers its placeholders and defaults to public",
			dialect:      "postgres",
			schema:       "",
			wantScopeArg: "public",
			wantTokens:   []string{"$1", "$2", "$3"},
			wantCatalog:  "pg_class",
		},
		{
			name:         "postgres honors a pinned schema",
			dialect:      "postgres",
			schema:       "tenant",
			wantScopeArg: "tenant",
			wantTokens:   []string{"$1", "$2", "$3"},
			wantCatalog:  "pg_class",
		},
		{
			name:         "cockroachdb shares the postgres catalog probe",
			dialect:      "cockroachdb",
			schema:       "",
			wantScopeArg: "public",
			wantTokens:   []string{"$1", "$2", "$3"},
			wantCatalog:  "pg_class",
		},
		{
			name:    "mysql passes an empty scope through to DATABASE()",
			dialect: "mysql",
			schema:  "",
			// Empty is deliberate: the query COALESCEs it to DATABASE(), which
			// is how the MySQL writer resolves its own cleanup scope.
			wantScopeArg: "",
			wantTokens:   []string{"?"},
			wantCatalog:  "information_schema.tables",
		},
		{
			name:         "mariadb shares the mysql probe",
			dialect:      "mariadb",
			schema:       "app",
			wantScopeArg: "app",
			wantTokens:   []string{"?"},
			wantCatalog:  "information_schema.tables",
		},
		{
			name:         "sqlserver names its placeholders and defaults to dbo",
			dialect:      "sqlserver",
			schema:       "",
			wantScopeArg: "dbo",
			wantTokens:   []string{"@p1", "@p2", "@p3"},
			wantCatalog:  "sys.tables",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			query, args := revisionTableProbe(test.dialect, test.schema, names)

			c.Assert(args, qt.HasLen, len(names)+1)
			c.Assert(args[0], qt.Equals, test.wantScopeArg)
			c.Assert(args[1:], qt.DeepEquals, []any{revisiontable.Atlas, revisiontable.Ptah})
			c.Assert(query, qt.Contains, test.wantCatalog)
			for _, token := range test.wantTokens {
				c.Assert(query, qt.Contains, token)
			}
			// No revision table name may be interpolated into the SQL; they are
			// bound, so a name carrying a quote cannot reshape the statement.
			c.Assert(query, qt.Not(qt.Contains), revisiontable.Ptah)
			c.Assert(query, qt.Not(qt.Contains), revisiontable.Atlas)
		})
	}
}

// TestRevisionTableProbeRefusesDialectsItCannotRead pins that an unrecognized
// dialect yields no query at all.
//
// The alternative — falling through to the PostgreSQL branch — would send a
// pg_class query to whatever database happened to be marked as covered, and the
// resulting error would name the wrong problem. Every dialect listed here is
// one that coverageFor already reports as not probing, so the two must be
// changed together.
func TestRevisionTableProbeRefusesDialectsItCannotRead(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
	}{
		{name: "sqlite has no probe", dialect: "sqlite"},
		{name: "clickhouse has no probe", dialect: "clickhouse"},
		{name: "an unmeasured dialect has no probe", dialect: "spanner"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			query, args := revisionTableProbe(test.dialect, "", revisiontable.DefaultNames())

			c.Assert(query, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

// TestUnlistedObjectsSkipsWhatThePlanAlreadyNames is the guard against the one
// way this change could make the report worse: listing the same destruction
// twice.
//
// PostgreSQL is the live case — its reader hides schema_migrations but surfaces
// atlas_schema_revisions, so the probe finds a table the plan already carries.
func TestUnlistedObjectsSkipsWhatThePlanAlreadyNames(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name       string
		listed     []Object
		candidates []Object
		want       []Object
	}{
		{
			name:       "adds a table the reader hid",
			listed:     []Object{{Type: ObjectTypeTable, Name: "users"}},
			candidates: []Object{{Type: ObjectTypeTable, Name: revisiontable.Ptah}},
			want:       []Object{{Type: ObjectTypeTable, Name: revisiontable.Ptah}},
		},
		{
			name: "skips a table the reader already surfaced",
			listed: []Object{
				{Type: ObjectTypeTable, Name: "users"},
				{Type: ObjectTypeTable, Name: revisiontable.Atlas},
			},
			candidates: []Object{{Type: ObjectTypeTable, Name: revisiontable.Atlas}},
			want:       []Object{},
		},
		{
			name: "skips on name alone, ignoring a schema the reader qualified",
			listed: []Object{
				{Type: ObjectTypeTable, Schema: "dbo", Name: revisiontable.Atlas},
			},
			candidates: []Object{{Type: ObjectTypeTable, Name: revisiontable.Atlas}},
			want:       []Object{},
		},
		{
			name:   "adds each distinct name once",
			listed: []Object{{Type: ObjectTypeTable, Name: "users"}},
			candidates: []Object{
				{Type: ObjectTypeTable, Name: revisiontable.Atlas},
				{Type: ObjectTypeTable, Name: revisiontable.Ptah},
				{Type: ObjectTypeTable, Name: revisiontable.Atlas},
			},
			want: []Object{
				{Type: ObjectTypeTable, Name: revisiontable.Atlas},
				{Type: ObjectTypeTable, Name: revisiontable.Ptah},
			},
		},
		{
			name:       "a same-named object of another kind is not a duplicate",
			listed:     []Object{{Type: ObjectTypeView, Name: revisiontable.Ptah}},
			candidates: []Object{{Type: ObjectTypeTable, Name: revisiontable.Ptah}},
			want:       []Object{{Type: ObjectTypeTable, Name: revisiontable.Ptah}},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(unlistedObjects(test.listed, test.candidates), qt.DeepEquals, test.want)
		})
	}
}

// TestRevisionTableCoverageMatchesWriterBehavior pins which dialects probe for
// bookkeeping tables at all.
//
// SQLite is the control that stops this from being a blanket "always probe":
// its reader hides schema_migrations and its writer deliberately keeps that
// table (dropAllTables passes includeRevisionTable=false), so probing would put
// a survivor on a destruction plan. ClickHouse is the other control — its
// reader hides nothing, so the plan already names both tables.
func TestRevisionTableCoverageMatchesWriterBehavior(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		want    bool
	}{
		{name: "postgres hides schema_migrations and drops it", dialect: "postgres", want: true},
		{name: "cockroachdb shares the postgres reader", dialect: "cockroachdb", want: true},
		{name: "yugabytedb shares the postgres reader", dialect: "yugabytedb", want: true},
		{name: "mysql hides schema_migrations and drops it", dialect: "mysql", want: true},
		{name: "mariadb shares the mysql reader", dialect: "mariadb", want: true},
		{name: "sqlserver hides both revision tables and drops them", dialect: "sqlserver", want: true},
		{name: "sqlite hides schema_migrations but keeps it", dialect: "sqlite", want: false},
		{name: "clickhouse hides nothing", dialect: "clickhouse", want: false},
		{name: "an unmeasured dialect probes nothing", dialect: "spanner", want: false},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(coverageFor(test.dialect).revisionTables, qt.Equals, test.want)
		})
	}
}
