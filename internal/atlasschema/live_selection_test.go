package atlasschema_test

import (
	"bytes"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/readscope"
)

// realmInfo is a PostgreSQL connection whose URL pins no search_path, so a
// selector's leading segment is a schema.
var realmInfo = catalog.ServerInfo{Dialect: "postgres", URL: "postgres://host/app", Schema: "public"}

// pinnedInfo is a PostgreSQL connection limited to public by its URL.
var pinnedInfo = catalog.ServerInfo{Dialect: "postgres", URL: "postgres://host/app?search_path=public", Schema: "public"}

// liveSelectionSides is a database holding a table of another tool's beside an
// application's two tables, the second of which needs the first, and the desired
// state declaring the application's two, and one in a schema the database lacks.
func liveSelectionSides() (*catalog.Database, *schemamodel.Database) {
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "users", Schema: "public"},
			{Name: "orders", Schema: "public"},
			{Name: "databasechangelog", Schema: "public", Columns: []catalog.Column{{Name: "id"}, {Name: "author"}}},
		},
		Constraints: []catalog.Constraint{{
			Name: "orders_user_fk", TableName: "orders", Schema: "public", Type: "FOREIGN KEY",
			ColumnNames: []string{"user_id"}, ForeignTable: new("users"), ForeignColumns: []string{"id"},
		}},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users", Schema: "public"},
			{StructName: "Order", Name: "orders", Schema: "public"},
			{StructName: "Report", Name: "reports", Schema: "analytics"},
		},
	}
	return current, desired
}

func currentTableNames(db *catalog.Database) []string {
	names := make([]string, 0, len(db.Tables))
	for _, table := range db.Tables {
		names = append(names, table.Name)
	}
	return names
}

// desiredTableNames sorts what it lists, because a desired state's tables stay in
// the order their dependencies put them in.
func desiredTableNames(db *schemamodel.Database) []string {
	names := make([]string, 0, len(db.Tables))
	for _, table := range db.Tables {
		names = append(names, table.Name)
	}
	slices.Sort(names)
	return names
}

// TestLiveSelection_ApplyProjectsBothSides covers what each selector keeps, on
// each side of the comparison.
func TestLiveSelection_ApplyProjectsBothSides(t *testing.T) {
	tests := []struct {
		name        string
		info        catalog.ServerInfo
		selection   atlasschema.LiveSelection
		wantCurrent []string
		wantDesired []string
	}{
		{
			name:        "a qualified exclude on a run covering every schema",
			info:        realmInfo,
			selection:   atlasschema.LiveSelection{Exclude: []string{"public.databasechangelog"}},
			wantCurrent: []string{"users", "orders"},
			wantDesired: []string{"orders", "reports", "users"},
		},
		{
			name:        "a bare exclude on a URL pinned to the schema",
			info:        pinnedInfo,
			selection:   atlasschema.LiveSelection{Exclude: []string{"databasechangelog"}},
			wantCurrent: []string{"users", "orders"},
			wantDesired: []string{"orders", "reports", "users"},
		},
		{
			name:        "--schemas alone selects both sides",
			info:        realmInfo,
			selection:   atlasschema.LiveSelection{Schemas: []string{"public"}},
			wantCurrent: []string{"users", "orders", "databasechangelog"},
			wantDesired: []string{"orders", "users"},
		},
		{
			name:        "--schemas joins the selection beside a selector",
			info:        realmInfo,
			selection:   atlasschema.LiveSelection{Schemas: []string{"public"}, Exclude: []string{"public.databasechangelog"}},
			wantCurrent: []string{"users", "orders"},
			wantDesired: []string{"orders", "users"},
		},
		{
			name:        "an include keeps what it names and what it needs",
			info:        realmInfo,
			selection:   atlasschema.LiveSelection{Include: []string{"public.users", "public.orders"}},
			wantCurrent: []string{"users", "orders"},
			wantDesired: []string{"orders", "users"},
		},
		{
			name:        "an include matching one side only leaves the other side empty",
			info:        realmInfo,
			selection:   atlasschema.LiveSelection{Include: []string{"public.databasechangelog"}},
			wantCurrent: []string{"databasechangelog"},
			wantDesired: make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current, desired := liveSelectionSides()
			var diagnostics bytes.Buffer
			test.selection.Diagnostics = &diagnostics

			gotCurrent, gotDesired, err := test.selection.Apply(c.Context(), must.Must(builtin.New()), test.info, current, desired)

			c.Assert(err, qt.IsNil)
			c.Assert(currentTableNames(gotCurrent), qt.DeepEquals, test.wantCurrent)
			c.Assert(desiredTableNames(gotDesired), qt.DeepEquals, test.wantDesired)
			c.Assert(diagnostics.String(), qt.Equals, "")
		})
	}
}

// TestLiveSelection_ApplyCountsAPatternAgainstTheConnection covers what a pattern's
// first segment names. A run covering every schema addresses a column with three
// parts, and a run limited to one schema has that schema's slot filled already.
func TestLiveSelection_ApplyCountsAPatternAgainstTheConnection(t *testing.T) {
	c := qt.New(t)
	selection := atlasschema.LiveSelection{Exclude: []string{"public.databasechangelog.author"}}
	current, desired := liveSelectionSides()

	got, _, err := selection.Apply(c.Context(), must.Must(builtin.New()), realmInfo, current, desired)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Tables[2].Name, qt.Equals, "databasechangelog")
	c.Assert(got.Tables[2].Columns, qt.DeepEquals, []catalog.Column{{Name: "id"}})

	_, _, err = selection.Apply(c.Context(), must.Must(builtin.New()), pinnedInfo, current, desired)

	c.Assert(err, qt.ErrorMatches, `apply --exclude to current schema: too many parts in pattern .*`)
}

// TestLiveSelection_ApplyWithoutASelectorChangesNothing is the control for the
// rows above. Without a schema or object selector, both sides stay whole.
func TestLiveSelection_ApplyWithoutASelectorChangesNothing(t *testing.T) {
	tests := []struct {
		name      string
		selection atlasschema.LiveSelection
	}{
		{name: "the zero value"},
		{name: "an empty --exclude", selection: atlasschema.LiveSelection{Exclude: []string{""}}},
		{name: "an empty --include", selection: atlasschema.LiveSelection{Include: []string{" , "}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current, desired := liveSelectionSides()

			gotCurrent, gotDesired, err := test.selection.Apply(c.Context(), must.Must(builtin.New()), realmInfo, current, desired)

			c.Assert(err, qt.IsNil)
			c.Assert(gotCurrent, qt.Equals, current)
			c.Assert(gotDesired, qt.Equals, desired)
		})
	}
}

// TestLiveSelection_ApplyWarnsAboutAnExcludeThatMatchedNothing holds the warning
// to a selector that named nothing on either side. One that named an object on
// one side is what a create or a drop looks like.
func TestLiveSelection_ApplyWarnsAboutAnExcludeThatMatchedNothing(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
		want    string
	}{
		{
			name:    "a selector naming nothing",
			exclude: []string{"public.nothing_here"},
			want:    "Warning: the --exclude selection matched no objects: \"public.nothing_here\".\n",
		},
		{
			name:    "a selector naming the live side only",
			exclude: []string{"public.databasechangelog"},
			want:    "",
		},
		{
			name:    "a selector naming the desired side only",
			exclude: []string{"analytics.reports"},
			want:    "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current, desired := liveSelectionSides()
			var diagnostics bytes.Buffer
			selection := atlasschema.LiveSelection{Exclude: test.exclude, Diagnostics: &diagnostics}

			_, _, err := selection.Apply(c.Context(), must.Must(builtin.New()), realmInfo, current, desired)

			c.Assert(err, qt.IsNil)
			c.Assert(diagnostics.String(), qt.Equals, test.want)
		})
	}
}

// TestLiveSelection_ApplyWarnsToNoWriter pins that a caller with no diagnostics
// stream is not a caller that crashes on a selector that matched nothing.
func TestLiveSelection_ApplyWarnsToNoWriter(t *testing.T) {
	c := qt.New(t)
	current, desired := liveSelectionSides()
	selection := atlasschema.LiveSelection{Exclude: []string{"public.nothing_here"}}

	gotCurrent, _, err := selection.Apply(c.Context(), must.Must(builtin.New()), realmInfo, current, desired)

	c.Assert(err, qt.IsNil)
	c.Assert(currentTableNames(gotCurrent), qt.DeepEquals, []string{"users", "orders", "databasechangelog"})
}

// TestLiveSelection_ApplyRefusesAnIncludeThatMatchedNothing covers the selection
// that would otherwise report a synced database. Nothing was compared, so the
// answer is the refusal and no state.
func TestLiveSelection_ApplyRefusesAnIncludeThatMatchedNothing(t *testing.T) {
	c := qt.New(t)
	current, desired := liveSelectionSides()
	selection := atlasschema.LiveSelection{Include: []string{"public.nothing_here"}}

	gotCurrent, gotDesired, err := selection.Apply(c.Context(), must.Must(builtin.New()), realmInfo, current, desired)

	var empty *atlasfilter.EmptySelectionError
	c.Assert(err, qt.ErrorAs, &empty)
	c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "public.nothing_here"`)
	c.Assert(gotCurrent, qt.IsNil)
	c.Assert(gotDesired, qt.IsNil)
}

// TestLiveSelection_ApplyRefusesASelectionThatDropsADependency covers the object a
// selected object needs. The refusal takes a positive selection, which is what
// each row has, and the last test is what a run without one gets.
func TestLiveSelection_ApplyRefusesASelectionThatDropsADependency(t *testing.T) {
	tests := []struct {
		name      string
		selection atlasschema.LiveSelection
	}{
		{name: "an include", selection: atlasschema.LiveSelection{Include: []string{"public.orders"}}},
		{name: "--schemas and an exclude", selection: atlasschema.LiveSelection{Schemas: []string{"public"}, Exclude: []string{"public.users"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current, desired := liveSelectionSides()

			gotCurrent, gotDesired, err := test.selection.Apply(c.Context(), must.Must(builtin.New()), realmInfo, current, desired)

			var refused *atlasfilter.CrossScopeError
			c.Assert(err, qt.ErrorAs, &refused)
			c.Assert(err, qt.ErrorMatches, `(?s).*table "public\.orders" depends on table "public\.users" via a foreign key, but "public\.users" is not selected.*`)
			c.Assert(gotCurrent, qt.IsNil)
			c.Assert(gotDesired, qt.IsNil)
		})
	}
}

// TestLiveSelection_ValidateRefusesAMalformedSelector covers what a command
// refuses before it contacts a database.
func TestLiveSelection_ValidateRefusesAMalformedSelector(t *testing.T) {
	tests := []struct {
		name      string
		selection atlasschema.LiveSelection
		want      string
	}{
		{
			name:      "an exclude too deep for any scope",
			selection: atlasschema.LiveSelection{Exclude: []string{"a.b.c.d"}},
			want:      `too many parts in pattern "a.b.c.d".*`,
		},
		{
			name:      "an include of a type no selector can name",
			selection: atlasschema.LiveSelection{Include: []string{"*[type=bogus]"}},
			want:      `unsupported Atlas include resource type "bogus" in selector "\*\[type=bogus\]"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.selection.Validate(selectext.None()), qt.ErrorMatches, test.want)
		})
	}
}

// TestLiveSelection_ReadContextNamesTheKindsLeftOut covers the link to a reader
// that refuses to answer for a kind it cannot see. Only a selector leaving out a
// whole kind relieves it.
func TestLiveSelection_ReadContextNamesTheKindsLeftOut(t *testing.T) {
	tests := []struct {
		name      string
		selection atlasschema.LiveSelection
		kind      readscope.Kind
		wantNeeds bool
	}{
		{name: "no selector", kind: readscope.Triggers, wantNeeds: true},
		{
			name:      "an exclude of every trigger",
			selection: atlasschema.LiveSelection{Exclude: []string{"*[type=trigger]"}},
			kind:      readscope.Triggers,
			wantNeeds: false,
		},
		{
			name:      "an exclude of one trigger",
			selection: atlasschema.LiveSelection{Exclude: []string{"audit_*[type=trigger]"}},
			kind:      readscope.Triggers,
			wantNeeds: true,
		},
		{
			name:      "an exclude of every event",
			selection: atlasschema.LiveSelection{Exclude: []string{"*[type=event]"}},
			kind:      readscope.Events,
			wantNeeds: false,
		},
		{
			name:      "an include of tables alone",
			selection: atlasschema.LiveSelection{Include: []string{"*[type=table]"}},
			kind:      readscope.Views,
			wantNeeds: false,
		},
		{
			name:      "--schemas alone",
			selection: atlasschema.LiveSelection{Schemas: []string{"public"}},
			kind:      readscope.Views,
			wantNeeds: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := test.selection.ReadContext(c.Context(), selectext.None())
			c.Assert(readscope.Needs(got, test.kind), qt.Equals, test.wantNeeds)
		})
	}
}
