package atlasschema

// White-box testing required: scopeInspectSchema and diffPatternScope are
// unexported, and the fact under test is which scope each surface hands the
// filter. The filter's own tests prove it honors the flag; nothing there proves
// a caller sets it, and the caller is where this was wrong.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/internal/atlassource"
)

// patternScopeFixture is two tables in two schemas, the second one named so a
// realm-relative pattern has somewhere to reach that the connection's schema
// does not cover.
func patternScopeFixture() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "users", Columns: []catalog.Column{{Name: "id"}, {Name: "name"}}},
			{Schema: "app", Name: "orders", Columns: []catalog.Column{{Name: "id"}}},
		},
	}
}

// No credentials in it: only the query string decides the answer, so a URL
// carrying a password would add nothing but a secret scanner's attention.
const patternScopeBase = "postgres://127.0.0.1:5432/db?sslmode=disable"

// TestScopeInspectSchema_TakesThePatternScopeFromTheURL pins the wiring.
//
// Both rows connect to the same database as the same user and describe the same
// state; only the URL differs, and the pinned community binary v1.3.0 answers
// them differently for it. Ptah answered both the bound way, because the
// connection's schema was handed to the depth rule whatever the run described
// (stokaro/ptah#1703).
func TestScopeInspectSchema_TakesThePatternScopeFromTheURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		// wantErr is the refusal, empty where the pattern is honored.
		wantErr string
		// wantColumns is what the users table keeps.
		wantColumns []string
	}{
		{
			name:        "a database URL describes the realm",
			url:         patternScopeBase,
			wantColumns: []string{"id"},
		},
		{
			name:        "a URL naming a schema fills the pattern's schema slot",
			url:         patternScopeBase + "&search_path=public",
			wantErr:     `too many parts in pattern "public.users.name": this connection is bound to schema "public", so a pattern names object or object.child; write "users.name"`,
			wantColumns: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			info := catalog.ServerInfo{Dialect: "postgres", Schema: "public", URL: test.url}

			got, _, err := scopeInspectSchema(
				patternScopeFixture(), info, InspectOptions{Exclude: []string{"public.users.name"}})

			c.Assert(patternScopeError(err), qt.Equals, test.wantErr)
			c.Assert(patternScopeColumns(got), qt.DeepEquals, test.wantColumns)
		})
	}
}

// TestDiffPatternScope_TakesBothHalvesFromOneSide pins the diff's version of the
// same wiring.
//
// The default schema and the realm answer must come from the same state. Taking
// them from different sides produces a scope neither side has: the connection's
// schema counted against a run the other side described.
func TestDiffPatternScope_TakesBothHalvesFromOneSide(t *testing.T) {
	tests := []struct {
		name      string
		from      atlassource.State
		to        atlassource.State
		wantName  string
		wantRealm bool
	}{
		{
			name:      "a database --from pins both",
			from:      atlassource.State{DefaultSchema: "public", RealmScoped: true},
			to:        atlassource.State{},
			wantName:  "public",
			wantRealm: true,
		},
		{
			name:      "a file --from leaves the answer to --to",
			from:      atlassource.State{},
			to:        atlassource.State{DefaultSchema: "public", RealmScoped: true},
			wantName:  "public",
			wantRealm: true,
		},
		{
			// The half that would be wrong if the two were read separately:
			// --from names the schema and is NOT realm-scoped, so the realm
			// answer is its own, not the later side's.
			name:      "a schema-bound --from is not overtaken by a realm --to",
			from:      atlassource.State{DefaultSchema: "public", RealmScoped: false},
			to:        atlassource.State{DefaultSchema: "app", RealmScoped: true},
			wantName:  "public",
			wantRealm: false,
		},
		{
			name:      "two files fall back to the dialect default",
			from:      atlassource.State{},
			to:        atlassource.State{},
			wantName:  "public",
			wantRealm: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			name, realm := diffPatternScope("postgres", test.from, test.to)

			c.Assert(name, qt.Equals, test.wantName)
			c.Assert(realm, qt.Equals, test.wantRealm)
		})
	}
}

func patternScopeError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// patternScopeColumns names what the users table kept, or nil when the run was
// refused and produced no schema.
func patternScopeColumns(schema *catalog.Database) []string {
	if schema == nil {
		return nil
	}
	for _, table := range schema.Tables {
		if table.Name != "users" {
			continue
		}
		names := make([]string, 0, len(table.Columns))
		for _, column := range table.Columns {
			names = append(names, column.Name)
		}
		return names
	}
	return nil
}
