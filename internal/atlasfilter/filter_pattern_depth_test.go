package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// TestExcludeDatabaseWithDefaultSchema_RefusesPatternsDeeperThanTheScope pins
// every cell issue #1181 measured against the pinned community binary, plus the
// three it recorded as pre-existing and two found while reproducing it.
//
// The refusals are Ptah's own text, and that is a retained divergence rather
// than a gap. The arithmetic is the binary's -- it prefixes the connection's
// schema before counting parts -- but its diagnostic quotes the prefixed
// pattern, "public.public.users.name" for a pattern written as
// "public.users.name", which names a string nobody typed and says nothing
// about what to write instead. Same refusal, better diagnostic; rule 1 in
// docs/conformance.md is about never being looser, and naming the spelling
// that works is not looser (stokaro/ptah#1703).
//
// Reverting the depth gate prints, for every row, `got non-nil error / got:
// nil` from the error assertion — and the six mutation rows would additionally
// return a schema whose `users` table has lost a column, which is the half of
// the bug that mattered. Rows whose pattern is deep enough to be refused by
// part count alone (the two four-part rows) stay red on a gate that only
// consulted the raw pattern and never prefixed the schema, which is what
// separates them from the three-part rows.
func TestExcludeDatabaseWithDefaultSchema_RefusesPatternsDeeperThanTheScope(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{
			name:    "qualified column",
			pattern: "public.users.name",
			want:    `too many parts in pattern "public.users.name": this connection is bound to schema "public", so a pattern names object or object.child; write "users.name"`,
		},
		{
			name:    "qualified primary key column",
			pattern: "public.users.id",
			want:    `too many parts in pattern "public.users.id": this connection is bound to schema "public", so a pattern names object or object.child; write "users.id"`,
		},
		{
			name:    "qualified every column of a table",
			pattern: "public.users.*",
			want:    `too many parts in pattern "public.users.*": this connection is bound to schema "public", so a pattern names object or object.child; write "users.*"`,
		},
		{
			name:    "qualified column across tables",
			pattern: "public.*.name",
			want:    `too many parts in pattern "public.*.name": this connection is bound to schema "public", so a pattern names object or object.child; write "*.name"`,
		},
		{
			name:    "wildcard schema column",
			pattern: "*.users.name",
			want:    `too many parts in pattern "*.users.name": this connection is bound to schema "public", so a pattern names object or object.child`,
		},
		{
			name:    "wildcard at every depth",
			pattern: "*.*.*",
			want:    `too many parts in pattern "*.*.*": this connection is bound to schema "public", so a pattern names object or object.child`,
		},
		{
			name:    "past the column",
			pattern: "public.users.name.x",
			want:    `too many parts in pattern "public.users.name.x": this connection is bound to schema "public", so a pattern names object or object.child`,
		},
		{
			name:    "wildcard past the column",
			pattern: "*.*.*.*",
			want:    `too many parts in pattern "*.*.*.*": this connection is bound to schema "public", so a pattern names object or object.child`,
		},
		{
			name:    "schema-relative past the column",
			pattern: "users.name.x",
			want:    `too many parts in pattern "users.name.x": this connection is bound to schema "public", so a pattern names object or object.child`,
		},
		{
			// A table whose name contains dots reads as a three-part pattern.
			// The binary refuses it here too, so ptah loses no spelling the
			// binary offers.
			name:    "dotted object name",
			pattern: "a.b.c",
			want:    `too many parts in pattern "a.b.c": this connection is bound to schema "public", so a pattern names object or object.child`,
		},
		{
			// The documented extension field selector is counted the way the
			// binary counts it: on the raw pattern, selector text included.
			name:    "qualified extension field selector",
			pattern: "public.*[type=extension].version",
			want:    `too many parts in pattern "public.*[type=extension].version": this connection is bound to schema "public", so a pattern names object or object.child; write "*[type=extension].version"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
				defaultSchemaFixture(), []string{test.pattern}, "public")

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestExcludeGeneratedWithDefaultSchema_RefusesPatternsDeeperThanTheScope keeps
// the desired side refusing what the introspected side refuses.
//
// Both sides of a comparison subtract the same patterns. A depth gate on only
// one of them would let a pattern strip a column from the desired state while
// the introspected state kept it, and the plan would then contain the DROP the
// user was trying to prevent.
//
// Reverting the gate on the generated path alone prints `got non-nil error /
// got: nil` here while the database test above stays green, which is exactly
// the asymmetry this row exists to catch.
func TestExcludeGeneratedWithDefaultSchema_RefusesPatternsDeeperThanTheScope(t *testing.T) {
	c := qt.New(t)

	schema := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "name", Type: "TEXT"},
		},
	}

	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(schema, []string{"public.users.name"}, "public")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, `too many parts in pattern "public.users.name": this connection is bound to schema "public", so a pattern names object or object.child; write "users.name"`)
	c.Assert(got, qt.IsNil)
}

// TestExcludeDatabase_RealmRelativeDepthStaysAddressable is the inverse mutant
// of the depth gate. The gate keys on whether a default schema fills the schema
// slot, not on the raw part count, so with no default schema the full
// schema.object.child depth is still addressable — which is what the binary
// does on a URL that names a database rather than a schema.
//
// Reverting the gate leaves this green: it asserts pre-existing behavior.
// Replacing the gate with a flat "at most two parts" rule turns it red with
// `too many parts in pattern: "app.orders.id"`, and narrowing it to columns
// only turns the index assertion red with "orders_id_idx" still listed.
func TestExcludeDatabase_RealmRelativeDepthStaysAddressable(t *testing.T) {
	c := qt.New(t)

	got, err := atlasfilter.ExcludeDatabase(defaultSchemaFixture(), []string{"app.orders.id"})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"users", "app.orders"})
	c.Assert(columnNames(got.Tables[1].Columns), qt.HasLen, 0)
	c.Assert(indexNames(got.Indexes), qt.DeepEquals, []string{"users_name_idx"})
}

// TestExcludeDatabaseWithDefaultSchema_SchemaRelativeChildrenStayAddressable is
// the cost control: the fix refuses a spelling, never a capability. Every child
// the refused three-part patterns reached is still reachable by the
// schema-relative spelling the binary itself requires in this scope, and
// `--exclude users.name` on a schema-bound URL is measured to drop the column
// on the pinned binary too.
//
// Reverting the gate leaves this green. Widening the gate to refuse two-part
// patterns as well — the reading that says "the grammar admits no column part"
// — turns every row red with `too many parts in pattern: "public.users.name"`
// and its siblings.
func TestExcludeDatabaseWithDefaultSchema_SchemaRelativeChildrenStayAddressable(t *testing.T) {
	// The fixture's untouched values. Every row states all three, so a pattern
	// that reaches a child it was not aimed at fails the row that names the
	// child it was aimed at.
	allColumns := []string{"id", "name"}
	allIndexes := []string{"users_name_idx", "orders_id_idx"}
	// extensionVersions renders "name:version", so an empty tail is a cleared
	// version rather than a dropped extension.
	allExtensions := []string{"pgcrypto:1.3", "plpgsql:1.0"}

	tests := []struct {
		name           string
		pattern        string
		wantColumns    []string
		wantIndexes    []string
		wantExtensions []string
	}{
		{
			// users_name_idx indexes the excluded column, and an index left
			// behind by the column it covers would not exist on the filtered
			// side of the comparison either.
			name:           "column",
			pattern:        "users.name",
			wantColumns:    []string{"id"},
			wantIndexes:    []string{"orders_id_idx"},
			wantExtensions: allExtensions,
		},
		{
			name:           "every column of a table",
			pattern:        "users.*",
			wantColumns:    make([]string, 0),
			wantIndexes:    []string{"orders_id_idx"},
			wantExtensions: allExtensions,
		},
		{
			name:           "index",
			pattern:        "users.users_name_idx",
			wantColumns:    allColumns,
			wantIndexes:    []string{"orders_id_idx"},
			wantExtensions: allExtensions,
		},
		{
			name:           "extension field selector",
			pattern:        "*[type=extension].version",
			wantColumns:    allColumns,
			wantIndexes:    allIndexes,
			wantExtensions: []string{"pgcrypto:", "plpgsql:"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
				defaultSchemaFixture(), []string{test.pattern}, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"users", "app.orders"})
			c.Assert(columnNames(got.Tables[0].Columns), qt.DeepEquals, test.wantColumns)
			c.Assert(indexNames(got.Indexes), qt.DeepEquals, test.wantIndexes)
			c.Assert(extensionVersions(got.Extensions), qt.DeepEquals, test.wantExtensions)
		})
	}
}

// TestValidateExcludeSelectors_RefusesDepthNoScopeCanAddress covers the
// pre-connect pass, which runs before a schema is known and can therefore only
// apply the scope-independent half of the rule: a pattern too deep for the
// deepest scope that exists is rejected without contacting a database, and a
// pattern that only a schema prefix pushes over the limit is left to the filter.
//
// Reverting the gate prints `got non-nil error / got: nil` for the two refused
// rows. Moving the whole rule here instead — where the default schema is not
// known — turns the "three parts" row red with a refusal of a pattern that is
// legal against a database-scoped URL.
func TestValidateExcludeSelectors_RefusesDepthNoScopeCanAddress(t *testing.T) {
	tests := []struct {
		name    string
		values  []string
		wantErr string
	}{
		{name: "one part", values: []string{"users"}},
		{name: "two parts", values: []string{"users.name"}},
		{name: "three parts", values: []string{"public.users.name"}},
		{
			name:    "four parts",
			values:  []string{"public.users.name.x"},
			wantErr: `too many parts in pattern "public.users.name.x": a pattern names at most schema.object.child`,
		},
		{
			name:    "four parts inside a comma list",
			values:  []string{"users,*.*.*.*"},
			wantErr: `too many parts in pattern "*.*.*.*": a pattern names at most schema.object.child`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := atlasfilter.ValidateExcludeSelectors(test.values)

			c.Assert(errorText(err), qt.Equals, test.wantErr)
		})
	}
}

// TestPatternDepthRefusalNamesTheSpellingThatWorks pins the three things the
// refusal is allowed to say, one per scope it can be asked in.
//
// The issue this closes asked for two of them: the text must quote what the
// user typed rather than the prefixed string, and it must name the alternative
// spelling. The third is what made the second honest -- the pre-connect pass
// runs before any connection has said which scope applies, so a message that
// named one there would be guessing (stokaro/ptah#1703).
//
// The rows without a suggestion carry the whole risk. Dropping the leading
// segment is only the answer when that segment IS the bound schema: on a
// connection bound to "public", the leading "other" in "other.notes.body" is a
// table name, so suggesting "notes.body" would send the user at a different
// object. The suggestion is offered where it is true and withheld where it is
// not, and a rule that always suggested would be caught by those rows.
func TestPatternDepthRefusalNamesTheSpellingThatWorks(t *testing.T) {
	tests := []struct {
		name string
		// refuse asks the depth rule through the surface that carries the scope
		// under test, so the row names its scope instead of a flag deciding it.
		refuse  func(pattern string) error
		pattern string
		want    string
	}{
		{
			name:    "the leading segment is the bound schema, so it can go",
			refuse:  boundRefusal,
			pattern: "public.users.name",
			want: `too many parts in pattern "public.users.name": this connection is bound to schema ` +
				`"public", so a pattern names object or object.child; write "users.name"`,
		},
		{
			name:    "a leading segment that is not the bound schema is a table",
			refuse:  boundRefusal,
			pattern: "other.notes.body",
			want: `too many parts in pattern "other.notes.body": this connection is bound to schema ` +
				`"public", so a pattern names object or object.child`,
		},
		{
			name:    "dropping the schema would still leave it too deep",
			refuse:  boundRefusal,
			pattern: "public.users.name.x",
			want: `too many parts in pattern "public.users.name.x": this connection is bound to schema ` +
				`"public", so a pattern names object or object.child`,
		},
		{
			name:    "before a connection, no scope is claimed",
			refuse:  preConnectRefusal,
			pattern: "public.users.name.x",
			want:    `too many parts in pattern "public.users.name.x": a pattern names at most schema.object.child`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.refuse(test.pattern)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.want)
		})
	}
}

// boundRefusal asks the schema-bound filter, where the connection has named a
// schema and the pattern is counted against it.
func boundRefusal(pattern string) error {
	_, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(defaultSchemaFixture(), []string{pattern}, "public")
	return err
}

// preConnectRefusal asks the pass that runs before any connection, which has no
// scope to name.
func preConnectRefusal(pattern string) error {
	return atlasfilter.ValidateExcludeSelectors([]string{pattern})
}
