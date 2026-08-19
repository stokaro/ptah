package atlasfilter_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// grammarFixture holds one object of every kind the exclude filter walks in the
// connection's own schema, where every reader leaves Schema blank, and a second
// one of each in "app", where readers report the schema. It is the shape the
// live PostgreSQL reproduction of stokaro/ptah#933 uses.
func grammarFixture() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Schemas: []dbschematypes.DBSchemaInfo{{Name: "public"}, {Name: "app"}},
		Tables: []dbschematypes.DBTable{
			{Name: "users", Comment: "a users comment", Columns: []dbschematypes.DBColumn{{Name: "id"}, {Name: "name"}}},
			{Schema: "app", Name: "orders", Comment: "an orders comment", Columns: []dbschematypes.DBColumn{{Name: "id"}}},
		},
		Enums: []dbschematypes.DBEnum{
			{Name: "mood", Values: []string{"a", "b"}},
			{Schema: "app", Name: "color", Values: []string{"r"}},
		},
		Functions: []dbschematypes.DBFunction{
			{Name: "fn_audit"},
			{Schema: "app", Name: "fn_app"},
		},
		Views: []dbschematypes.DBView{
			{Name: "v_users", Comment: "a view comment"},
			{Schema: "app", Name: "v_orders", Comment: "another view comment"},
		},
		MatViews: []dbschematypes.DBMatView{
			{Name: "mv_users", Comment: "a matview comment"},
			{Schema: "app", Name: "mv_orders"},
		},
		Sequences: []dbschematypes.DBSequence{
			{Name: "seq_pub"},
			{Schema: "app", Name: "seq_app"},
		},
		Domains: []dbschematypes.DBDomain{
			{Name: "dom_pub", BaseType: "text"},
			{Schema: "app", Name: "dom_app", BaseType: "text"},
		},
		Composites: []dbschematypes.DBComposite{
			{Name: "comp_pub"},
			{Schema: "app", Name: "comp_app"},
		},
		Ranges: []dbschematypes.DBRange{
			{Name: "rng_pub", Subtype: "int4"},
			{Schema: "app", Name: "rng_app", Subtype: "int4"},
		},
		Extensions: []dbschematypes.DBExtension{
			{Schema: "public", Name: "pgcrypto", Version: "1.3"},
			{Schema: "app", Name: "hstore", Version: "1.8"},
		},
		Indexes: []dbschematypes.DBIndex{
			{TableName: "users", Name: "users_name_idx", Columns: []string{"name"}},
			{Schema: "app", TableName: "orders", Name: "orders_id_idx", Columns: []string{"id"}},
		},
		Constraints: []dbschematypes.DBConstraint{
			{TableName: "users", Name: "users_name_key", Type: "UNIQUE", ColumnNames: []string{"name"}},
			{Schema: "app", TableName: "orders", Name: "orders_id_key", Type: "UNIQUE", ColumnNames: []string{"id"}},
		},
		Triggers: []dbschematypes.DBTrigger{
			{Name: "users_trg", Table: "users"},
			{Schema: "app", Name: "orders_trg", Table: "orders"},
		},
		RLSPolicies: []dbschematypes.DBRLSPolicy{
			{Name: "users_pol", Table: "users"},
			{Name: "orders_pol", Table: "app.orders"},
		},
		Roles: []dbschematypes.DBRole{{Name: "app_reader"}, {Name: "app_writer"}},
		Grants: []dbschematypes.DBGrant{
			{Role: "app_reader", ObjectType: "TABLE", ObjectName: "orders", Schema: "app", Privilege: "SELECT"},
		},
	}
}

// grammarCensus renders every object the exclude filter walks as
// "<kind> <name>", sorted. One census assertion per row covers every object
// kind at once, so a filter that removes the wrong object fails the row that
// named the right one.
func grammarCensus(schema *dbschematypes.DBSchema) []string {
	var out []string
	add := func(kind, name string) { out = append(out, kind+" "+name) }
	for _, value := range schema.Tables {
		add("table", value.QualifiedName())
		for _, column := range value.Columns {
			add("column", value.QualifiedName()+"."+column.Name)
		}
	}
	for _, value := range schema.Enums {
		add("enum", dbschematypes.QualifyTableName(value.Schema, value.Name))
	}
	for _, value := range schema.Functions {
		add("function", dbschematypes.QualifyTableName(value.Schema, value.Name))
	}
	for _, value := range schema.Views {
		add("view", value.QualifiedName())
	}
	for _, value := range schema.MatViews {
		add("materialized_view", value.QualifiedName())
	}
	for _, value := range schema.Sequences {
		add("sequence", value.QualifiedName())
	}
	for _, value := range schema.Domains {
		add("domain", value.QualifiedName())
	}
	for _, value := range schema.Composites {
		add("composite", value.QualifiedName())
	}
	for _, value := range schema.Ranges {
		add("range", value.QualifiedName())
	}
	for _, value := range schema.Extensions {
		add("extension", value.Name)
	}
	for _, value := range schema.Indexes {
		add("index", value.Name)
	}
	for _, value := range schema.Constraints {
		add("constraint", value.Name)
	}
	for _, value := range schema.Triggers {
		add("trigger", value.Name)
	}
	for _, value := range schema.RLSPolicies {
		add("policy", value.Name)
	}
	for _, value := range schema.Roles {
		add("role", value.Name)
	}
	for _, value := range schema.Grants {
		add("grant", value.Role+"."+value.QualifiedTarget())
	}
	slices.Sort(out)
	return out
}

// grammarFieldCensus renders every field the exclude filter can subtract from
// an object it keeps, as "<kind> <name>.<field>", for the objects that carry
// one. An empty field is absent from the census, so pairing it with
// grammarRemoved states which fields a pattern subtracted -- the whole set, so
// a selector that also emptied a neighbour's comment is a diff rather than an
// assertion nobody thought to write.
func grammarFieldCensus(schema *dbschematypes.DBSchema) []string {
	var out []string
	add := func(kind, name, field, value string) {
		if value == "" {
			return
		}
		out = append(out, kind+" "+name+"."+field)
	}
	for _, value := range schema.Tables {
		add("table", value.QualifiedName(), "comment", value.Comment)
	}
	for _, value := range schema.Views {
		add("view", value.QualifiedName(), "comment", value.Comment)
	}
	for _, value := range schema.MatViews {
		add("materialized_view", value.QualifiedName(), "comment", value.Comment)
	}
	for _, value := range schema.Extensions {
		add("extension", value.Name, "version", value.Version)
	}
	slices.Sort(out)
	return out
}

// grammarGeneratedFieldCensus is the desired-schema half of the same census.
func grammarGeneratedFieldCensus(db *goschema.Database) []string {
	var out []string
	add := func(kind, name, field, value string) {
		if value == "" {
			return
		}
		out = append(out, kind+" "+name+"."+field)
	}
	for _, value := range db.Tables {
		add("table", value.QualifiedName(), "comment", value.Comment)
	}
	for _, value := range db.Views {
		add("view", value.Name, "comment", value.Comment)
	}
	for _, value := range db.MaterializedViews {
		add("materialized_view", value.Name, "comment", value.Comment)
	}
	slices.Sort(out)
	return out
}

// grammarRemoved reports what the filter subtracted, sorted, so a row states
// only its own effect and any collateral removal shows up as a diff.
func grammarRemoved(before, after []string) []string {
	kept := make(map[string]int, len(after))
	for _, entry := range after {
		kept[entry]++
	}
	out := make([]string, 0)
	for _, entry := range before {
		if kept[entry] > 0 {
			kept[entry]--
			continue
		}
		out = append(out, entry)
	}
	slices.Sort(out)
	return out
}

// TestExcludeDatabase_LeadingSchemaTypeSelector covers the one multi-segment
// selector spelling the pinned community binary v1.3.0 implements rather than
// merely accepts. Measured on PostgreSQL 16 across schemas public and app, that
// binary answers `--exclude '*[type=schema].*[type=table]'` with exit 0, both
// tables gone and both enums and both schema blocks kept; Ptah refused the
// spelling outright with "type selectors are supported on the final pattern
// segment only", so a command line that ran there aborted here.
//
// Every row asserts the census of what the filter removed rather than an exit
// code: an exclude that silently matches nothing and one that matches correctly
// share a nil error, so only the surviving objects tell them apart.
func TestExcludeDatabase_LeadingSchemaTypeSelector(t *testing.T) {
	tests := []struct {
		name        string
		patterns    []string
		wantRemoved []string
	}{
		{
			name:        "every table in every schema",
			patterns:    []string{"*[type=schema].*[type=table]"},
			wantRemoved: []string{"column app.orders.id", "column users.id", "column users.name", "constraint orders_id_key", "constraint users_name_key", "grant app_reader.app.orders", "index orders_id_idx", "index users_name_idx", "policy orders_pol", "policy users_pol", "table app.orders", "table users", "trigger orders_trg", "trigger users_trg"},
		},
		{
			// The leading glob narrows which schema, which is what makes the
			// segment a selector rather than a fixed prefix.
			name:        "narrowed to one schema",
			patterns:    []string{"app[type=schema].*[type=table]"},
			wantRemoved: []string{"column app.orders.id", "constraint orders_id_key", "grant app_reader.app.orders", "index orders_id_idx", "policy orders_pol", "table app.orders", "trigger orders_trg"},
		},
		{
			// The final type selector still decides the kind, so nothing else
			// moves. This is the half the community binary's own answer pins:
			// both enums survived there.
			name:        "another kind entirely",
			patterns:    []string{"*[type=schema].*[type=enum]"},
			wantRemoved: []string{"enum app.color", "enum mood"},
		},
		{
			name:        "a schema that holds nothing removes nothing",
			patterns:    []string{"nosuch[type=schema].*[type=table]"},
			wantRemoved: make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(grammarFixture(), test.patterns, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(grammarRemoved(grammarCensus(grammarFixture()), grammarCensus(got)), qt.DeepEquals, test.wantRemoved)
		})
	}
}

// TestExcludeGenerated_LeadingSchemaTypeSelector mirrors the spelling on the
// desired-schema side. Both sides of a comparison must subtract the same
// objects: a selector that removed a table from the introspected side but not
// from the desired side would turn a filtered-out object into a CREATE.
func TestExcludeGenerated_LeadingSchemaTypeSelector(t *testing.T) {
	fixture := func() *goschema.Database {
		return &goschema.Database{
			Tables: []goschema.Table{
				{StructName: "User", Name: "users"},
				{StructName: "Order", Schema: "app", Name: "orders"},
			},
			Enums: []goschema.Enum{
				{Name: "mood", Values: []string{"a"}},
				{Name: "app.color", Values: []string{"r"}},
			},
		}
	}

	tests := []struct {
		name     string
		patterns []string
		// wantTables and wantEnums are what survives, both stated on every row:
		// the failure a type selector risks is subtracting the kind it did not
		// name, and only the untouched half can report that.
		wantTables []string
		wantEnums  []string
	}{
		{
			name:       "every table in every schema",
			patterns:   []string{"*[type=schema].*[type=table]"},
			wantTables: make([]string, 0),
			wantEnums:  []string{"mood", "app.color"},
		},
		{
			name:       "narrowed to one schema",
			patterns:   []string{"app[type=schema].*[type=table]"},
			wantTables: []string{"users"},
			wantEnums:  []string{"mood", "app.color"},
		},
		{
			name:       "another kind entirely",
			patterns:   []string{"*[type=schema].*[type=enum]"},
			wantTables: []string{"app.orders", "users"},
			wantEnums:  make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(fixture(), test.patterns, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(grammarGeneratedTableNames(got.Tables), qt.DeepEquals, test.wantTables)
			c.Assert(grammarGeneratedEnumNames(got.Enums), qt.DeepEquals, test.wantEnums)
		})
	}
}

// TestExcludeDatabase_FieldSelectorsSubtractFields covers the field-selector
// half of the grammar, one row per (resource kind x field spelling).
//
// The pinned community binary v1.3.0 accepts every field selector and honors
// none of them: measured on PostgreSQL 16 with two commented tables,
// `--exclude '*[type=table].comment'`, `--exclude 'public.*[type=table].comment'`
// and `--exclude '*[type=table].*'` are each exit 0 there with output
// byte-identical to the same command without the flag, comments included. Ptah
// honors the fields it can subtract instead, because accepting a scoping
// instruction and silently not carrying it out is the failure this issue is
// about.
func TestExcludeDatabase_FieldSelectorsSubtractFields(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		// wantSubtracted is every field the pattern took away. Each row also
		// asserts that no OBJECT moved, which is the property a field selector
		// owes whatever it names: subtracting a comment must never be spelled
		// as removing the table that carried it.
		wantSubtracted []string
	}{
		{
			name:           "table comment is subtracted and the table stays",
			patterns:       []string{"*[type=table].comment"},
			wantSubtracted: []string{"table app.orders.comment", "table users.comment"},
		},
		{
			// A glob narrows which object loses the field. The
			// schema-qualified spelling of the same narrowing,
			// `app.*[type=table].comment`, is a pattern-depth error rather than
			// a field one; see
			// TestExcludeDatabase_QualifiedFieldSelectorStaysADepthError.
			name:           "a glob narrows which table loses its comment",
			patterns:       []string{"orders[type=table].comment"},
			wantSubtracted: []string{"table app.orders.comment"},
		},
		{
			name:           "star names every subtractable field of the selected kind",
			patterns:       []string{"*[type=table].*"},
			wantSubtracted: []string{"table app.orders.comment", "table users.comment"},
		},
		{
			name:           "view comment",
			patterns:       []string{"*[type=view].comment"},
			wantSubtracted: []string{"view app.v_orders.comment", "view v_users.comment"},
		},
		{
			name:           "materialized view comment",
			patterns:       []string{"*[type=materialized_view].comment"},
			wantSubtracted: []string{"materialized_view mv_users.comment"},
		},
		{
			name:           "extension version stays supported",
			patterns:       []string{"*[type=extension].version"},
			wantSubtracted: []string{"extension hstore.version", "extension pgcrypto.version"},
		},
		{
			name:     "two kinds in one selector",
			patterns: []string{"*[type=table|view].comment"},
			wantSubtracted: []string{
				"table app.orders.comment", "table users.comment",
				"view app.v_orders.comment", "view v_users.comment",
			},
		},
		{
			name:     "two selectors in one run",
			patterns: []string{"*[type=table].comment", "*[type=view].comment"},
			wantSubtracted: []string{
				"table app.orders.comment", "table users.comment",
				"view app.v_orders.comment", "view v_users.comment",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(grammarFixture(), test.patterns, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(grammarRemoved(grammarFieldCensus(grammarFixture()), grammarFieldCensus(got)),
				qt.DeepEquals, test.wantSubtracted)
			c.Assert(grammarRemoved(grammarCensus(grammarFixture()), grammarCensus(got)),
				qt.DeepEquals, make([]string, 0))
		})
	}
}

// TestExcludeGenerated_FieldSelectorsSubtractFields is the desired-schema
// mirror. A comment subtracted on one side only would be planned as a COMMENT
// ON change.
func TestExcludeGenerated_FieldSelectorsSubtractFields(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		// wantSubtracted is every field the pattern took away, and the three
		// objects it left behind are asserted on every row: a selector that
		// removed the object instead of its comment would plan a DROP where the
		// introspected side planned nothing at all.
		wantSubtracted []string
	}{
		{
			name:           "table comment",
			patterns:       []string{"*[type=table].comment"},
			wantSubtracted: []string{"table users.comment"},
		},
		{
			name:           "view comment",
			patterns:       []string{"*[type=view].comment"},
			wantSubtracted: []string{"view v_users.comment"},
		},
		{
			name:           "materialized view comment",
			patterns:       []string{"*[type=materialized_view].comment"},
			wantSubtracted: []string{"materialized_view mv_users.comment"},
		},
	}

	fixture := func() *goschema.Database {
		return &goschema.Database{
			Tables:            []goschema.Table{{StructName: "User", Name: "users", Comment: "a users comment"}},
			Views:             []goschema.View{{StructName: "VUser", Name: "v_users", Comment: "a view comment"}},
			MaterializedViews: []goschema.MaterializedView{{StructName: "MVUser", Name: "mv_users", Comment: "a matview comment"}},
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(fixture(), test.patterns, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(grammarRemoved(grammarGeneratedFieldCensus(fixture()), grammarGeneratedFieldCensus(got)),
				qt.DeepEquals, test.wantSubtracted)
			c.Assert(got.Tables, qt.HasLen, 1)
			c.Assert(got.Views, qt.HasLen, 1)
			c.Assert(got.MaterializedViews, qt.HasLen, 1)
		})
	}
}

// TestExcludeDatabase_FieldSelectorCountsAsAMatch pins the interaction with the
// unmatched-selector refusal. A field selector subtracts a field rather than an
// object, so without asking the field patterns the run would report the
// selector as having named nothing and `schema apply` would exit 1 on a
// database where the tables plainly exist.
func TestExcludeDatabase_FieldSelectorCountsAsAMatch(t *testing.T) {
	c := qt.New(t)

	_, report, err := atlasfilter.ExcludeDatabaseReport(
		grammarFixture(), []string{"*[type=table].comment", "*[type=view].comment"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(report.Unmatched, qt.HasLen, 0)
}

// TestExcludeDatabase_SelectorGrammarRefusals pins the fail-closed half of the
// grammar: a selector Ptah cannot honor is refused before any database is
// contacted rather than accepted and then ignored.
func TestExcludeDatabase_SelectorGrammarRefusals(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		wantErr string
	}{
		{
			name:    "a field Ptah cannot subtract",
			pattern: "*[type=table].charset",
			wantErr: `unsupported Atlas exclude field selector "\.charset": Ptah refuses a field selector it would have to ignore; supported for the selected resource types: comment`,
		},
		{
			name:    "a kind with no subtractable field",
			pattern: "*[type=enum].values",
			wantErr: `unsupported Atlas exclude field selector "\.values": Ptah refuses a field selector it would have to ignore; the selected resource types have no subtractable fields`,
		},
		{
			name:    "star on a kind with no subtractable field",
			pattern: "*[type=enum].*",
			wantErr: `unsupported Atlas exclude field selector "\.\*": .*no subtractable fields`,
		},
		{
			name:    "a type selector on a middle segment",
			pattern: "public.*[type=table].c*[type=column]",
			wantErr: `unsupported Atlas exclude selector "public\.\*\[type=table\]\.c\*\[type=column\]": type selectors are supported on the final pattern segment only`,
		},
		{
			// Only [type=schema] is honored on the leading segment, because a
			// schema is the only thing a leading segment can name.
			name:    "a leading type selector that is not a schema",
			pattern: "*[type=table].*[type=column]",
			wantErr: `unsupported Atlas exclude selector "\*\[type=table\]\.\*\[type=column\]": type selectors are supported on the final pattern segment only`,
		},
		{
			name:    "a malformed glob",
			pattern: "us[ers",
			wantErr: `invalid Atlas exclude glob "us\[ers": syntax error in pattern`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(grammarFixture(), []string{test.pattern}, "public")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(atlasfilter.ValidateExcludeSelectors([]string{test.pattern}), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestExcludeDatabase_QualifiedFieldSelectorStaysADepthError pins why honoring
// `.comment` did not also make `public.*[type=table].comment` work.
//
// The parts of a pattern are counted on the text as written, selector and field
// suffix included, after the connection's schema is prefixed. That is the
// pinned community binary v1.3.0's own arithmetic, measured on a schema-bound
// URL (`?search_path=public`), where it answers
//
//	Error: too many parts in pattern: "public.public.*[type=table].comment"
//
// On a database URL that binary does not prefix and therefore accepts the same
// pattern. Ptah applies one depth rule to every scope, so counting the resource
// glob instead would exit 0 on the schema-bound URL where that binary exits 1 —
// the one direction the compatibility rule forbids. The unqualified spelling is
// the one both scopes honor.
func TestExcludeDatabase_QualifiedFieldSelectorStaysADepthError(t *testing.T) {
	c := qt.New(t)

	_, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
		grammarFixture(), []string{"public.*[type=table].comment"}, "public")

	c.Assert(err, qt.ErrorMatches, `too many parts in pattern: "public\.public\.\*\[type=table\]\.comment"`)
}

// TestExcludeDatabase_LeadingSchemaSegmentIsNotCountedTwice is the other half
// of that arithmetic: a pattern that fills its own schema slot is not prefixed
// again, so the spelling is addressable in a schema-scoped run.
func TestExcludeDatabase_LeadingSchemaSegmentIsNotCountedTwice(t *testing.T) {
	c := qt.New(t)

	_, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
		grammarFixture(), []string{"*[type=schema].*[type=table]"}, "public")

	c.Assert(err, qt.IsNil)
}

func grammarGeneratedTableNames(tables []goschema.Table) []string {
	out := make([]string, 0, len(tables))
	for _, value := range tables {
		out = append(out, value.QualifiedName())
	}
	return out
}

func grammarGeneratedEnumNames(enums []goschema.Enum) []string {
	out := make([]string, 0, len(enums))
	for _, value := range enums {
		out = append(out, value.Name)
	}
	return out
}
