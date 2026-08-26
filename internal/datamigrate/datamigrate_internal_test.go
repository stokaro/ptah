package datamigrate

// White-box testing required: insertableColumns, rejectsExplicitInsert, and
// findManagedTable encode dialect-specific insert semantics (identity and
// auto-increment behavior, default-schema matching) that cannot be exercised
// through the exported Generate API without live PostgreSQL and SQL Server
// databases, which the unit suite does not provision.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
)

func TestRejectsExplicitInsert(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		col     catalog.Column
		want    bool
	}{
		{name: "sqlserver identity rejects", dialect: "sqlserver", col: catalog.Column{IsAutoIncrement: true}, want: true},
		{name: "sqlserver plain accepts", dialect: "sqlserver", col: catalog.Column{}, want: false},
		{name: "postgres always rejects", dialect: "postgres", col: catalog.Column{IdentityGeneration: "ALWAYS"}, want: true},
		{name: "postgres by-default accepts", dialect: "postgres", col: catalog.Column{IdentityGeneration: "BY_DEFAULT"}, want: false},
		{name: "postgres serial accepts", dialect: "postgres", col: catalog.Column{IsAutoIncrement: true}, want: false},
		{name: "mysql auto_increment accepts", dialect: "mysql", col: catalog.Column{IsAutoIncrement: true}, want: false},
		{name: "sqlite autoincrement accepts", dialect: "sqlite", col: catalog.Column{IsAutoIncrement: true}, want: false},
		{name: "plain column accepts", dialect: "postgres", col: catalog.Column{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rejectsExplicitInsert(tt.dialect, tt.col), qt.Equals, tt.want)
		})
	}
}

func TestInsertableColumns_Success(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		columns  []catalog.Column
		keys     []string
		wantCols []string
	}{
		{
			name:     "plain columns sorted",
			dialect:  "postgres",
			columns:  []catalog.Column{{Name: "name"}, {Name: "id"}, {Name: "created"}},
			keys:     []string{"id"},
			wantCols: []string{"created", "id", "name"},
		},
		{
			name:     "generated column excluded",
			dialect:  "sqlite",
			columns:  []catalog.Column{{Name: "id"}, {Name: "label"}, {Name: "label_len", GeneratedKind: "STORED"}},
			keys:     []string{"id"},
			wantCols: []string{"id", "label"},
		},
		{
			name:     "postgres serial non-key kept",
			dialect:  "postgres",
			columns:  []catalog.Column{{Name: "code"}, {Name: "n", IsAutoIncrement: true}},
			keys:     []string{"code"},
			wantCols: []string{"code", "n"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			table := catalog.Table{Name: "t", Columns: tt.columns}
			cols, err := insertableColumns(tt.dialect, "t", table, tt.keys)
			c.Assert(err, qt.IsNil)
			c.Assert(cols, qt.DeepEquals, tt.wantCols)
		})
	}
}

func TestInsertableColumns_Refusals(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		columns []catalog.Column
		keys    []string
		wantErr string
	}{
		{
			name:    "postgres identity-always non-key refused",
			dialect: "postgres",
			columns: []catalog.Column{{Name: "code"}, {Name: "seq", IdentityGeneration: "ALWAYS"}},
			keys:    []string{"code"},
			wantErr: `reject explicit inserts`,
		},
		{
			name:    "sqlserver identity non-key refused",
			dialect: "sqlserver",
			columns: []catalog.Column{{Name: "code"}, {Name: "n", IsAutoIncrement: true}},
			keys:    []string{"code"},
			wantErr: `"n"`,
		},
		{
			name:    "missing key column",
			dialect: "postgres",
			columns: []catalog.Column{{Name: "name"}},
			keys:    []string{"id"},
			wantErr: `key column "id"`,
		},
		{
			name:    "generated key column",
			dialect: "sqlite",
			columns: []catalog.Column{{Name: "id", GeneratedKind: "STORED"}, {Name: "name"}},
			keys:    []string{"id"},
			wantErr: `key column "id"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			table := catalog.Table{Name: "t", Columns: tt.columns}
			cols, err := insertableColumns(tt.dialect, "t", table, tt.keys)
			c.Assert(err, qt.IsNotNil)
			c.Assert(cols, qt.IsNil)
			c.Assert(err.Error(), qt.Contains, tt.wantErr)
		})
	}
}

func TestFindManagedTable(t *testing.T) {
	tests := []struct {
		name          string
		tables        []catalog.Table
		wantSchema    string
		defaultSchema string
		table         string
		wantFound     bool
		wantGotSchema string
	}{
		{
			name:          "explicit default schema matches blanked introspected schema",
			tables:        []catalog.Table{{Name: "regions", Schema: ""}},
			wantSchema:    "main",
			defaultSchema: "main",
			table:         "regions",
			wantFound:     true,
			wantGotSchema: "",
		},
		{
			name:          "explicit non-default schema exact match",
			tables:        []catalog.Table{{Name: "regions", Schema: "reference"}},
			wantSchema:    "reference",
			defaultSchema: "main",
			table:         "regions",
			wantFound:     true,
			wantGotSchema: "reference",
		},
		{
			name:          "explicit schema with no match not found",
			tables:        []catalog.Table{{Name: "regions", Schema: "other"}},
			wantSchema:    "reference",
			defaultSchema: "main",
			table:         "regions",
			wantFound:     false,
			wantGotSchema: "",
		},
		{
			name:          "omitted schema unique bare match",
			tables:        []catalog.Table{{Name: "regions", Schema: ""}},
			wantSchema:    "",
			defaultSchema: "main",
			table:         "regions",
			wantFound:     true,
			wantGotSchema: "",
		},
		{
			name:          "omitted schema prefers default among duplicates",
			tables:        []catalog.Table{{Name: "regions", Schema: "other"}, {Name: "regions", Schema: "public"}},
			wantSchema:    "",
			defaultSchema: "public",
			table:         "regions",
			wantFound:     true,
			wantGotSchema: "public",
		},
		{
			name:          "table not present",
			tables:        []catalog.Table{{Name: "regions", Schema: ""}},
			wantSchema:    "",
			defaultSchema: "main",
			table:         "missing",
			wantFound:     false,
			wantGotSchema: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, found := findManagedTable(&catalog.Database{Tables: tt.tables}, tt.wantSchema, tt.defaultSchema, tt.table)
			c.Assert(found, qt.Equals, tt.wantFound)
			c.Assert(got.Schema, qt.Equals, tt.wantGotSchema)
		})
	}
}
