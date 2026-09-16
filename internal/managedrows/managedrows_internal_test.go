package managedrows

// White-box testing required: insertableColumns and rejectsExplicitInsert
// encode dialect-specific insert semantics (identity and auto-increment
// behavior) that cannot be exercised through the exported Compare API without
// live PostgreSQL and SQL Server databases, which the unit suite does not
// provision.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
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
		// Oracle sets IsAutoIncrement for both identity modes, and only
		// GENERATED ALWAYS refuses an explicit value (ORA-32795), so the
		// generation mode decides: stokaro/ptah#3295.
		{name: "oracle always rejects", dialect: "oracle", col: catalog.Column{IsAutoIncrement: true, IdentityGeneration: "ALWAYS"}, want: true},
		{name: "oracle by-default accepts", dialect: "oracle", col: catalog.Column{IsAutoIncrement: true, IdentityGeneration: "BY_DEFAULT"}, want: false},
		{name: "plain column accepts", dialect: "postgres", col: catalog.Column{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rejectsExplicitInsert(tt.dialect, tt.col), qt.Equals, tt.want)
		})
	}
}

func TestInsertableColumns_HappyPath(t *testing.T) {
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
			name:     "oracle by-default identity key kept",
			dialect:  "oracle",
			columns:  []catalog.Column{{Name: "CODE"}, {Name: "ID", IsAutoIncrement: true, IdentityGeneration: "BY_DEFAULT"}},
			keys:     []string{"ID"},
			wantCols: []string{"CODE", "ID"},
		},
		{
			name:     "postgres serial non-key kept",
			dialect:  "postgres",
			columns:  []catalog.Column{{Name: "code"}, {Name: "n", IsAutoIncrement: true}},
			keys:     []string{"code"},
			wantCols: []string{"code", "n"},
		},
		// Oracle folds the bare name the renderer wrote, so a declaration that
		// spells its key in lower case names the column the catalog reports as
		// CODE. Matched exactly, the table reads as one that lost its key and
		// the reversible full delete is refused (stokaro/ptah#3321). The
		// matched key is read under the declaration's own spelling, because
		// that is the name the comparison indexes each live row by.
		{
			name:     "oracle reads a folded key under the declared spelling",
			dialect:  "oracle",
			columns:  []catalog.Column{{Name: "CODE"}, {Name: "LABEL"}},
			keys:     []string{"code"},
			wantCols: []string{"LABEL", "code"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			table := catalog.Table{Name: "t", Columns: tt.columns}
			cols, err := insertableColumns(tt.dialect, identifier.ForDialect(tt.dialect), "t", table, tt.keys)
			c.Assert(err, qt.IsNil)
			c.Assert(cols, qt.DeepEquals, tt.wantCols)
		})
	}
}

func TestInsertableColumns_FailurePath(t *testing.T) {
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
			name:    "oracle identity-always key refused",
			dialect: "oracle",
			columns: []catalog.Column{{Name: "ID", IsAutoIncrement: true, IdentityGeneration: "ALWAYS"}, {Name: "CODE"}},
			keys:    []string{"ID"},
			wantErr: `column(s) "ID" reject explicit inserts`,
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
		// The control for the Oracle row in the happy path: PostgreSQL keeps
		// case, so a key spelled in another case is another column there.
		{
			name:    "postgres keeps case, so a folded key is missing",
			dialect: "postgres",
			columns: []catalog.Column{{Name: "CODE"}, {Name: "LABEL"}},
			keys:    []string{"code"},
			wantErr: `key column "code"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			table := catalog.Table{Name: "t", Columns: tt.columns}
			cols, err := insertableColumns(tt.dialect, identifier.ForDialect(tt.dialect), "t", table, tt.keys)
			c.Assert(err, qt.IsNotNil)
			c.Assert(cols, qt.IsNil)
			c.Assert(err.Error(), qt.Contains, tt.wantErr)
		})
	}
}
