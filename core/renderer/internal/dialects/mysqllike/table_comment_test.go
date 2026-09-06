package mysqllike_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer/internal/dialects/internal/bufwriter"
	"ptah.run/core/renderer/internal/dialects/mysqllike"
)

// TestVisitCreateTable_CarriesTheTableComment pins where a table's comment goes.
//
// It went above the statement, as an SQL line comment the server never reads:
//
//	-- MYSQL TABLE: customers (people who buy) --
//	CREATE TABLE `customers` (...);
//
// so the comment did not survive a replay. Measured on MariaDB 12.3, source
// against the replay of Ptah's own description read back with an independent
// reader:
//
//	-CREATE TABLE `customers` (...) COMMENT "people who buy";
//	+CREATE TABLE `customers` (...);
//
// The reader carries it correctly and --dry-run against the source answered
// `Schema is synced` throughout, so only a second database could tell
// (stokaro/ptah#2129).
func TestVisitCreateTable_CarriesTheTableComment(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		options map[string]string
		want    string
	}{
		{
			name:    "a comment and nothing else",
			comment: "people who buy",
			want:    ") COMMENT='people who buy';",
		},
		{
			// Options and a comment are both table options, and the comment
			// comes last so an existing option list is not reordered by it.
			name:    "a comment beside the options",
			comment: "people who buy",
			options: map[string]string{"ENGINE": "InnoDB"},
			want:    ") ENGINE=InnoDB COMMENT='people who buy';",
		},
		{
			// An apostrophe is the character a comment is most likely to hold,
			// and an unescaped one ends the literal and breaks the statement.
			name:    "a comment holding an apostrophe",
			comment: "what the buyer's account owns",
			want:    ") COMMENT='what the buyer''s account owns';",
		},
		{
			// The control. A table with no comment must gain no clause --
			// COMMENT='' is a comment, and it is not the same as having none.
			name: "no comment at all",
			want: ");",
		},
		{
			// The other control: the options are still rendered without one.
			name:    "options and no comment",
			options: map[string]string{"ENGINE": "InnoDB"},
			want:    ") ENGINE=InnoDB;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			writer := &bufwriter.Writer{}
			renderer := mysqllike.NewWithCapabilities("mysql", writer, capability.ForDialect("mysql"))
			table := &ast.CreateTableNode{
				Name:    "customers",
				Comment: test.comment,
				Options: test.options,
				Columns: []*ast.ColumnNode{ast.NewColumn("id", "BIGINT")},
			}

			c.Assert(renderer.VisitCreateTable(table), qt.IsNil)

			c.Assert(renderer.GetOutput(), qt.Contains, test.want)
		})
	}
}
