package mysqlprepare_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/mysql/mysqlprepare"
	"ptah.run/dialect/mysql/mysqlschema"
)

// mysqlDefaults is the part of a MySQL 8.4 read's CHARACTER_SETS the tests
// below name.
var mysqlDefaults = map[string]string{
	"latin1":  "latin1_swedish_ci",
	"utf8mb3": "utf8mb3_general_ci",
	"utf8mb4": "utf8mb4_0900_ai_ci",
}

// charsetColumn is a field that names its own character set.
func charsetColumn(name, typ, charset string) schemamodel.Field {
	return schemamodel.Field{Name: name, Type: typ, Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredColumnSettings{Charset: charset}))}
}

// charsetTable is a table whose declaration names charset as its default
// character set, and collation as its default collation where not empty.
func charsetTable(charset, collation string) schemamodel.Table {
	return schemamodel.Table{Name: "t", Collate: collation, Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredTable{Charset: charset}))}
}

// prepared runs the preparation on one table and returns the prepared table.
func prepared(c *qt.C, target string, table schemamodel.Table, fields []schemamodel.Field, aliases map[string]string) schemapreparation.Table {
	c.Helper()
	request := schemapreparation.Request{
		Target:                   target,
		Tables:                   []schemapreparation.Table{{Desired: schemacapture.TableDeclaration{Table: table, Fields: fields}}},
		CharsetDefaultCollations: mysqlDefaults,
		CharsetAliases:           aliases,
	}
	result, err := (mysqlprepare.Service{}).PrepareTables(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Tables, qt.HasLen, 1)
	_, err = schemapreparation.Accept(request, result)
	c.Assert(err, qt.IsNil)
	return result.Tables[0]
}

// TestPrepareTables_ResolvesTheTableDefault resolves a table's default
// collation from the collation it names, from the default of the character
// set it names, and from a platform override group, which wins over both as
// it does where CREATE TABLE is rendered.
func TestPrepareTables_ResolvesTheTableDefault(t *testing.T) {
	tests := []struct {
		name  string
		table schemamodel.Table
		want  string
	}{
		{name: "a named collation", table: charsetTable("utf8mb4", "utf8mb4_unicode_ci"), want: "utf8mb4_unicode_ci"},
		{name: "a named character set", table: charsetTable("utf8mb4", ""), want: "utf8mb4_0900_ai_ci"},
		{name: "a character set written in upper case", table: charsetTable("UTF8MB4", ""), want: "utf8mb4_0900_ai_ci"},
		{
			name:  "an override collation",
			table: schemamodel.Table{Name: "t", Collate: "utf8mb4_bin", Overrides: map[string]map[string]string{"mysql": {"collate": "utf8mb4_unicode_ci"}}},
			want:  "utf8mb4_unicode_ci",
		},
		{
			name:  "an override character set",
			table: schemamodel.Table{Name: "t", Overrides: map[string]map[string]string{"mysql": {"charset": "latin1"}}},
			want:  "latin1_swedish_ci",
		},
		{name: "neither", table: schemamodel.Table{Name: "t"}, want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := prepared(c, "mysql", test.table, []schemamodel.Field{{Name: "id", Type: "int"}}, nil)

			c.Assert(table.DefaultCollation, qt.Equals, test.want)
			c.Assert(table.InheritedCollations, qt.IsNil)
		})
	}
}

// TestPrepareTables_ResolvesInheritedColumnCollations resolves the collation
// of each column that names none and holds one: from the character set the
// column names, the national character set for NCHAR, or the table's default
// collation. A column that names its own collation, a type that holds none, a
// type whose attribute selects a collation of its own and a character set the
// read does not report are left out.
func TestPrepareTables_ResolvesInheritedColumnCollations(t *testing.T) {
	c := qt.New(t)
	fields := []schemamodel.Field{
		{Name: "id", Type: "int"},
		{Name: "plain", Type: "varchar(20)"},
		{Name: "text", Type: "TEXT"},
		{Name: "enumerated", Type: "enum('a','b')"},
		charsetColumn("latin", "varchar(20)", "latin1"),
		charsetColumn("aliased", "char(3)", "utf8"),
		charsetColumn("unreported", "varchar(20)", "ascii"),
		{Name: "national", Type: "nchar(3)"},
		{Name: "spelled", Type: "NATIONAL VARCHAR(3)"},
		{Name: "own", Type: "varchar(20)", Collate: "utf8mb4_bin"},
		{Name: "binary", Type: "varchar(20) BINARY"},
		{Name: "document", Type: "json"},
		{Name: "unclosed", Type: "varchar(20"},
	}

	table := prepared(c, "mysql", charsetTable("utf8mb4", "utf8mb4_unicode_ci"), fields, nil)

	c.Assert(table.InheritedCollations, qt.DeepEquals, map[string]string{
		"plain":      "utf8mb4_unicode_ci",
		"text":       "utf8mb4_unicode_ci",
		"enumerated": "utf8mb4_unicode_ci",
		"latin":      "latin1_swedish_ci",
		"aliased":    "utf8mb3_general_ci",
		"national":   "utf8mb3_general_ci",
		"spelled":    "utf8mb3_general_ci",
	})
}

// TestPrepareTables_NamesTheStoredBinaryTypes names the binary type the
// server stores each character column of the binary character set as, with
// its declared length. A column of another character set, of a type with an
// attribute of its own, or of a type that is not a character type is stored
// as declared.
func TestPrepareTables_NamesTheStoredBinaryTypes(t *testing.T) {
	for _, target := range []string{"mysql", "mariadb"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			fields := []schemamodel.Field{
				charsetColumn("varying", "varchar(10)", "binary"),
				charsetColumn("fixed", "CHAR(4)", "BINARY"),
				charsetColumn("small", "tinytext", "binary"),
				charsetColumn("text", "text", "binary"),
				charsetColumn("medium", "mediumtext", "binary"),
				charsetColumn("long", "longtext", "binary"),
				charsetColumn("latin", "varchar(10)", "latin1"),
				charsetColumn("attribute", "varchar(10) BINARY", "binary"),
				charsetColumn("listed", "enum('a','b')", "binary"),
				{Name: "plain", Type: "varchar(10)"},
			}

			table := prepared(c, target, charsetTable("utf8mb4", ""), fields, nil)

			c.Assert(table.StoredTypes, qt.DeepEquals, map[string]string{
				"varying": "varbinary(10)",
				"fixed":   "binary(4)",
				"small":   "tinyblob",
				"text":    "blob",
				"medium":  "mediumblob",
				"long":    "longblob",
			})
		})
	}
}

// TestPrepareTables_ReadsWhatTheServerAliasNames resolves utf8 through the
// alias the read reports, and through the target descriptor's where the read
// reports none.
func TestPrepareTables_ReadsWhatTheServerAliasNames(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		aliases map[string]string
		want    string
	}{
		{name: "the descriptor on MySQL", target: "mysql", want: "utf8mb3_general_ci"},
		{name: "the descriptor on MariaDB", target: "mariadb", want: "utf8mb3_general_ci"},
		{name: "a read where utf8 names utf8mb3", target: "mariadb", aliases: map[string]string{"utf8": "utf8mb3"}, want: "utf8mb3_general_ci"},
		{name: "a read where utf8 names utf8mb4", target: "mariadb", aliases: map[string]string{"utf8": "utf8mb4"}, want: "utf8mb4_0900_ai_ci"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := prepared(c, test.target, charsetTable("utf8", ""), []schemamodel.Field{{Name: "c", Type: "varchar(20)"}}, test.aliases)

			c.Assert(table.DefaultCollation, qt.Equals, test.want)
			c.Assert(table.InheritedCollations, qt.DeepEquals, map[string]string{"c": test.want})
		})
	}
}

// TestPrepareTables_WithoutCharacterSetDefaults resolves only what the
// declaration names: a read that reports no character set defaults leaves a
// character set's default unresolved and keeps the table's named collation.
func TestPrepareTables_WithoutCharacterSetDefaults(t *testing.T) {
	c := qt.New(t)
	request := schemapreparation.Request{Target: "mysql", Tables: []schemapreparation.Table{{Desired: schemacapture.TableDeclaration{
		Table:  charsetTable("utf8mb4", "utf8mb4_unicode_ci"),
		Fields: []schemamodel.Field{{Name: "plain", Type: "varchar(20)"}, charsetColumn("latin", "varchar(20)", "latin1")},
	}}}}

	result, err := (mysqlprepare.Service{}).PrepareTables(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Tables[0].DefaultCollation, qt.Equals, "utf8mb4_unicode_ci")
	c.Assert(result.Tables[0].InheritedCollations, qt.DeepEquals, map[string]string{"plain": "utf8mb4_unicode_ci"})
	c.Assert(request.Tables[0].InheritedCollations, qt.IsNil)
	c.Assert(request.Tables[0].DefaultCollation, qt.Equals, "")
}

func TestPrepareTables_FailurePath(t *testing.T) {
	t.Run("another target", func(t *testing.T) {
		c := qt.New(t)
		result, err := (mysqlprepare.Service{}).PrepareTables(c.Context(), schemapreparation.Request{Target: "postgres"})
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
		c.Assert(err, qt.ErrorMatches, `.*MySQL preparation target "postgres"`)
		c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
	})
	t.Run("a canceled context", func(t *testing.T) {
		c := qt.New(t)
		ctx, cancel := context.WithCancel(c.Context())
		cancel()
		result, err := (mysqlprepare.Service{}).PrepareTables(ctx, schemapreparation.Request{Target: "mysql"})
		c.Assert(err, qt.ErrorIs, context.Canceled)
		c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
	})
	t.Run("a column settings value of another type", func(t *testing.T) {
		c := qt.New(t)
		request := schemapreparation.Request{Target: "mysql", Tables: []schemapreparation.Table{{Desired: schemacapture.TableDeclaration{
			Table: charsetTable("utf8mb4", ""),
			Fields: []schemamodel.Field{{Name: "c", Type: "varchar(20)",
				Facets: must.Must(schemaext.NewFacets(&mismatchedSettings{}))}},
		}}}}
		result, err := (mysqlprepare.Service{}).PrepareTables(c.Context(), request)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
	})
}

// mismatchedSettings claims the MySQL column settings kind with a type the
// owner does not define.
type mismatchedSettings struct{}

func (*mismatchedSettings) Kind() schemaext.Kind   { return mysqlschema.ColumnSettingsKind }
func (*mismatchedSettings) Clone() schemaext.Value { return &mismatchedSettings{} }
func (*mismatchedSettings) Equal(other schemaext.Value) bool {
	_, ok := other.(*mismatchedSettings)
	return ok
}
