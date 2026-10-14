// Package chcolumn spells a declared column for ClickHouse: the type it maps
// to, whether that type is wrapped in Nullable, the DEFAULT or computed clause,
// and the column definition CREATE TABLE and ADD COLUMN take.
//
// The ClickHouse renderer writes every column through it, and the ClickHouse
// storage owner writes the columns it adds in the same ALTER TABLE as a new
// sorting key through it too, so a column added either way is the same column.
package chcolumn

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/chtype"
	"ptah.run/internal/defaultlit"
)

// Mapping is the ClickHouse type a column renders with. Notice, when not
// empty, is a one-line warning the caller writes as a `-- CLICKHOUSE:` comment
// before the column, so a silent conversion does not surprise the author.
type Mapping struct {
	Type   string
	Notice string
}

// Type returns the type a column renders with: a nullable column that is not
// in the primary key is wrapped in Nullable, unless its type already admits
// NULL. A JSON or JSONB type is rewritten to a String-based type, and its
// notice names the final type, after the Nullable wrap, such as
// Nullable(String).
func Type(col *ast.ColumnNode) (Mapping, error) {
	mapping, err := columnType(col)
	if err != nil {
		return Mapping{}, err
	}
	// A type that already admits NULL is not wrapped again: ClickHouse refuses
	// a Nullable around LowCardinality(Nullable(T)).
	if Nullable(col) && !chtype.AdmitsNull(mapping.Type) {
		mapping.Type = "Nullable(" + mapping.Type + ")"
	}
	return withNotice(col, mapping), nil
}

// KeyType returns the type of a column of the sorting key or primary key,
// which is never wrapped in Nullable, because ClickHouse refuses a Nullable key
// column; the caller decides whether a nullable declaration may be keyed at
// all.
func KeyType(col *ast.ColumnNode) (Mapping, error) {
	mapping, err := columnType(col)
	if err != nil {
		return Mapping{}, err
	}
	return withNotice(col, mapping), nil
}

func withNotice(col *ast.ColumnNode, mapping chtype.Mapping) Mapping {
	result := Mapping{Type: mapping.Type}
	switch {
	case mapping.JSON:
		result.Notice = fmt.Sprintf("mapped JSON → %s; override via platform.clickhouse.type=JSON if you want experimental native JSON", result.Type)
	case mapping.Unrecognized:
		result.Notice = fmt.Sprintf("unrecognized SQL type %q passed through verbatim; verify it is a native ClickHouse type", col.Type)
	}
	return result
}

// columnType writes a type the caller named for ClickHouse itself as it
// stands, and maps everything else through [chtype.Map], which the schema
// comparison asks too, so the type it compares a live column with is the type
// written here.
//
// The mapping exists for a PORTABLE declaration: `DATETIME` is the spelling a
// schema written for several engines uses, ClickHouse has no such type, and
// DateTime64(3) is what preserves a timestamp with subsecond precision. The
// same conversion applied to a type somebody named for ClickHouse ITSELF is a
// different column: DateTime is second precision and four bytes wide,
// DateTime64(3) is millisecond precision and eight.
//
// Measured on ClickHouse 26.7, sql() came back widened:
//
//	column "created_at" { type = sql("DateTime") }   ->   DateTime64(3)
//
// which is the one form whose whole purpose is to be written as it stands
// (stokaro/ptah#2142).
//
// The SQL Server renderer answers the same question the same way and for the
// same reason (stokaro/ptah#2147), as does SQLite for the types its catalog
// keeps verbatim (stokaro/ptah#2040): canonicalizing is right for a declaration
// a person wrote and wrong for a description of a database that already has
// one.
//
// A bare DATETIME is untouched by this and still becomes DateTime64(3). That is
// the portable case, and it stays portable. What #2142 still asks for beyond
// this is the other half -- `schema inspect` marking the types that came from
// ClickHouse's own catalog, so a replay carries the fact this honors.
func columnType(col *ast.ColumnNode) (chtype.Mapping, error) {
	if (col.TypeIsDeclaredText || col.TypeRawSQL) && strings.TrimSpace(col.Type) != "" {
		return chtype.Mapping{Type: strings.TrimSpace(col.Type)}, nil
	}
	mapping, err := chtype.Map(col.Type)
	if err != nil {
		return chtype.Mapping{}, fmt.Errorf("column %q: %w", col.Name, err)
	}
	return mapping, nil
}

// Nullable reports whether a column's type is wrapped in Nullable(): it is
// declared nullable and is not a primary-key column.
func Nullable(col *ast.ColumnNode) bool {
	return col.Nullable && !col.Primary
}

// Value returns the keyword and the expression that give a column its value:
// DEFAULT with a declared default, or MATERIALIZED or ALIAS with a computed
// expression. Both are empty for a column that declares neither. A computed
// column has no DEFAULT: the keyword takes its place, and ClickHouse refuses a
// column that carries both.
func Value(col *ast.ColumnNode) (keyword, expression string) {
	switch {
	case col.GeneratedExpression != "":
		return ComputedKeyword(col.GeneratedKind), col.GeneratedExpression
	case col.Default != nil && col.Default.Expression != "":
		return "DEFAULT", col.Default.Expression
	case col.Default != nil && col.Default.HasLiteral():
		return "DEFAULT", defaultlit.Render(col.Default.Value, StringLiteral)
	}
	return "", ""
}

// ComputedKeyword maps the kinds a declaration may carry onto the two
// ClickHouse has.
//
// There is no GENERATED ALWAYS AS here. A column computed from others is
// written MATERIALIZED -- computed and stored when a row is inserted -- or
// ALIAS, computed on read and never stored. Nothing wrote either before
// stokaro/ptah#2142, so the expression was dropped and the replayed column was
// a plain one that holds the empty string forever where the source computes it
// per row. Measured on clickhouse/clickhouse-server:26.7, source against replay
// in system.columns:
//
//	src   full_name  String  MATERIALIZED  concat(email, ' ')
//	rep   full_name  String
//
// with `schema apply --dry-run` reporting `Schema is synced` throughout.
//
// A column read back from ClickHouse already carries the native keyword and
// passes through. A DECLARED one carries the SQL-standard spelling instead, and
// the two line up: STORED means the value is written, which is MATERIALIZED;
// VIRTUAL means it is computed on read, which is ALIAS.
//
// An unnamed kind takes MATERIALIZED rather than ALIAS, because the two differ
// in whether the data EXISTS. A column guessed into ALIAS is absent from every
// part on disk, and finding that out means noticing a query got cheaper.
func ComputedKeyword(kind string) string {
	switch strings.ToUpper(strings.TrimSpace(kind)) {
	case "ALIAS", "VIRTUAL":
		return "ALIAS"
	default:
		return "MATERIALIZED"
	}
}

// Definition joins a column definition as CREATE TABLE and ADD COLUMN take it:
// the name, the type, the keyword and expression [Value] returns when keyword
// is not empty, and the comment when it is not empty.
func Definition(name, typ, keyword, expression, comment string) string {
	parts := []string{name, typ}
	if keyword != "" {
		parts = append(parts, keyword, expression)
	}
	if comment != "" {
		parts = append(parts, "COMMENT", StringLiteral(comment))
	}
	return strings.Join(parts, " ")
}

// StringLiteral quotes s as a ClickHouse string literal, doubling every single
// quote it holds.
func StringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
