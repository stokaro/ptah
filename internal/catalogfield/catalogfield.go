// Package catalogfield describes one column a database reported as a field of
// the desired-schema model.
//
// It is the half of the catalog-to-model conversion that answers about a single
// column, split out of internal/convert/dbschematogo so that the schema
// COMPARISON can reach it too: a removed column is absent from the desired
// schema by definition, so the only place its definition exists is the catalog,
// and a change that carries the column has to describe it the same way the
// document does. A second copy of these rules would answer differently the
// first time either one learned something (stokaro/ptah#2315).
//
// What stays behind is what is about the Go source rather than the column: the
// struct a field belongs to and the Go field name are parser artifacts, and
// nothing below this package has them.
package catalogfield

import (
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/ydbtype"
)

// ForeignKey is the field-level foreign key a column carries, in the spelling a
// declaration writes.
//
// It is the caller's to supply because it is not on the column: the catalog
// reports foreign keys as constraints over columns, and deciding which of them
// is a single column's own needs the whole table.
type ForeignKey struct {
	// Name is the constraint name.
	Name string
	// Reference is the "table(column)" the key points at.
	Reference string
	// OnDelete and OnUpdate are the referential actions.
	OnDelete string
	OnUpdate string
	// Deferrable and Initially carry the deferral the catalog reported, so a
	// single-column foreign key read back off a live server keeps the property
	// the schema declared (stokaro/ptah#1624).
	Deferrable bool
	Initially  string
	// Match and NotEnforced carry the MATCH type and enforcement the catalog
	// reported, for the same reason (stokaro/ptah#3853).
	Match       string
	NotEnforced bool
}

// Options is what a column cannot answer about itself.
type Options struct {
	// CoveredByTablePrimaryKey reports whether a TABLE-level primary key
	// already names this column, which is why a column's own IsPrimaryKey does
	// not become Primary: the two would render the key twice.
	CoveredByTablePrimaryKey bool

	// ForeignKey is the column's own foreign key, or nil.
	ForeignKey *ForeignKey

	// Dialect is the dialect that reported the column. It decides how a
	// reported default is told apart from an expression; see [Field].
	Dialect string
}

// Field describes one reported column as a field of the desired-schema model.
//
// StructName and FieldName are left empty: they name the Go source a field was
// parsed from, and a column the database reported was not parsed from any.
//
// A reported default becomes the field's Default when it is a value and its
// DefaultExpr when it is an expression. A quoted default is a value. On YDB
// every stored default is a value, reported as the typed YQL literal
// internal/ydbtype writes (`5t`, `'x'u`, `Timestamp('...')`), and is carried
// as the value that literal was written from.
func Field(column catalog.Column, opts Options) schemamodel.Field {
	field := schemamodel.Field{
		Facets:             column.Facets,
		Name:               column.Name,
		Type:               Type(column),
		Comment:            column.Comment,
		TypeIsDeclaredText: column.TypeIsDeclaredText,
		Nullable:           column.IsNullable == "YES",
		// Carried from the catalog rather than derived. PostgreSQL 18
		// names every NOT NULL and flags none of them as generated, so
		// a faithful description returns what the catalog holds
		// (stokaro/ptah#2161).
		NotNullConstraintName: column.NotNullConstraintName,
		Primary:               column.IsPrimaryKey && !opts.CoveredByTablePrimaryKey,
		AutoInc:               column.IsAutoIncrement,
		Unique:                column.IsUnique,
		Collate:               column.Collate,
		GeneratedKind:         column.GeneratedKind,
		IdentityGeneration:    column.IdentityGeneration,
		IdentityStart:         column.IdentityStart,
		IdentityIncrement:     column.IdentityIncrement,
	}
	if column.GeneratedExpression != nil {
		field.GeneratedExpression = *column.GeneratedExpression
	}
	if column.ColumnDefault != nil && serialType(column) == "" {
		setDefault(&field, *column.ColumnDefault, opts.Dialect)
	}
	if opts.ForeignKey != nil {
		field.Foreign = opts.ForeignKey.Reference
		field.ForeignKeyName = opts.ForeignKey.Name
		field.OnDelete = opts.ForeignKey.OnDelete
		field.Deferrable = opts.ForeignKey.Deferrable
		field.Initially = opts.ForeignKey.Initially
		field.ForeignKeyMatch = opts.ForeignKey.Match
		field.ForeignKeyNotEnforced = opts.ForeignKey.NotEnforced
		field.OnUpdate = opts.ForeignKey.OnUpdate
	}
	return field
}

func Type(dbColumn catalog.Column) string {
	if serialType := serialType(dbColumn); serialType != "" {
		return serialType
	}
	// The server's own spelling wins wherever the reader had to ask for it,
	// which today means PostgreSQL array and domain columns. DataType for an
	// array is the bare category "ARRAY" -- a word no engine accepts as a type,
	// so a schema read back out of a database rendered DDL that could not be
	// executed (stokaro/ptah#1138).
	//
	// It is read from FormattedType rather than from ColumnType deliberately.
	// ColumnType is also what the Atlas-compatible JSON inspect output prints,
	// and measured on the pinned community binary v1.3.0 that output is
	// `"type": "ARRAY"` for an array column -- the same value Ptah prints there
	// today. Routing the fix through ColumnType would have made that surface
	// disagree with the binary in order to fix a surface the binary does not
	// have.
	//
	// It stays AHEAD of the USER-DEFINED branch below, and that order is the
	// whole content of one half of #1138. A domain whose base type is itself
	// user-defined is reported by information_schema with data_type
	// "USER-DEFINED" and udt_name naming the BASE, while domain_name names the
	// domain -- so with the branches the other way round the domain was
	// flattened to its base and the CHECK it carries was silently dropped.
	// Measured on PostgreSQL 17, one cluster, two domains that differ only in
	// what they are built on:
	//
	//	CREATE DOMAIN point3d AS cube CHECK (cube_dim(VALUE) = 3);
	//	CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);
	//
	//	column      data_type      udt_name   domain_name   format_type
	//	c_point3d   USER-DEFINED   cube       point3d       point3d
	//	c_domain    integer        int4       positive_int  positive_int
	//
	// Before this, c_point3d inspected as `cube` and c_domain as
	// `positive_int`: applying that document back built the column as a bare
	// cube, so the domain and its constraint were gone from the database with
	// nothing reported. The pinned community binary v1.3.0 renders
	// `sql("point3d")` for the same column, so this is also the compatible
	// answer. The same split is visible on stock extension domains -- `lo`
	// (over oid) survived while `earth` (over cube) did not.
	if dbColumn.FormattedType != "" {
		return dbColumn.FormattedType
	}
	if strings.EqualFold(dbColumn.DataType, "USER-DEFINED") && dbColumn.UDTName != "" {
		return dbColumn.UDTName
	}
	if dbColumn.ColumnType != "" {
		return dbColumn.ColumnType
	}
	if sizedType := sizedType(dbColumn); sizedType != "" {
		return sizedType
	}
	return dbColumn.DataType
}

// serialType reports the SERIAL shorthand a column can be written back
// as, or "" when it cannot.
//
// A domain column can never be written back as SERIAL. PostgreSQL's SERIAL
// shorthand only ever builds a column of an integer type, so spelling a column
// of domain `positive` as SERIAL rebuilds it as a plain integer and drops the
// domain's CHECK with it. The domain wins, and the sequence default it was
// drawing from is then carried as an ordinary default rather than folded into
// the shorthand. Measured on PostgreSQL 17.10 against `id positive DEFAULT
// nextval('s')` with the sequence OWNED BY that column: the pinned binary
// v1.3.0 reports `type = sql("positive")` with the nextval default beside it,
// and Ptah reported `type = serial` with no default at all. See
// stokaro/ptah#1242.
func serialType(dbColumn catalog.Column) string {
	if dbColumn.DomainName != "" {
		return ""
	}
	if !dbColumn.IsAutoIncrement || dbColumn.ColumnDefault == nil ||
		!strings.Contains(strings.ToLower(*dbColumn.ColumnDefault), "nextval(") {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(dbColumn.DataType)) {
	case "smallint":
		return "SMALLSERIAL"
	case "integer":
		return "SERIAL"
	case "bigint":
		return "BIGSERIAL"
	default:
		return ""
	}
}

// sizedType renders the width a read carried in a field of its own.
//
// Every family PostgreSQL keeps a width for belongs here, and the two bit ones
// were missing. `ptah schema inspect` wrote a `bit(4)` column as `bit`, and
// replaying that document into a fresh database produced `bit(1)` -- measured
// on PostgreSQL 17.11, three bits of every value gone. A `bit varying(8)` came
// back unlimited, and applying the document to the SOURCE database removed the
// declared width from the live column (stokaro/ptah#2034).
func sizedType(dbColumn catalog.Column) string {
	dataType := strings.ToLower(strings.TrimSpace(dbColumn.DataType))
	switch dataType {
	case "character varying", "varchar":
		if dbColumn.CharacterMaxLength != nil {
			return fmt.Sprintf("VARCHAR(%d)", *dbColumn.CharacterMaxLength)
		}
	case "character", "char":
		if dbColumn.CharacterMaxLength != nil {
			return fmt.Sprintf("CHAR(%d)", *dbColumn.CharacterMaxLength)
		}
	case "bit":
		if dbColumn.CharacterMaxLength != nil {
			return fmt.Sprintf("BIT(%d)", *dbColumn.CharacterMaxLength)
		}
	case "bit varying", "varbit":
		// Lower case, unlike the arms around it. Those are modeled HCL type
		// names that the renderer lower-cases on the way out; this one is not
		// writable bare -- two identifiers separated by a space is not one HCL
		// expression -- so it reaches the document through sql() carrying
		// whatever case it has here, and that binary's type names are case
		// sensitive.
		if dbColumn.CharacterMaxLength != nil {
			return fmt.Sprintf("bit varying(%d)", *dbColumn.CharacterMaxLength)
		}
	case "numeric", "decimal":
		if dbColumn.NumericPrecision != nil && dbColumn.NumericScale != nil {
			return fmt.Sprintf("NUMERIC(%d,%d)", *dbColumn.NumericPrecision, *dbColumn.NumericScale)
		}
		if dbColumn.NumericPrecision != nil {
			return fmt.Sprintf("NUMERIC(%d)", *dbColumn.NumericPrecision)
		}
	}
	return ""
}

func setDefault(field *schemamodel.Field, defaultSQL, dialect string) {
	if platform.NormalizeDialect(dialect) == platform.YDB {
		if value, isLiteral := ydbtype.LiteralValue(defaultSQL); isLiteral {
			field.Default = value
			field.DefaultSet = true
			return
		}
	}
	if sqlutil.DefaultLooksLikeExpression(defaultSQL) {
		field.DefaultExpr = defaultSQL
		return
	}
	field.Default = defaultSQL
}

// IsNotNullRow reports whether a CHECK row a database reported is a column's
// NOT NULL rather than a CHECK its author wrote.
//
// PostgreSQL 17 and earlier list every NOT NULL as a CHECK in
// information_schema, named `<n>_<n>_<n>_not_null` and holding exactly
// `<column> IS NOT NULL`, and PostgreSQL 18 keeps a NOT NULL as a constraint
// with no condition. Such a row is the column's nullability, which the column
// already carries. A CHECK the author named with the same suffix is not: the
// row `p_not_null CHECK (p > 0)` is a CHECK like any other, and taking it for a
// NOT NULL leaves it out of every comparison, so each plan adds it again
// (stokaro/ptah#3935). The row is told apart by its name and by what it holds,
// never by the name alone.
//
// The conversion to the desired-schema model and the schema comparison both
// ask this, and must answer alike: a row one of them takes for a NOT NULL and
// the other for a CHECK is written by one side and dropped by the other.
func IsNotNullRow(constraint catalog.Constraint) bool {
	if !strings.EqualFold(strings.TrimSpace(constraint.Type), "CHECK") ||
		!strings.HasSuffix(constraint.Name, "_not_null") {
		return false
	}
	if constraint.CheckClause == nil || strings.TrimSpace(*constraint.CheckClause) == "" {
		return true
	}
	clause := strings.ToUpper(strings.TrimSpace(*constraint.CheckClause))
	return strings.HasSuffix(clause, " IS NOT NULL") && strings.Count(clause, " IS NOT NULL") == 1
}
