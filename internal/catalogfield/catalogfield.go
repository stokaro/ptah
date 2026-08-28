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

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/core/sqlutil"
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
}

// Options is what a column cannot answer about itself.
type Options struct {
	// CoveredByTablePrimaryKey reports whether a TABLE-level primary key
	// already names this column, which is why a column's own IsPrimaryKey does
	// not become Primary: the two would render the key twice.
	CoveredByTablePrimaryKey bool

	// ForeignKey is the column's own foreign key, or nil.
	ForeignKey *ForeignKey
}

// Field describes one reported column as a field of the desired-schema model.
//
// StructName and FieldName are left empty: they name the Go source a field was
// parsed from, and a column the database reported was not parsed from any.
func Field(column catalog.Column, opts Options) schemamodel.Field {
	field := schemamodel.Field{
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
		Charset:               column.Charset,
		Collate:               column.Collate,
		GeneratedKind:         column.GeneratedKind,
		UpdateExpression:      column.UpdateExpression,
		IdentityGeneration:    column.IdentityGeneration,
		IdentityStart:         column.IdentityStart,
		IdentityIncrement:     column.IdentityIncrement,
	}
	if column.GeneratedExpression != nil {
		field.GeneratedExpression = *column.GeneratedExpression
	}
	if column.ColumnDefault != nil && serialType(column) == "" {
		setDefault(&field, *column.ColumnDefault)
	}
	if opts.ForeignKey != nil {
		field.Foreign = opts.ForeignKey.Reference
		field.ForeignKeyName = opts.ForeignKey.Name
		field.OnDelete = opts.ForeignKey.OnDelete
		field.Deferrable = opts.ForeignKey.Deferrable
		field.Initially = opts.ForeignKey.Initially
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

func setDefault(field *schemamodel.Field, defaultSQL string) {
	if sqlutil.DefaultLooksLikeExpression(defaultSQL) {
		field.DefaultExpr = defaultSQL
		return
	}
	field.Default = defaultSQL
}
