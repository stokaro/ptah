package sqlschema

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/autoincrement"
	"ptah.run/internal/pgname"
	"ptah.run/internal/tableref"
)

// applyAlterSequence applies `ALTER SEQUENCE ... OWNED BY` to the sequence an
// earlier statement declared: the column it names becomes the owner, and
// NONE takes the owner away.
//
// Measured on PostgreSQL 18.6, the owner is a column of a table in the
// sequence's own schema (`sequence must be in same schema as table it is
// linked to`), so the owner is recorded as `table.column`, the form a read of
// the database reports. A table or a column the schema does not declare is
// refused, as the server refuses a relation that does not exist.
func applyAlterSequence(database *schemamodel.Database, document *Document, node *ast.AlterSequenceNode, sourcePlatform string) error {
	written := schemaQualified(node.Schema, node.Name)
	if node.AsType != "" || node.Start != nil || node.Increment != nil || node.MinValue != nil ||
		node.MaxValue != nil || node.Cache != nil || node.Cycle != nil || node.OwnedBy == "" {
		return fmt.Errorf("%w: ALTER SEQUENCE %s: a schema file may set a sequence's owner this way; "+
			"declare its options in its CREATE SEQUENCE", ErrUnmodeledStatement, written)
	}
	databases := declaringDatabases(database, document.base)
	sequence := resolveSequence(databases, schemamodel.Sequence{
		Schema: normalizeSQLIdentifier(sourcePlatform, node.Schema),
		Name:   normalizeSQLIdentifier(sourcePlatform, node.Name),
	}, sourcePlatform)
	if sequence == nil {
		return undeclared(fmt.Errorf("%w: ALTER SEQUENCE %s names a sequence this schema does not declare",
			ErrUnmodeledStatement, written))
	}
	if node.OwnedBy == "NONE" {
		sequence.OwnedBy = ""
		return nil
	}
	owner, err := readOwner(database, document, "ALTER SEQUENCE "+written, sequence.Schema, node.OwnedBy, sourcePlatform)
	if err != nil {
		return err
	}
	sequence.OwnedBy = owner
	return nil
}

// errOwnerWithoutTable and errOwnerInAnotherSchema refuse an OWNED BY clause
// the server refuses, measured on PostgreSQL 16.16, 17.11 and 18.6: a column
// named without its table (`invalid OWNED BY option`), and a column of a table
// in a schema other than the sequence's (`sequence must be in same schema as
// table it is linked to`). CockroachDB v25.4.20 accepts a table in another
// schema, and the read refuses it there too, which is stricter than that
// server.
//
// errOwnerInADatabase refuses a column a database name qualifies. PostgreSQL
// 18.6 takes `db.public.t.id` where db is the database it is connected to and
// refuses any other (`cross-database references are not implemented`), and a
// schema file does not say which database it is read for.
var (
	errOwnerWithoutTable    = errors.New("OWNED BY names a column of a table, or NONE")
	errOwnerInAnotherSchema = errors.New("a sequence is owned by a column of a table in its own schema")
	errOwnerInADatabase     = errors.New("OWNED BY names a column as table.column or schema.table.column")
)

// ownedByColumn splits the column an OWNED BY clause names, `table.column` or
// `schema.table.column`, into the table, qualified as written, and the column.
// Each part keeps its quotes.
func ownedByColumn(ownedBy string) (table, column string, err error) {
	parts := splitSQLIdentifier(ownedBy)
	switch {
	case len(parts) < 2:
		return "", "", errOwnerWithoutTable
	case len(parts) > 3:
		return "", "", errOwnerInADatabase
	}
	return strings.Join(parts[:len(parts)-1], "."), parts[len(parts)-1], nil
}

// readOwner reads the column an OWNED BY clause names as the owner of a
// sequence in sequenceSchema. statement is how a refusal names the statement,
// `CREATE SEQUENCE s`.
//
// CREATE SEQUENCE and ALTER SEQUENCE take the clause in one grammar, so both
// read it here. Read two ways, one owner is recorded two ways, and only one of
// them matches the column a read of the database reports: read as a table
// name, `public.t.id` is the column `id` of a table `public.t`, and a plan
// against the database that holds the sequence plans `ALTER SEQUENCE ...
// OWNED BY "public.t"."id"`, which the server refuses.
//
// The table and the column are found as an ALTER TABLE finds them, so a table
// or a column the schema does not declare is refused, as PostgreSQL 16.16,
// 17.11 and 18.6 refuse a relation or a column that does not exist, a table the
// file creates after the sequence included. A schema written or left out on
// either side is the dialect's default schema, as [schemaOf] reads it, and a
// table in another schema is refused, as the server refuses it.
func readOwner(database *schemamodel.Database, document *Document, statement, sequenceSchema, ownedBy, sourcePlatform string) (string, error) {
	tableName, columnName, err := ownedByColumn(ownedBy)
	if err != nil {
		return "", fmt.Errorf("%s OWNED BY %s: %w", statement, ownedBy, err)
	}
	target, found := findAlterTarget(database, document.base, tableName, sourcePlatform)
	if !found {
		return "", undeclared(fmt.Errorf("%w: %s OWNED BY %s names a table this schema does not declare",
			ErrUnmodeledStatement, statement, ownedBy))
	}
	field := target.field(columnName)
	if field == nil {
		return "", fmt.Errorf("%s OWNED BY %s names a column the table does not declare", statement, ownedBy)
	}
	if schemaOf(target.table.Schema, sourcePlatform) != schemaOf(sequenceSchema, sourcePlatform) {
		return "", fmt.Errorf("%s OWNED BY %s: %w", statement, ownedBy, errOwnerInAnotherSchema)
	}
	return ownerReference(target.table.Name, field.Name), nil
}

// ownerReference is how a sequence records column of table as its owner: the
// two names as [schemamodel.QualifyTableName] writes a schema and a table, so
// a name holding a dot is quoted, `"a.b".id` and `a."x.y"`. The table is in
// the sequence's own schema. A read of the database writes the owner the same
// way, and the renderer reads the quotes back. Joined with a bare dot, the two
// owners are the one string `a.b.id`, which [ownerOf] cannot split and the
// renderer writes as `OWNED BY "a"."b"."id"`.
func ownerReference(table, column string) string {
	return schemamodel.QualifyTableName(table, column)
}

// schemaOf is the schema a name written in schema is in: schema itself, or
// the dialect's default schema for a name written without one.
func schemaOf(schema, sourcePlatform string) string {
	return cmp.Or(schema, factsOf(sourcePlatform).defaultSchema)
}

// resolveSequence returns the declared sequence that a statement naming
// written reaches, looking in each database in order, or nil. written is read
// already, as [toSequence] reads a declaration.
func resolveSequence(databases []*schemamodel.Database, written schemamodel.Sequence, sourcePlatform string) *schemamodel.Sequence {
	key := sequenceKey(written, sourcePlatform)
	for _, database := range databases {
		for i := range database.Sequences {
			if sequenceKey(database.Sequences[i], sourcePlatform) == key {
				return &database.Sequences[i]
			}
		}
	}
	return nil
}

// applyAddIdentity gives a column the identity `ALTER COLUMN ... ADD
// GENERATED ... AS IDENTITY` states, as the same clause written on the column
// gives it: the generation, the start, the increment and the options as
// written.
//
// Measured on PostgreSQL 18.6, the server refuses the statement on a column
// that is already an identity (`is already an identity column`), one with a
// default, which a serial column has (`already has a default value`), a
// nullable one (`must be declared NOT NULL before identity can be added`),
// and one of any type but smallint, integer and bigint, a domain over one
// included (`identity column type must be smallint, integer, or bigint`).
// The read refuses each of them too.
func applyAddIdentity(target alterTarget, field *schemamodel.Field, operation *ast.AlterColumnOperation) error {
	refuse := func(reason string) error {
		return fmt.Errorf("ALTER TABLE %s ALTER COLUMN %s ADD GENERATED: %s", target.written, operation.ColumnName, reason)
	}
	switch {
	case field.IdentityGeneration != "":
		return refuse("the column is an identity column already")
	case field.DefaultSet || field.Default != "" || field.DefaultExpr != "" || autoincrement.SerialType(field.Type):
		return refuse("the column has a default, and an identity column takes none")
	case field.Nullable && !target.isPrimaryKeyColumn(field):
		return refuse("the column has to be NOT NULL to take an identity")
	case integerTypeOf(field.Type) == "":
		return refuse("an identity column is smallint, integer or bigint")
	}
	field.IdentityGeneration = operation.IdentityGeneration
	field.IdentityStart = operation.IdentityStart
	field.IdentityIncrement = operation.IdentityIncrement
	field.IdentityOptions = operation.IdentityOptions
	field.AutoInc = true
	if factsOf(target.sourcePlatform).columns.IdentityNotNull {
		field.Nullable = false
	}
	return nil
}

// integerTypeOf is the integer type a column type names, smallint, integer
// or bigint, or "" for any other type.
func integerTypeOf(columnType string) string {
	switch strings.ToLower(strings.TrimSpace(columnType)) {
	case "smallint", "int2":
		return "smallint"
	case "integer", "int", "int4":
		return "integer"
	case "bigint", "int8":
		return "bigint"
	default:
		return ""
	}
}

// serialTypes are the serial type that builds a column of each integer type,
// and the largest value that type holds, which is the MAXVALUE of the
// sequence the serial type builds.
var serialTypes = map[string]struct {
	serial   string
	maxValue int64
}{
	"smallint": {serial: "smallserial", maxValue: math.MaxInt16},
	"integer":  {serial: "serial", maxValue: math.MaxInt32},
	"bigint":   {serial: "bigserial", maxValue: math.MaxInt64},
}

// nextvalOf matches a column default that draws from one sequence the way the
// server ties the two: nextval of a regclass constant, written with or without
// the cast. Under a `::text` cast the name is looked up on each call, and the
// server records no dependency on the sequence, so the default is an ordinary
// default.
var nextvalOf = regexp.MustCompile(`(?i)^nextval\('((?:[^']|'')+)'(?:::regclass)?\)$`)

// foldColumnSequences folds each sequence the serial type builds for a column
// into the column, once a document's statements are read, on a dialect whose
// serial column draws from a sequence it owns, as the descriptor's
// Comparison.SerialOwnsSequence states.
//
// pg_dump writes a serial column as four statements: the column with a plain
// integer type, CREATE SEQUENCE, ALTER SEQUENCE ... OWNED BY the column, and
// SET DEFAULT nextval of the sequence. They build one column, the one `id
// serial` builds, and a read of the database reports it so: the column, and no
// sequence of its own ([catalog.Column.OwnedSequence]). So the column takes
// its serial type and the sequence leaves the declarations, and the file reads
// as the model `id serial` reads as. Kept as four, the file plans CREATE
// SEQUENCE for the sequence the database it was dumped from holds.
//
// A sequence is folded only where the serial type builds it as written: the
// column NOT NULL, of an integer type, drawing its default from the sequence
// that column owns; the sequence of the column's type, under the name the
// serial type gives it, with every option the server's default and no
// comment. Any other stays a sequence of its own, with its owner and its
// options, so nothing the file states is dropped.
func (d *Document) foldColumnSequences(database *schemamodel.Database, sourcePlatform string) {
	if !factsOf(sourcePlatform).serialOwnsSequence {
		return
	}
	databases := declaringDatabases(database, d.base)
	for _, holder := range databases {
		kept := slices.DeleteFunc(holder.Sequences, func(sequence schemamodel.Sequence) bool {
			field, serial := serialColumnOf(databases, sequence, sourcePlatform)
			if field == nil {
				return false
			}
			field.Type = serial
			field.DefaultExpr = ""
			return true
		})
		// A file that declares its serial columns in one statement each
		// declares no sequence, and so holds none rather than an empty list.
		if len(kept) == 0 {
			kept = nil
		}
		holder.Sequences = kept
	}
}

// serialColumnOf returns the column whose serial type builds sequence, and
// that type, or nil where [Document.foldColumnSequences] keeps the sequence.
func serialColumnOf(databases []*schemamodel.Database, sequence schemamodel.Sequence, sourcePlatform string) (*schemamodel.Field, string) {
	if sequence.Comment != "" || !sequence.Facets.IsZero() || len(sequence.Dialects) > 0 {
		return nil, ""
	}
	table, field := ownerOf(databases, sequence, sourcePlatform)
	if field == nil || (field.Nullable && !field.Primary && !slices.Contains(table.PrimaryKey, field.Name)) {
		return nil, ""
	}
	// A column of another type has no serial type, and no sequence holds the
	// options of one: a sequence's type is an integer type.
	integer := integerTypeOf(field.Type)
	serial := serialTypes[integer]
	if !drawsFrom(field.DefaultExpr, sequence, sourcePlatform) ||
		sequence.Name != pgname.Sequence(table.Name, field.Name) || !serialOptions(sequence, integer, serial.maxValue) {
		return nil, ""
	}
	return field, serial.serial
}

// ownerOf returns the table and the column that own sequence, or nil for
// each where the sequence has no owner the schema declares.
//
// The owner is held as [ownerReference] writes it, a table of the sequence's
// own schema and its column, with each name as the model holds it, so it is
// split by its quotes and matched against the model's names as they are rather
// than read again as SQL: read again, the unquoted `Order` of `Order.Id` would
// fold to `order`.
func ownerOf(databases []*schemamodel.Database, sequence schemamodel.Sequence, sourcePlatform string) (*schemamodel.Table, *schemamodel.Field) {
	owner, owned := tableref.Parse(sequence.OwnedBy)
	if !owned || !owner.Qualified {
		return nil, nil
	}
	tableName, columnName := owner.Schema, owner.Name
	for _, database := range databases {
		for i := range database.Tables {
			table := &database.Tables[i]
			if table.Name != tableName || schemaOf(table.Schema, sourcePlatform) != schemaOf(sequence.Schema, sourcePlatform) {
				continue
			}
			fields, names := tableColumns(databases, table.StructName)
			if index := slices.Index(names, columnName); index >= 0 {
				return table, fields[index]
			}
			return table, nil
		}
	}
	return nil, nil
}

// drawsFrom reports whether a column default is nextval of sequence.
func drawsFrom(defaultExpr string, sequence schemamodel.Sequence, sourcePlatform string) bool {
	drawn := nextvalOf.FindStringSubmatch(strings.TrimSpace(defaultExpr))
	if drawn == nil {
		return false
	}
	schema, name := normalizeSQLTableIdentifier(sourcePlatform, strings.ReplaceAll(drawn[1], "''", "'"))
	return sequenceKey(schemamodel.Sequence{Schema: schema, Name: name}, sourcePlatform) == sequenceKey(sequence, sourcePlatform)
}

// serialOptions reports whether sequence holds the options a serial type
// gives the sequence of a column of the integer type: that type, which a
// sequence that states none takes only when it is bigint, start and minimum
// 1, increment and cache 1, the type's largest value as the maximum, and no
// cycle. An option the declaration leaves out is the server's default.
func serialOptions(sequence schemamodel.Sequence, integer string, maxValue int64) bool {
	canonical := sequence
	canonical.Canonicalize()
	if cmp.Or(canonical.AsType, "bigint") != integer || sequence.Cycle {
		return false
	}
	for _, option := range []struct {
		value *int64
		want  int64
	}{
		{sequence.Start, 1}, {sequence.Increment, 1}, {sequence.MinValue, 1}, {sequence.Cache, 1}, {sequence.MaxValue, maxValue},
	} {
		if option.value != nil && *option.value != option.want {
			return false
		}
	}
	return true
}
