package sqlschema

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/columnkey"
	"ptah.run/internal/pgname"
)

// ErrDuplicateConstraintName is the class of a PostgreSQL CREATE TABLE that
// writes a constraint name another constraint of the statement already holds
// when the server builds the written one. The server refuses the statement,
// and a model holding both would describe a database no server builds
// (stokaro/ptah#3858).
//
// Measured on PostgreSQL 18.6, each of these is refused:
//
//	x int UNIQUE, y int, CONSTRAINT c_x_key UNIQUE (y)       relation "c_x_key" already exists
//	a int CHECK (a > 0), CONSTRAINT k_a_check CHECK (a < 9)   check constraint "k_a_check" already exists
//	x int CHECK (x > 0), CONSTRAINT k_x_check UNIQUE (id)     constraint "k_x_check" for relation "k" already exists
//	id int PRIMARY KEY, x int, CONSTRAINT k_pkey UNIQUE (x)   relation "k_pkey" already exists
//	a int, b int, CONSTRAINT d UNIQUE (a), CONSTRAINT d UNIQUE (b)   relation "d" already exists
//
// Written the other way round, where the server builds the written name first,
// the statement is accepted and the derived name is numbered; see
// [nameCreatedConstraints].
var ErrDuplicateConstraintName = errors.New("two constraints of one CREATE TABLE claim the same name")

// createdRange is where one CREATE TABLE's columns and constraints begin in the
// model, and the place of each of its constraints; see [constraintPlaces].
type createdRange struct {
	fields      int
	constraints int
	places      []int
}

// constraintPlaces answers where the body of one CREATE TABLE writes each
// constraint the statement appended at constraintsStart, as a place: a
// column's own constraints sit at [columnPlace], and a table element written
// after n columns sits between the n-th column and the next. order is what
// [declaredOrder] handed back.
func constraintPlaces(order []namedElement, constraintsStart, count int) []int {
	places := make([]int, count)
	for _, element := range order {
		if element.constraint != noPosition {
			places[element.constraint-constraintsStart] = 2*element.columnsBefore - 1
		}
	}
	return places
}

// columnPlace answers the place of the constraints the column at position of
// the statement's columns declares. A constraint read from the column onto the
// table, such as a named UNIQUE or a second CHECK, is written after the column
// and sits after its place.
func columnPlace(position int) int {
	return 2 * position
}

// createdConstraint is one constraint a PostgreSQL CREATE TABLE declares, with
// what the naming pass needs to settle its name.
type createdConstraint struct {
	// place orders the constraints of one phase as the statement writes them.
	place int
	// written is the name the statement gives the constraint, empty for none.
	written string
	// derive answers the name the server gives the constraint when it has
	// none, avoiding every name taken answers true for.
	derive func(taken func(string) bool) string
	// keep records a derived name in the model, and is nil where the model
	// keeps none: for a primary key.
	keep func(string)
	// indexed reports whether the server builds an index for the constraint,
	// whose name is then a relation of the schema as well.
	indexed bool
	// what names the constraint in an error.
	what string
}

// createdNames is what one PostgreSQL CREATE TABLE's naming pass knows: the
// names the schema held before the statement, and the ones its own
// constraints took so far.
type createdNames struct {
	constraints pgname.Names
	relations   pgname.Names
	// held maps a name one of the statement's constraints took to that
	// constraint, for an error to name.
	held map[string]string
	// table is the table the statement creates, for an error to name.
	table string
}

// nameCreatedConstraints gives every unnamed CHECK, UNIQUE, EXCLUDE and
// FOREIGN KEY one PostgreSQL CREATE TABLE declares the name the server gives
// it, and refuses a written name the server refuses.
//
// The name has to be decided on the desired model, because the other side of a
// comparison is a catalog, and a catalog holds the name the server chose. A
// schema file compared with the database its own SQL created otherwise kept the
// server's `<table>_<column>_fkey` and added a second, identical key named
// Ptah's `fk_<table>_<column>` beside it, and dropped the server's
// `<table>_<columns>_key` to add the same UNIQUE back without a name, which the
// server named again (stokaro/ptah#3643). Left unnamed, a CHECK never pairs
// with the server's `<table>_check`, and the comparison drops the server's
// CHECK to add the same one back (stokaro/ptah#3729), and so does an EXCLUDE
// with the server's `<table>_<elements>_excl` (stokaro/ptah#3749).
//
// The order is the server's. Measured on PostgreSQL 18.6, it builds the table's
// CHECK constraints first, then its written NOT NULL constraint names, then the
// primary key wherever the statement writes it, then every UNIQUE and EXCLUDE,
// a column's own UNIQUE among them, then the foreign keys. Within each phase
// it follows the statement, a column's constraints at the column's place: `CHECK
// (a > 0), a int CHECK (a < 10)` names `a > 0` h_a_check, and `UNIQUE NULLS NOT
// DISTINCT (a), a int UNIQUE` gives the table's key u_a_key and the column's
// u_a_key1.
//
// A derived name avoids every name the schema holds when the server builds its
// constraint, and nothing the statement writes later. A name the statement
// writes is refused when a constraint the server builds before it already
// holds it; see [ErrDuplicateConstraintName]. So `CONSTRAINT c_x_key UNIQUE
// (y), x int UNIQUE` names the column's key c_x_key1, and `x int UNIQUE, y
// int, CONSTRAINT c_x_key UNIQUE (y)` is refused, as the server refuses it.
//
// The primary key takes its name here and keeps none in the model. So does a
// column-level UNIQUE the server names `<table>_<column>_key`: the comparison
// derives the name again; see [columnkey.Name]. A column-level UNIQUE the
// server numbers, because a constraint it builds first holds that name, is
// read as a UNIQUE on the table under the numbered name, as a named column
// UNIQUE is. A render writes the column's own constraints before the table's,
// so left on the column, the key would take the first name there and the
// constraint that holds it would be refused: `UNIQUE NULLS NOT DISTINCT (a), a
// int UNIQUE` builds u_a_key and u_a_key1, and `a int UNIQUE, CONSTRAINT u_a_key
// UNIQUE NULLS NOT DISTINCT (a)` does not build.
//
// A derived NOT NULL name is left out, because PostgreSQL keeps one only from
// version 18, and a written one is refused by no version before it.
func nameCreatedConstraints(
	database, base *schemamodel.Database,
	table schemamodel.Table,
	created createdRange,
	sourcePlatform string,
) error {
	if !namesConstraintsLikePostgres(sourcePlatform) {
		return nil
	}
	names := statementNames(database, base, table, created)
	statement := &createdStatement{
		table:       table,
		fields:      database.Fields[created.fields:],
		constraints: database.Constraints[created.constraints:],
		places:      created.places,
	}
	if err := names.settle(statement.checks()); err != nil {
		return err
	}
	for _, field := range statement.fields {
		names.constraints.Claim(field.NotNullConstraintName)
	}
	if err := names.settle(statement.primaryKey()); err != nil {
		return err
	}
	if err := names.settle(statement.keys()); err != nil {
		return err
	}
	if err := names.settle(statement.foreignKeys()); err != nil {
		return err
	}
	for _, key := range statement.numbered {
		field := &database.Fields[created.fields+key.field]
		field.Unique = false
		database.Constraints = append(database.Constraints,
			numberedKeyOnTable(table.StructName, table.QualifiedName(), field.Name, key.name))
	}
	return nil
}

// numberedKeyOnTable is the UNIQUE on the table that a column's own UNIQUE the
// server numbers is read as; see [nameCreatedConstraints]. qualified is the
// table as the read resolved it.
func numberedKeyOnTable(structName, qualified, column, name string) schemamodel.Constraint {
	return schemamodel.Constraint{
		StructName: structName,
		Name:       name,
		Type:       "UNIQUE",
		Table:      normalizeSQLTableReference("", qualified),
		Columns:    []string{column},
	}
}

// numberedAddedColumnKey answers the UNIQUE on the table that the own UNIQUE of
// a column an ALTER TABLE adds is read as, and false when the server does not
// number the key. It is numbered where a relation or a constraint of the schema
// holds `<table>_<column>_key`: measured on PostgreSQL 18.6, `ALTER TABLE c ADD
// COLUMN x int UNIQUE` beside an index c_x_key over y names the key c_x_key1.
// See [nameCreatedConstraints] for why the model keeps such a key on the table.
// field is not in the model yet, so its own name is not counted as taken.
func numberedAddedColumnKey(field schemamodel.Field, target alterTarget) (schemamodel.Constraint, bool) {
	if !namesConstraintsLikePostgres(target.sourcePlatform) || !field.Unique || strings.TrimSpace(field.UniqueExpr) != "" {
		return schemamodel.Constraint{}, false
	}
	schema, table := splitQualifiedTable(target.qualified)
	constraints := constraintNamesInSchema(target.databases, schema)
	relations := pgname.RelationNames(target.databases, schema)
	name, _ := columnkey.Name(platform.Postgres, table, field.Name, func(candidate string) bool {
		return constraints.Taken(candidate) || relations.Taken(candidate)
	})
	if name == pgname.ColumnKey(table, field.Name) {
		return schemamodel.Constraint{}, false
	}
	return numberedKeyOnTable(target.structName, target.qualified, field.Name, name), true
}

// statementNames collects the names the schema of table holds before the
// statement that creates it: every name this file and the earlier ones give,
// the statement's own constraints left out, and the table itself.
func statementNames(
	database, base *schemamodel.Database, table schemamodel.Table, created createdRange,
) *createdNames {
	earlier := *database
	earlier.Tables = database.Tables[:len(database.Tables)-1]
	earlier.Fields = database.Fields[:created.fields]
	earlier.Constraints = database.Constraints[:created.constraints]
	databases := []*schemamodel.Database{&earlier, base}
	relations := pgname.RelationNames(databases, table.Schema)
	relations.Claim(table.Name)
	return &createdNames{
		constraints: constraintNamesInSchema(databases, table.Schema),
		relations:   relations,
		held:        make(map[string]string),
		table:       table.QualifiedName(),
	}
}

// settle names the constraints of one phase in the order the statement writes
// them.
func (n *createdNames) settle(constraints []createdConstraint) error {
	slices.SortStableFunc(constraints, func(a, b createdConstraint) int { return cmp.Compare(a.place, b.place) })
	for _, constraint := range constraints {
		name := constraint.written
		if name != "" {
			if holder, held := n.held[name]; held {
				return fmt.Errorf("%w: CREATE TABLE %s gives the name %s to %s, which %s already holds "+
					"when the server builds it, and PostgreSQL refuses the statement; name one of them differently",
					ErrDuplicateConstraintName, n.table, name, constraint.what, holder)
			}
		} else {
			name = constraint.derive(n.taken(constraint.indexed))
			if constraint.keep != nil {
				constraint.keep(name)
			}
		}
		n.constraints.Claim(name)
		if constraint.indexed {
			n.relations.Claim(name)
		}
		n.held[name] = constraint.what
	}
	return nil
}

// taken answers whether a derived name is taken: in the constraint namespace
// of the schema, and for a constraint with an index, in its relation namespace
// too. A CHECK has no index behind it, so a relation does not constrain it:
// measured, an index already called `l_a_check` leaves `CHECK (a > 0)` on `l`
// named `l_a_check`.
func (n *createdNames) taken(indexed bool) func(string) bool {
	return func(candidate string) bool {
		return n.constraints.Taken(candidate) || (indexed && n.relations.Taken(candidate))
	}
}

// createdStatement is one PostgreSQL CREATE TABLE as the model holds it: the
// table, the columns it declares, and its constraints with their places.
type createdStatement struct {
	table       schemamodel.Table
	fields      []schemamodel.Field
	constraints []schemamodel.Constraint
	places      []int
	// numbered holds the column-level UNIQUEs the server numbers, to be read
	// as UNIQUEs on the table once every name is settled. They are appended
	// afterwards because an append can move the constraints the naming pass
	// holds.
	numbered []numberedColumnKey
}

// numberedColumnKey is a column-level UNIQUE the server names past
// `<table>_<column>_key`: the column's position among the statement's columns,
// and the name.
type numberedColumnKey struct {
	field int
	name  string
}

// columns answers the names of the statement's columns, in order.
func (s *createdStatement) columns() []string {
	columns := make([]string, 0, len(s.fields))
	for _, field := range s.fields {
		columns = append(columns, field.Name)
	}
	return columns
}

// checks lists the statement's CHECK constraints, on its columns and on the
// table. See [pgname.Check] for the name the server derives.
func (s *createdStatement) checks() []createdConstraint {
	columns := s.columns()
	var checks []createdConstraint
	for i := range s.fields {
		field := &s.fields[i]
		if field.Check == "" {
			continue
		}
		checks = append(checks, createdConstraint{
			place:   columnPlace(i),
			written: field.CheckName,
			derive: func(taken func(string) bool) string {
				return pgname.Check(s.table.Name, field.Check, columns, taken)
			},
			keep: func(name string) { field.CheckName = name },
			what: fmt.Sprintf("the CHECK (%s) of column %s", field.Check, field.Name),
		})
	}
	for i := range s.constraints {
		constraint := &s.constraints[i]
		if !isCheck(*constraint) {
			continue
		}
		checks = append(checks, createdConstraint{
			place:   s.places[i],
			written: constraint.Name,
			derive: func(taken func(string) bool) string {
				return pgname.Check(s.table.Name, constraint.CheckExpression, columns, taken)
			},
			keep: func(name string) { constraint.Name = name },
			what: fmt.Sprintf("the CHECK (%s)", constraint.CheckExpression),
		})
	}
	return checks
}

// primaryKey lists the statement's primary key, when it declares one. The
// model keeps no derived name for it.
func (s *createdStatement) primaryKey() []createdConstraint {
	declared := len(s.table.PrimaryKey) > 0 || len(s.table.PrimaryKeyParts) > 0 ||
		slices.ContainsFunc(s.fields, func(field schemamodel.Field) bool { return field.Primary })
	if !declared {
		return nil
	}
	return []createdConstraint{{
		written: s.table.PrimaryKeyName,
		derive: func(taken func(string) bool) string {
			return pgname.PrimaryKey(s.table.Name, taken)
		},
		indexed: true,
		what:    "the primary key",
	}}
}

// keys lists the statement's UNIQUE and EXCLUDE constraints, a column's own
// UNIQUE among them. A column's own UNIQUE keeps its derived name in the model
// only when the server numbers it; see [nameCreatedConstraints].
func (s *createdStatement) keys() []createdConstraint {
	var keys []createdConstraint
	for i, field := range s.fields {
		if !field.Unique || strings.TrimSpace(field.UniqueExpr) != "" {
			continue
		}
		keys = append(keys, createdConstraint{
			place: columnPlace(i),
			derive: func(taken func(string) bool) string {
				name, _ := columnkey.Name(platform.Postgres, s.table.Name, field.Name, taken)
				return name
			},
			keep: func(name string) {
				if name != pgname.ColumnKey(s.table.Name, field.Name) {
					s.numbered = append(s.numbered, numberedColumnKey{field: i, name: name})
				}
			},
			indexed: true,
			what:    "the UNIQUE of column " + field.Name,
		})
	}
	for i := range s.constraints {
		constraint := &s.constraints[i]
		if !isIndexedConstraint(*constraint) {
			continue
		}
		keys = append(keys, createdConstraint{
			place:   s.places[i],
			written: constraint.Name,
			derive: func(taken func(string) bool) string {
				return indexedConstraintName(*constraint, s.table.Name, taken)
			},
			keep:    func(name string) { constraint.Name = name },
			indexed: true,
			what:    describeIndexedConstraint(*constraint),
		})
	}
	return keys
}

// foreignKeys lists the statement's foreign keys, on its columns and on the
// table.
func (s *createdStatement) foreignKeys() []createdConstraint {
	var keys []createdConstraint
	for i := range s.fields {
		field := &s.fields[i]
		if field.Foreign == "" {
			continue
		}
		keys = append(keys, createdConstraint{
			place:   columnPlace(i),
			written: field.ForeignKeyName,
			derive: func(taken func(string) bool) string {
				return pgname.Constraint(s.table.Name, []string{field.Name}, foreignKeyLabel, taken)
			},
			keep: func(name string) { field.ForeignKeyName = name },
			what: "the foreign key of column " + field.Name,
		})
	}
	for i := range s.constraints {
		constraint := &s.constraints[i]
		if !isForeignKey(*constraint) {
			continue
		}
		keys = append(keys, createdConstraint{
			place:   s.places[i],
			written: constraint.Name,
			derive: func(taken func(string) bool) string {
				return pgname.Constraint(s.table.Name, constraint.Columns, foreignKeyLabel, taken)
			},
			keep: func(name string) { constraint.Name = name },
			what: fmt.Sprintf("the FOREIGN KEY (%s)", strings.Join(constraint.Columns, ", ")),
		})
	}
	return keys
}

// describeIndexedConstraint names a UNIQUE or an EXCLUDE for an error.
func describeIndexedConstraint(constraint schemamodel.Constraint) string {
	if strings.EqualFold(constraint.Type, "EXCLUDE") {
		return fmt.Sprintf("the EXCLUDE (%s)", constraint.ExcludeElements)
	}
	described := fmt.Sprintf("the UNIQUE (%s)", strings.Join(constraint.Columns, ", "))
	if len(constraint.IncludeColumns) > 0 {
		described += fmt.Sprintf(" INCLUDE (%s)", strings.Join(constraint.IncludeColumns, ", "))
	}
	return described
}
