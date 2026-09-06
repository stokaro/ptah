package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/astbuilder"
	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// The two documents stokaro/ptah#2773 was filed about, and the four that say
// when a foreign key takes part in the index namespace.
//
// Every one was measured on MySQL 26.7.0 and MariaDB 12.3.3 before it was
// written here, and the two engines answered identically on all six:
//
//	CONSTRAINT b FOREIGN KEY (a) ..., KEY (b)   b(a) and b_2(b)
//	KEY (b), CONSTRAINT b FOREIGN KEY (a) ...   ERROR 1061 Duplicate key name 'b'
//	KEY (a), fk on (a), KEY fk1 (b)             a(a) and fk1(b)
//	KEY (a, b), fk on (a), KEY fk1 (b)          a(a,b) and fk1(b)
//	PRIMARY KEY (a), fk on (a), KEY fk1 (b)     PRIMARY(a) and fk1(b)
//	KEY (b, a), fk on (a), KEY fk1 (b)          ERROR 1061 Duplicate key name 'fk1'
//	PRIMARY KEY (b, a), fk on (a), KEY fk1 (b)  ERROR 1061 Duplicate key name 'fk1'
//
// The trailing `KEY fk1 (b)` in the last five is the probe, and it is what
// makes the coverage rule observable at all. A foreign key that claims nothing
// leaves the name free and the document stands; one that claims its own name
// collides with the index written after it, which is the answer both servers
// give when they had to build a backing index of their own.
const (
	orderForeignKeyFirst = "CREATE TABLE c (a INT, b INT, " +
		"CONSTRAINT b FOREIGN KEY (a) REFERENCES p(id), KEY (b));"
	orderIndexFirst = "CREATE TABLE c (a INT, b INT, " +
		"KEY (b), CONSTRAINT b FOREIGN KEY (a) REFERENCES p(id));"
	coveredByItsOwnColumn = "CREATE TABLE c (a INT, b INT, KEY (a), " +
		"CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES p(id), KEY fk1 (b));"
	coveredByALeadingPrefix = "CREATE TABLE c (a INT, b INT, KEY (a, b), " +
		"CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES p(id), KEY fk1 (b));"
	coveredByThePrimaryKey = "CREATE TABLE c (a INT NOT NULL, b INT, PRIMARY KEY (a), " +
		"CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES p(id), KEY fk1 (b));"
	uncoveredByATrailingColumn = "CREATE TABLE c (a INT, b INT, KEY (b, a), " +
		"CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES p(id), KEY fk1 (b));"
	uncoveredByThePrimaryKey = "CREATE TABLE c (a INT NOT NULL, b INT NOT NULL, " +
		"PRIMARY KEY (b, a), CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES p(id), " +
		"KEY fk1 (b));"
)

// TestToDatabase_TheDeclaredOrderDecidesTheNames_HappyPath establishes that the
// document both engines accept converts to the model both engines build from
// it: the foreign key keeps the name its author wrote, and the index declared
// after it takes the suffixed one.
//
// Before the order reached this pass, the constraint and the index were held in
// two slices with no interleaving between them, so this document and the one in
// the failure path below converted identically -- to a model whose emission
// both servers answer with ERROR 1061. Ptah turned a document both engines
// accept into DDL neither can run.
func TestToDatabase_TheDeclaredOrderDecidesTheNames_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := dialectSchema(c, test.dialect, orderForeignKeyFirst)

			c.Assert(err, qt.IsNil)
			c.Assert(indexNames(database), qt.DeepEquals, []string{"b_2"})
			c.Assert(database.Indexes[0].Fields, qt.DeepEquals, []string{"b"})
			c.Assert(database.Constraints, qt.HasLen, 1)
			c.Assert(database.Constraints[0].Type, qt.Equals, "FOREIGN KEY")
			c.Assert(database.Constraints[0].Name, qt.Equals, "b")
			c.Assert(database.Constraints[0].Columns, qt.DeepEquals, []string{"a"})
		})
	}
}

// TestToDatabase_TheDeclaredOrderDecidesTheNames_FailurePath refuses the same
// two elements the other way round.
//
// It is not a stricter reading of a valid document: both engines answer
// `ERROR 1061 (42000): Duplicate key name 'b'` for it, because the unnamed
// `KEY (b)` takes the bare name as soon as it is read and the foreign key
// declared after it needs a backing index of the same name. Refusing at
// conversion reports it where the author can act on it, rather than partway
// through an apply.
func TestToDatabase_TheDeclaredOrderDecidesTheNames_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dialectSchema(c, test.dialect, orderIndexFirst)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrDuplicateIndexName)
			c.Assert(err, qt.ErrorMatches,
				`two indexes on one table claim the same name: b on c`)
		})
	}
}

// TestToDatabase_AForeignKeyCoveredByAnEarlierKeyClaimsNothing_HappyPath
// establishes when a foreign key stays out of the index namespace.
//
// A foreign key needs an index whose leading columns are its own. Where one is
// already declared, both engines reuse it and allocate no name, so a later
// index is free to take the name the constraint carries -- which is what the
// trailing `KEY fk1 (b)` in every row measures. Nothing else in the model would
// show it: a claim that is never rendered is invisible until some other index
// wants the name.
//
// The name the author wrote is asserted beside the index list, because a pass
// that decided a constraint claims nothing has no business blanking it either.
func TestToDatabase_AForeignKeyCoveredByAnEarlierKeyClaimsNothing_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		{
			name:    "mysql: a key on the foreign key's own column",
			dialect: platform.MySQL,
			sql:     coveredByItsOwnColumn,
			want:    []string{"a", "fk1"},
		},
		{
			name:    "mariadb: a key on the foreign key's own column",
			dialect: platform.MariaDB,
			sql:     coveredByItsOwnColumn,
			want:    []string{"a", "fk1"},
		},
		{
			name:    "mysql: a wider key whose leading column is the foreign key's",
			dialect: platform.MySQL,
			sql:     coveredByALeadingPrefix,
			want:    []string{"a", "fk1"},
		},
		{
			name:    "mariadb: a wider key whose leading column is the foreign key's",
			dialect: platform.MariaDB,
			sql:     coveredByALeadingPrefix,
			want:    []string{"a", "fk1"},
		},
		{
			name:    "mysql: the table's own primary key",
			dialect: platform.MySQL,
			sql:     coveredByThePrimaryKey,
			want:    []string{"fk1"},
		},
		{
			name:    "mariadb: the table's own primary key",
			dialect: platform.MariaDB,
			sql:     coveredByThePrimaryKey,
			want:    []string{"fk1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := dialectSchema(c, test.dialect, test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(indexNames(database), qt.DeepEquals, test.want)
			c.Assert(database.Constraints, qt.HasLen, 1)
			c.Assert(database.Constraints[0].Type, qt.Equals, "FOREIGN KEY")
			c.Assert(database.Constraints[0].Name, qt.Equals, "fk1")
		})
	}
}

// TestToDatabase_AForeignKeyNoKeyCoversClaimsItsOwnName_FailurePath is the
// other half of the same rule, and the half a single measurement generalized
// away.
//
// A key on `(b, a)` does not serve a foreign key on `(a)`: the leading columns
// have to be the key's own, in order. Neither does a primary key on `(b, a)`.
// Both engines build a backing index named after the constraint in that case,
// and both then refuse the `KEY fk1 (b)` written after it with
// `ERROR 1061 (42000): Duplicate key name 'fk1'`.
//
// The two rows differ in where the non-covering key came from, because coverage
// is seeded from the table's primary key before any element is walked, and a
// rule that read only the elements would pass the first row while leaving the
// second unmeasured.
func TestToDatabase_AForeignKeyNoKeyCoversClaimsItsOwnName_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{
			name:    "mysql: a key whose leading column is another one",
			dialect: platform.MySQL,
			sql:     uncoveredByATrailingColumn,
		},
		{
			name:    "mariadb: a key whose leading column is another one",
			dialect: platform.MariaDB,
			sql:     uncoveredByATrailingColumn,
		},
		{
			name:    "mysql: a primary key whose leading column is another one",
			dialect: platform.MySQL,
			sql:     uncoveredByThePrimaryKey,
		},
		{
			name:    "mariadb: a primary key whose leading column is another one",
			dialect: platform.MariaDB,
			sql:     uncoveredByThePrimaryKey,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dialectSchema(c, test.dialect, test.sql)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrDuplicateIndexName)
			c.Assert(err, qt.ErrorMatches,
				`two indexes on one table claim the same name: fk1 on c`)
		})
	}
}

// TestToDatabase_ATableWithNoRecordedOrderIsNamedConstraintsThenIndexes is what
// keeps the change from reaching producers that are not the parser.
//
// A table body has a declaration order only where something read one. A node
// assembled in Go carries constraints and indexes and nothing that says which
// came first, and the conversion has to name it anyway -- constraints before
// indexes, which is the order every reader saw before the recorded one existed.
//
// The empty Elements is asserted rather than assumed: it is the premise of the
// whole test, and a node that quietly started recording an order would turn
// this into a test of the other branch without failing.
//
// The unique constraint and the two indexes all derive from the same column, so
// the assertion distinguishes the fallback from its alternative: naming the
// indexes first would hand `a` to an index and `a_3` to the constraint.
func TestToDatabase_ATableWithNoRecordedOrderIsNamedConstraintsThenIndexes(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("u")
	table.AddColumn(ast.NewColumn("a", "INT"))
	table.AddColumn(ast.NewColumn("b", "INT"))
	table.Constraints = []*ast.ConstraintNode{ast.NewUniqueConstraint("", "a")}
	table.Indexes = []*ast.IndexNode{
		ast.NewIndex("", "u", "a"),
		ast.NewIndex("", "u", "a", "b"),
	}
	c.Assert(table.Elements, qt.HasLen, 0)

	database, err := sqlschema.ToDatabase(
		&ast.StatementList{Statements: []ast.Node{table}}, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Constraints, qt.HasLen, 1)
	c.Assert(database.Constraints[0].Name, qt.Equals, "a")
	c.Assert(indexNames(database), qt.DeepEquals, []string{"a_2", "a_3"})
}

// TestToDatabase_ABuilderBuiltTableStillNamesItsUniqueConstraint drives the
// producer named in the fallback's own comment.
//
// A fluent builder is the other way a CreateTableNode is made, and the naming
// pass has to answer for it too. It reaches the ordered walk rather than the
// fallback -- astbuilder adds its constraints through AddConstraint, which is
// what records an element -- and that is the point worth pinning: the two
// producers converge on the same answer, so a table assembled in Go is named
// exactly as the equivalent document is.
//
// Which branch it reaches is asserted rather than described, and it is asserted
// as the identity the ordered walk depends on: the element has to point at the
// very constraint the constraint slice holds, because naming one means writing
// to it. A builder that appended to Constraints itself would leave that element
// behind, and the sentence above would go on reading as though it had not.
func TestToDatabase_ABuilderBuiltTableStillNamesItsUniqueConstraint(t *testing.T) {
	c := qt.New(t)

	table := astbuilder.NewTable("u").
		Column("a", "INT").End().
		Column("b", "INT").End().
		Unique("", "a").
		Build()

	c.Assert(table.Elements, qt.HasLen, 1)
	c.Assert(table.Elements[0].Constraint, qt.Equals, table.Constraints[0])

	database, err := sqlschema.ToDatabase(
		&ast.StatementList{Statements: []ast.Node{table}}, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Constraints, qt.HasLen, 1)
	c.Assert(database.Constraints[0].Type, qt.Equals, "UNIQUE")
	c.Assert(database.Constraints[0].Name, qt.Equals, "a")
}

// orderKeyThenUnique and orderUniqueThenKey are the pair that shows the order
// deciding a name without either document being refused.
//
// Measured on MySQL 26.7.0 and MariaDB 12.3.3, identically on both:
// `KEY (a), UNIQUE (a)` builds a non-unique `a` and a unique `a_2`, and the
// same two elements the other way round build a unique `a` and a non-unique
// `a_2`. Both are accepted, so nothing refuses a conversion that reads them
// alike -- the two models simply describe different databases, and one of them
// describes a database nobody wrote.
//
// The pair the issue was filed about cannot say that. Constraints before
// indexes is the order `CONSTRAINT b FOREIGN KEY (a) ..., KEY (b)` was written
// in, so a pass that ignores the recorded order names it the same way and only
// the reordered document, which both engines refuse, tells the two apart.
const (
	orderKeyThenUnique = "CREATE TABLE c (a INT NOT NULL, b INT, KEY (a), UNIQUE (a));"
	orderUniqueThenKey = "CREATE TABLE c (a INT NOT NULL, b INT, UNIQUE (a), KEY (a));"
)

// TestToDatabase_ReorderingATableBodyMovesTheSuffixedName_HappyPath is the
// happy-path half of the order rule: two documents both engines accept, whose
// catalogs differ in which element got the bare name.
//
// The unique constraint's name is asserted beside the index list because that
// is where the swap lands. A pass that names constraints before indexes gives
// the constraint `a` in both documents, which is right for one of them and a
// database the author did not write for the other.
func TestToDatabase_ReorderingATableBodyMovesTheSuffixedName_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		dialect        string
		sql            string
		wantIndexes    []string
		wantConstraint string
	}{
		{
			name:           "mysql: a key declared before a unique constraint",
			dialect:        platform.MySQL,
			sql:            orderKeyThenUnique,
			wantIndexes:    []string{"a"},
			wantConstraint: "a_2",
		},
		{
			name:           "mariadb: a key declared before a unique constraint",
			dialect:        platform.MariaDB,
			sql:            orderKeyThenUnique,
			wantIndexes:    []string{"a"},
			wantConstraint: "a_2",
		},
		{
			name:           "mysql: the same two elements the other way round",
			dialect:        platform.MySQL,
			sql:            orderUniqueThenKey,
			wantIndexes:    []string{"a_2"},
			wantConstraint: "a",
		},
		{
			name:           "mariadb: the same two elements the other way round",
			dialect:        platform.MariaDB,
			sql:            orderUniqueThenKey,
			wantIndexes:    []string{"a_2"},
			wantConstraint: "a",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := dialectSchema(c, test.dialect, test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(indexNames(database), qt.DeepEquals, test.wantIndexes)
			c.Assert(database.Indexes[0].Fields, qt.DeepEquals, []string{"a"})
			c.Assert(database.Constraints, qt.HasLen, 1)
			c.Assert(database.Constraints[0].Type, qt.Equals, "UNIQUE")
			c.Assert(database.Constraints[0].Name, qt.Equals, test.wantConstraint)
			c.Assert(database.Constraints[0].Columns, qt.DeepEquals, []string{"a"})
		})
	}
}
