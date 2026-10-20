package pgextensionsource_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/sqlschema"
)

// readSQL reads one SQL document of dialect through the bundled runtime's
// owners.
func readSQL(c *qt.C, dialect, text string) (schemamodel.Database, *ast.StatementList, error) {
	c.Helper()
	return sqlschema.ReadWith(c.Context(), []byte(text), dialect, nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
}

// ownedExtensions returns the extensions a document hands to the owner, keyed
// by name.
func ownedExtensions(c *qt.C, db schemamodel.Database) map[string]pgextension.DesiredExtension {
	c.Helper()
	all, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	extensions := make(map[string]pgextension.DesiredExtension, len(all))
	for _, object := range all {
		extensions[pgextension.Name(object.Ref)] = *object.Value.(*pgextension.DesiredExtension)
	}
	return extensions
}

// loadDirectory writes files into a schema directory and loads it for
// PostgreSQL.
func loadDirectory(c *qt.C, files map[string]string) (*schemamodel.Database, error) {
	c.Helper()
	dir := c.TempDir()
	for name, text := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600), qt.IsNil)
	}
	return schemafile.LoadAll(c.Context(), []string{dir}, schemafile.Options{SQL: builtintest.Runtime().SQL(), Dialect: platform.Postgres})
}

// TestSQL_HappyPath pins what the owner reads from a PostgreSQL-family
// document, or one that names no target: each CREATE EXTENSION is an
// extension, with every option the statement may write, a plain name folded
// as the target folds one, and a COMMENT ON EXTENSION amends the extension a
// CREATE before it declared.
func TestSQL_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    map[string]pgextension.DesiredExtension
	}{
		{name: "a bare declaration", dialect: platform.Postgres, sql: "CREATE EXTENSION pg_trgm;",
			want: map[string]pgextension.DesiredExtension{"pg_trgm": {}}},
		{name: "every option", dialect: platform.Postgres,
			sql:  "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA ext VERSION '1.6';",
			want: map[string]pgextension.DesiredExtension{"pg_trgm": {Schema: "ext", Version: "1.6", IfNotExists: true}}},
		{name: "options in the other order", dialect: platform.Postgres, sql: `CREATE EXTENSION hstore VERSION "1.8" SCHEMA "Ext";`,
			want: map[string]pgextension.DesiredExtension{"hstore": {Schema: "Ext", Version: "1.8"}}},
		{name: "a plain name folds", dialect: platform.Postgres, sql: "CREATE EXTENSION PG_TRGM SCHEMA EXT;",
			want: map[string]pgextension.DesiredExtension{"pg_trgm": {Schema: "ext"}}},
		{name: "a quoted name is kept", dialect: platform.Postgres, sql: `CREATE EXTENSION "PG_TRGM";`,
			want: map[string]pgextension.DesiredExtension{"PG_TRGM": {}}},
		{name: "a comment amends the declaration", dialect: platform.Postgres,
			sql:  "CREATE EXTENSION pg_trgm VERSION '1.6';\nCOMMENT ON EXTENSION PG_TRGM IS 'it''s trigrams';",
			want: map[string]pgextension.DesiredExtension{"pg_trgm": {Version: "1.6", Comment: "it's trigrams"}}},
		{name: "an identical declaration twice is one", dialect: platform.Postgres,
			sql:  "CREATE EXTENSION IF NOT EXISTS pg_trgm;\nCREATE EXTENSION IF NOT EXISTS pg_trgm;",
			want: map[string]pgextension.DesiredExtension{"pg_trgm": {IfNotExists: true}}},
		{name: "a CockroachDB document", dialect: platform.CockroachDB, sql: "CREATE EXTENSION PG_TRGM;",
			want: map[string]pgextension.DesiredExtension{"pg_trgm": {}}},
		{name: "a document that names no target", dialect: "", sql: "CREATE EXTENSION PG_TRGM;",
			want: map[string]pgextension.DesiredExtension{"PG_TRGM": {}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := readSQL(c, test.dialect, test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(ownedExtensions(c, db), qt.DeepEquals, test.want)
			c.Assert(db.FeatureCoverage.Lookup(pgextension.ExtensionKind, pgextension.Ref("citext")).State, qt.Equals, schemaext.Complete)
		})
	}
}

// TestSQL_StatementsStandAsTheOwnersOperations pins what a reader of syntax
// sees where each statement stood: the owner's operation, which the owner
// renders.
func TestSQL_StatementsStandAsTheOwnersOperations(t *testing.T) {
	c := qt.New(t)

	_, statements, err := readSQL(c, platform.Postgres,
		"CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA ext;\nCOMMENT ON EXTENSION pg_trgm IS 'trigrams';")

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.DeepEquals, []ast.Node{
		&ast.ExtensionStatement{Payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Create, Schema: "ext", IfNotExists: true}},
		&ast.ExtensionStatement{Payload: &pgextension.CommentOperation{Name: "pg_trgm", Comment: "trigrams"}},
	})
}

// TestSQL_AnotherTargetRefusesTheStatement is the control: the owner does not
// read CREATE EXTENSION in a document that names a target outside the family,
// and the read refuses the statement there, because that target refuses it
// too.
func TestSQL_AnotherTargetRefusesTheStatement(t *testing.T) {
	c := qt.New(t)

	db, statements, err := readSQL(c, platform.MySQL, "CREATE EXTENSION pg_trgm;")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `CREATE EXTENSION pg_trgm: an extension is a PostgreSQL object, and mysql refuses the statement`)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(statements, qt.IsNil)
}

// TestSQL_FailurePath pins the refusals of a statement the owner cannot read,
// positioned in the document, and of a second declaration that differs from
// the first.
func TestSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "no name", sql: "CREATE EXTENSION;",
			wantErr: `expected extension name: expected identifier, got EOF at position 16`},
		{name: "an option the statement does not take", sql: "CREATE TABLE t (id int);\nCREATE EXTENSION pg_trgm CASCADE;",
			wantErr: `unsupported CREATE EXTENSION option "CASCADE" at position 50`},
		{name: "WITH before another word", sql: "CREATE EXTENSION pg_trgm WITH CASCADE;",
			wantErr: `expected SCHEMA or VERSION after WITH in CREATE EXTENSION`},
		{name: "a schema written twice", sql: "CREATE EXTENSION pg_trgm SCHEMA a SCHEMA b;",
			wantErr: `duplicate SCHEMA option in CREATE EXTENSION`},
		{name: "a comment without IS", sql: "CREATE EXTENSION pg_trgm;\nCOMMENT ON EXTENSION pg_trgm 'x';",
			wantErr: `expected IS after COMMENT ON EXTENSION pg_trgm at position 55`},
		{name: "a comment that is not a string", sql: "CREATE EXTENSION pg_trgm;\nCOMMENT ON EXTENSION pg_trgm IS NULL;",
			wantErr: `expected a string constant for the extension comment at position 58`},
		{name: "two declarations that differ", sql: "CREATE EXTENSION pg_trgm VERSION '1.5';\nCREATE EXTENSION pg_trgm VERSION '1.6';",
			wantErr: `duplicate feature identity: conflicting definitions of ptah.run/pgextension/extension pg_trgm`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := readSQL(c, platform.Postgres, test.sql)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

// TestSQL_ACommentOnNothingDeclaredIsRefused pins the refusal of a COMMENT ON
// EXTENSION no CREATE before it declared, which leads with the statement, and
// marks it as a change to an undeclared object.
func TestSQL_ACommentOnNothingDeclaredIsRefused(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "alone", sql: "COMMENT ON EXTENSION citext IS 'x';"},
		{name: "before the declaration", sql: "COMMENT ON EXTENSION citext IS 'x';\nCREATE EXTENSION citext;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := readSQL(c, platform.Postgres, test.sql)

			c.Assert(err, qt.ErrorMatches, `COMMENT ON EXTENSION citext IS 'x': extension "citext" is not declared in this schema`)
			c.Assert(err, qt.ErrorIs, sqlext.ErrUndeclared)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

// TestSQL_ADirectoryIsOneScript pins the files of a schema directory as one
// script: a comment amends the extension an earlier file declared, and a
// later file that declares it again under IF NOT EXISTS, alike, declares
// nothing new.
func TestSQL_ADirectoryIsOneScript(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  map[string]pgextension.DesiredExtension
	}{
		{name: "a comment in a later file", files: map[string]string{
			"001.sql": "CREATE EXTENSION pg_trgm VERSION '1.6';",
			"002.sql": "COMMENT ON EXTENSION pg_trgm IS 'trigrams';\nCREATE TABLE t (id int);",
		}, want: map[string]pgextension.DesiredExtension{"pg_trgm": {Version: "1.6", Comment: "trigrams"}}},
		{name: "a guarded declaration in each file", files: map[string]string{
			"001.sql": "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
			"002.sql": "CREATE EXTENSION IF NOT EXISTS pg_trgm;\nCREATE TABLE t (id int);",
		}, want: map[string]pgextension.DesiredExtension{"pg_trgm": {IfNotExists: true}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := loadDirectory(c, test.files)

			c.Assert(err, qt.IsNil)
			c.Assert(ownedExtensions(c, *db), qt.DeepEquals, test.want)
		})
	}
}

// TestSQL_ADirectoryRefusesASecondDeclaration pins the refusals of a later
// file that declares an extension an earlier file declared: without IF NOT
// EXISTS the server running the files refuses it, and under it a declaration
// that differs is refused rather than one of the two kept in silence.
func TestSQL_ADirectoryRefusesASecondDeclaration(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		wantErr string
	}{
		{name: "without IF NOT EXISTS", files: map[string]string{
			"001.sql": "CREATE EXTENSION pg_trgm;",
			"002.sql": "CREATE EXTENSION pg_trgm;",
		}, wantErr: `read state from "002.sql": extension "pg_trgm" already exists`},
		{name: "IF NOT EXISTS before a plain declaration", files: map[string]string{
			"001.sql": "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
			"002.sql": "CREATE EXTENSION pg_trgm;",
		}, wantErr: `read state from "002.sql": extension "pg_trgm" already exists`},
		{name: "a guarded declaration that differs", files: map[string]string{
			"001.sql": "CREATE EXTENSION IF NOT EXISTS pg_trgm VERSION '1.5';",
			"002.sql": "CREATE EXTENSION IF NOT EXISTS pg_trgm VERSION '1.6';",
		}, wantErr: `.*conflicting definitions of ptah.run/pgextension/extension pg_trgm`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := loadDirectory(c, test.files)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestSQL_ASpannerDocumentMakesBothClaims pins that the owner's claim joins
// the one a Spanner document makes for its row deletion policies, rather than
// replacing it: a document of a family target can carry both.
func TestSQL_ASpannerDocumentMakesBothClaims(t *testing.T) {
	c := qt.New(t)

	db, _, err := readSQL(c, platform.Spanner, "CREATE EXTENSION pg_trgm;")

	c.Assert(err, qt.IsNil)
	kinds := make([]schemaext.Kind, 0, 2)
	for _, record := range db.FeatureCoverage.KindRecords() {
		kinds = append(kinds, record.Model.Kind)
	}
	c.Assert(kinds, qt.Contains, pgextension.ExtensionKind)
	c.Assert(kinds, qt.Contains, spannerschema.RowDeletionKind)
}

// parseSyntax reads one SQL document of dialect into statements through the
// bundled runtime's owners, as migration lint reads a migration file.
func parseSyntax(c *qt.C, dialect, text string) (*ast.StatementList, error) {
	c.Helper()
	return parser.NewParser(text, parser.WithDialect(dialect), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
}

// TestSQL_ChangesStandAsTheOwnersOperations pins how the owner reads DROP
// EXTENSION and ALTER EXTENSION: each is the operation it performs, carried
// as written, with a plain name folded as the target folds one.
func TestSQL_ChangesStandAsTheOwnersOperations(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    *pgextension.Operation
	}{
		{name: "a drop", dialect: platform.Postgres, sql: "DROP EXTENSION pg_trgm;",
			want: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Drop}},
		{name: "a guarded drop with its dependents", dialect: platform.Postgres, sql: "DROP EXTENSION IF EXISTS PG_TRGM CASCADE;",
			want: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Drop, IfExists: true, Cascade: true}},
		{name: "a drop that restricts", dialect: platform.Postgres, sql: `DROP EXTENSION "PG_TRGM" RESTRICT;`,
			want: &pgextension.Operation{Name: "PG_TRGM", Action: pgextension.Drop}},
		{name: "an update", dialect: platform.Postgres, sql: "ALTER EXTENSION pg_trgm UPDATE TO '1.6';",
			want: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.UpdateVersion, Version: "1.6"}},
		{name: "an update to a version written as a name", dialect: platform.Postgres, sql: `ALTER EXTENSION pg_trgm UPDATE TO "1.6";`,
			want: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.UpdateVersion, Version: "1.6"}},
		{name: "a move", dialect: platform.Postgres, sql: "ALTER EXTENSION pg_trgm SET SCHEMA Ext;",
			want: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.SetSchema, Schema: "ext"}},
		{name: "a CockroachDB drop", dialect: platform.CockroachDB, sql: "DROP EXTENSION PG_TRGM;",
			want: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Drop}},
		{name: "a document that names no target", sql: "DROP EXTENSION PG_TRGM;",
			want: &pgextension.Operation{Name: "PG_TRGM", Action: pgextension.Drop}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseSyntax(c, test.dialect, test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.DeepEquals, []ast.Node{&ast.ExtensionStatement{Payload: test.want}})
		})
	}
}

// TestSQL_ChangesFailurePath pins the forms of DROP EXTENSION and ALTER
// EXTENSION the operation cannot carry, refused at their position rather
// than read as a different statement.
func TestSQL_ChangesFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "two extensions in one drop", sql: "DROP EXTENSION hstore, pg_trgm;",
			wantErr: `.*DROP EXTENSION hstore, \.\.\. at position 21: the extension owner reads one extension per DROP EXTENSION; write a statement for each`},
		{name: "IF without EXISTS", sql: "DROP EXTENSION IF pg_trgm;", wantErr: `.*expected EXISTS after IF at position 18`},
		{name: "CASCADE and RESTRICT", sql: "DROP EXTENSION pg_trgm CASCADE RESTRICT;", wantErr: `.*unexpected "RESTRICT" after DROP EXTENSION pg_trgm at position 31`},
		{name: "an update without a version", sql: "ALTER EXTENSION pg_trgm UPDATE;",
			wantErr: `.*ALTER EXTENSION pg_trgm UPDATE at position 24: the extension owner reads an update to a named version; write UPDATE TO the version`},
		{name: "SET without SCHEMA", sql: "ALTER EXTENSION pg_trgm SET ext;", wantErr: `.*expected SCHEMA after SET in ALTER EXTENSION pg_trgm at position 28`},
		{name: "a member added", sql: "ALTER EXTENSION pg_trgm ADD FUNCTION f();",
			wantErr: `.*unsupported ALTER EXTENSION pg_trgm action "ADD" at position 24: the extension owner reads UPDATE TO and SET SCHEMA`},
		{name: "a word after a move", sql: "ALTER EXTENSION pg_trgm SET SCHEMA ext CASCADE;",
			wantErr: `.*unexpected "CASCADE" after ALTER EXTENSION pg_trgm at position 39`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseSyntax(c, platform.Postgres, test.sql)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
