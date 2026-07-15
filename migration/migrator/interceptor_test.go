package migrator_test

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

// recordingInterceptor handles every statement and records what it saw.
type recordingInterceptor struct {
	statements   []string
	directives   []map[string]string
	err          error
	validateErr  error
	handled      bool // whether ExecuteStatement claims to have handled statements
	handledSetup bool
}

func (r *recordingInterceptor) ValidateDirectives(map[string]string) error {
	return r.validateErr
}

func (r *recordingInterceptor) ExecuteStatement(_ context.Context, _ *dbschema.DatabaseConnection, stmt string, directives map[string]string) (bool, error) {
	if r.err != nil {
		return false, r.err
	}
	r.statements = append(r.statements, stmt)
	r.directives = append(r.directives, directives)
	if r.handledSetup {
		return r.handled, nil
	}
	return true, nil
}

func TestFSMigrationProvider_StatementInterceptorSeesStatementsAndDirectives(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"0000000001_widen.up.sql": &fstest.MapFile{Data: []byte(
			"-- +ptah online_ddl_tool=ghost\n" +
				"ALTER TABLE users ADD COLUMN bio TEXT;\n" +
				"ALTER TABLE users ADD COLUMN age INT;\n",
		)},
		"0000000001_widen.down.sql": &fstest.MapFile{Data: []byte(
			"ALTER TABLE users DROP COLUMN age;\n",
		)},
	}

	interceptor := &recordingInterceptor{}
	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithStatementInterceptor(interceptor))
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)

	// The interceptor handles every statement, so a nil connection is never
	// touched.
	c.Assert(migrations[0].Up(context.Background(), nil), qt.IsNil)
	c.Assert(interceptor.statements, qt.DeepEquals, []string{
		"ALTER TABLE users ADD COLUMN bio TEXT",
		"ALTER TABLE users ADD COLUMN age INT",
	})
	c.Assert(interceptor.directives, qt.HasLen, 2)
	c.Assert(interceptor.directives[0], qt.DeepEquals, map[string]string{"online_ddl_tool": "ghost"})

	// Down migrations carry their own (here: empty) directive set.
	c.Assert(migrations[0].Down(context.Background(), nil), qt.IsNil)
	c.Assert(interceptor.statements[2], qt.Equals, "ALTER TABLE users DROP COLUMN age")
	c.Assert(interceptor.directives[2], qt.DeepEquals, map[string]string{})
}

func TestMigrationFuncFromSQLFilenameWithInterceptor_ErrorAbortsMigration(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"m.sql": &fstest.MapFile{Data: []byte("ALTER TABLE users ADD COLUMN bio TEXT;")},
	}
	interceptor := &recordingInterceptor{err: context.DeadlineExceeded}

	fn := migrator.MigrationFuncFromSQLFilenameWithInterceptor("m.sql", fsys, interceptor)
	err := fn(context.Background(), nil)
	c.Assert(err, qt.ErrorMatches, "failed to execute migration SQL: .*")
}

func TestMigrationFuncFromSQLFilenameWithInterceptor_AtlasTxtarExecutesMigrationSectionOnly(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"20240305171146_seed.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- migration.sql --
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- down.sql --
DELETE FROM users WHERE id = 1;
`)},
	}
	interceptor := &recordingInterceptor{}

	fn := migrator.MigrationFuncFromSQLFilenameWithInterceptor("20240305171146_seed.sql", fsys, interceptor)
	err := fn(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	c.Assert(interceptor.statements, qt.DeepEquals, []string{
		"INSERT INTO users (id, name) VALUES (1, 'Alice')",
	})
}

func TestMigrationFuncFromSQLFilenameWithInterceptor_ValidateDirectivesRunsBeforeAnyStatement(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"m.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE t (id INT);\nALTER TABLE t ADD COLUMN a INT;\n")},
	}
	interceptor := &recordingInterceptor{validateErr: errors.New("bad directive value")}

	fn := migrator.MigrationFuncFromSQLFilenameWithInterceptor("m.sql", fsys, interceptor)
	err := fn(context.Background(), nil)

	c.Assert(err, qt.ErrorMatches, "invalid migration directives in m.sql: bad directive value")
	c.Assert(interceptor.statements, qt.HasLen, 0,
		qt.Commentf("no statement may execute once directive validation fails"))
}

func TestMigrationFuncFromSQLFilenameWithInterceptor_DeclinedStatementReachesWriter(t *testing.T) {
	c := qt.New(t)

	// When the interceptor declines a statement (handled=false), the
	// migrator must run it on the connection. With a nil connection that
	// dispatch panics — which is exactly what proves the writer path is
	// taken (a regression that skipped declined statements would not panic).
	fsys := fstest.MapFS{
		"m.sql": &fstest.MapFile{Data: []byte("ALTER TABLE t ADD COLUMN a INT;")},
	}
	interceptor := &recordingInterceptor{handledSetup: true, handled: false}

	fn := migrator.MigrationFuncFromSQLFilenameWithInterceptor("m.sql", fsys, interceptor)
	c.Assert(func() { _ = fn(context.Background(), nil) }, qt.PanicMatches, ".*")
	c.Assert(interceptor.statements, qt.HasLen, 1,
		qt.Commentf("the interceptor was still consulted before the writer dispatch"))
}
