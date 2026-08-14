package migrator_test

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

type statementObserverRecorder struct {
	events []migrator.StatementEvent
	after  func(migrator.StatementEvent) error
}

func newStatementObserverRecorder() *statementObserverRecorder {
	return &statementObserverRecorder{after: successfulStatementObservation}
}

func successfulStatementObservation(migrator.StatementEvent) error {
	return nil
}

func (r *statementObserverRecorder) ObserveStatement(
	_ context.Context,
	event migrator.StatementEvent,
) error {
	snapshot := event
	snapshot.Directives = maps.Clone(event.Directives)
	r.events = append(r.events, snapshot)
	return r.after(event)
}

type statementObserverInterceptor struct {
	handled    bool
	statements []string
	directives []map[string]string
}

func (i *statementObserverInterceptor) ValidateDirectives(map[string]string) error {
	return nil
}

func (i *statementObserverInterceptor) ExecuteStatement(
	_ context.Context,
	_ *dbschema.DatabaseConnection,
	statement string,
	directives map[string]string,
) (bool, error) {
	i.statements = append(i.statements, statement)
	i.directives = append(i.directives, maps.Clone(directives))
	return i.handled, nil
}

type rejectingStatementInterceptor struct {
	err error
}

func (i *rejectingStatementInterceptor) ValidateDirectives(map[string]string) error {
	return i.err
}

func (*rejectingStatementInterceptor) ExecuteStatement(
	context.Context,
	*dbschema.DatabaseConnection,
	string,
	map[string]string,
) (bool, error) {
	panic("ExecuteStatement called after directive validation failed")
}

type failingStatementInterceptor struct {
	err error
}

func (*failingStatementInterceptor) ValidateDirectives(map[string]string) error {
	return nil
}

func (i *failingStatementInterceptor) ExecuteStatement(
	context.Context,
	*dbschema.DatabaseConnection,
	string,
	map[string]string,
) (bool, error) {
	return false, i.err
}

type mutatingStatementInterceptor struct {
	directives map[string]string
}

func (*mutatingStatementInterceptor) ValidateDirectives(directives map[string]string) error {
	directives["validation"] = "mutated"
	return nil
}

func (i *mutatingStatementInterceptor) ExecuteStatement(
	_ context.Context,
	_ *dbschema.DatabaseConnection,
	_ string,
	directives map[string]string,
) (bool, error) {
	directives["execution"] = "mutated"
	i.directives = maps.Clone(directives)
	return true, nil
}

type statementStateObserver struct {
	conn   *dbschema.DatabaseConnection
	query  string
	counts []int
}

func (o *statementStateObserver) ObserveStatement(
	ctx context.Context,
	_ migrator.StatementEvent,
) error {
	var count int
	err := o.conn.QueryRowContext(ctx, o.query).Scan(&count)
	o.counts = append(o.counts, count)
	return err
}

var (
	_ migrator.StatementObserver = (*statementObserverRecorder)(nil)
	_ migrator.StatementObserver = (*statementStateObserver)(nil)
	_ migrator.StatementObserver = migrator.StatementObserverFunc(nil)
)

func openStatementObserverSQLite(t *testing.T) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)

	conn, err := dbschema.ConnectToDatabase(
		context.Background(),
		"sqlite://"+filepath.Join(t.TempDir(), "statement-observer.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	return conn
}

func statementObserverTableCount(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	tableName string,
) int {
	t.Helper()
	c := qt.New(t)

	var count int
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		tableName,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func statementObserverRowCount(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	tableName string,
) int {
	t.Helper()
	c := qt.New(t)

	var count int
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT count(*) FROM "+tableName,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

type statementObserverRevisionProgress struct {
	applied        int
	total          int
	errorStatement string
}

func readStatementObserverRevisionProgress(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	tableName string,
	version int64,
) statementObserverRevisionProgress {
	t.Helper()
	c := qt.New(t)

	var progress statementObserverRevisionProgress
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT applied, total, error_stmt FROM "+tableName+" WHERE version = ?",
		version,
	).Scan(&progress.applied, &progress.total, &progress.errorStatement)
	c.Assert(err, qt.IsNil)
	return progress
}

func TestStatementObserver_PtahPairReportsOrderedEvents(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_accounts.up.sql": &fstest.MapFile{Data: []byte(
			"-- +ptah direction=up\n" +
				"CREATE TABLE accounts (id INTEGER PRIMARY KEY);\n" +
				"INSERT INTO accounts (id) VALUES (1);\n",
		)},
		"0000000001_accounts.down.sql": &fstest.MapFile{Data: []byte(
			"-- +ptah direction=down\n" +
				"DELETE FROM accounts;\n" +
				"DROP TABLE accounts;\n",
		)},
	}
	observer := newStatementObserverRecorder()
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	conn := openStatementObserverSQLite(t)

	c.Assert(migrations[0].Up(context.Background(), conn), qt.IsNil)
	c.Assert(migrations[0].Down(context.Background(), conn), qt.IsNil)

	c.Assert(observer.events, qt.DeepEquals, []migrator.StatementEvent{
		{
			SourcePath: "0000000001_accounts.up.sql",
			Statement:  "CREATE TABLE accounts (id INTEGER PRIMARY KEY)",
			Index:      1,
			Total:      2,
			Directives: map[string]string{"direction": "up"},
		},
		{
			SourcePath: "0000000001_accounts.up.sql",
			Statement:  "INSERT INTO accounts (id) VALUES (1)",
			Index:      2,
			Total:      2,
			Directives: map[string]string{"direction": "up"},
		},
		{
			SourcePath: "0000000001_accounts.down.sql",
			Statement:  "DELETE FROM accounts",
			Index:      1,
			Total:      2,
			Directives: map[string]string{"direction": "down"},
		},
		{
			SourcePath: "0000000001_accounts.down.sql",
			Statement:  "DROP TABLE accounts",
			Index:      2,
			Total:      2,
			Directives: map[string]string{"direction": "down"},
		},
	})
}

func TestStatementObserver_AtlasSingleAndTxtarReportSectionSources(t *testing.T) {
	c := qt.New(t)
	const (
		singlePath = "20240101000001_single.sql"
		txtarPath  = "20240101000002_archive.sql"
	)
	fsys := fstest.MapFS{
		singlePath: &fstest.MapFile{Data: []byte(
			"-- +ptah source=single\n" +
				"CREATE TABLE atlas_single (id INTEGER PRIMARY KEY);\n",
		)},
		txtarPath: &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- migration.sql --
-- +ptah source=txtar_up
CREATE TABLE atlas_archive (id INTEGER PRIMARY KEY);
INSERT INTO atlas_archive (id) VALUES (1);

-- schema.sql --
THIS SECTION MUST NOT EXECUTE;

-- down.sql --
-- +ptah source=txtar_down
DROP TABLE atlas_archive;
`)},
	}
	observer := newStatementObserverRecorder()
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 2)
	conn := openStatementObserverSQLite(t)

	c.Assert(migrations[0].Up(context.Background(), conn), qt.IsNil)
	c.Assert(migrations[1].Up(context.Background(), conn), qt.IsNil)
	c.Assert(migrations[1].Down(context.Background(), conn), qt.IsNil)

	c.Assert(observer.events, qt.DeepEquals, []migrator.StatementEvent{
		{
			SourcePath: singlePath,
			Statement:  "CREATE TABLE atlas_single (id INTEGER PRIMARY KEY)",
			Index:      1,
			Total:      1,
			Directives: map[string]string{"source": "single"},
		},
		{
			SourcePath: txtarPath + "#migration.sql",
			Statement:  "CREATE TABLE atlas_archive (id INTEGER PRIMARY KEY)",
			Index:      1,
			Total:      2,
			Directives: map[string]string{"source": "txtar_up"},
		},
		{
			SourcePath: txtarPath + "#migration.sql",
			Statement:  "INSERT INTO atlas_archive (id) VALUES (1)",
			Index:      2,
			Total:      2,
			Directives: map[string]string{"source": "txtar_up"},
		},
		{
			SourcePath: txtarPath + "#down.sql",
			Statement:  "DROP TABLE atlas_archive",
			Index:      1,
			Total:      1,
			Directives: map[string]string{"source": "txtar_down"},
		},
	})
}

func TestStatementObserver_NormalWriterExecutionIsObservedOnce(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_writer.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE writer_observed (id INTEGER PRIMARY KEY);\n"),
		},
		"0000000001_writer.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE writer_observed;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	conn := openStatementObserverSQLite(t)

	c.Assert(migrations[0].Up(context.Background(), conn), qt.IsNil)

	c.Assert(statementObserverTableCount(t, conn, "writer_observed"), qt.Equals, 1)
	c.Assert(observer.events, qt.DeepEquals, []migrator.StatementEvent{
		{
			SourcePath: "0000000001_writer.up.sql",
			Statement:  "CREATE TABLE writer_observed (id INTEGER PRIMARY KEY)",
			Index:      1,
			Total:      1,
			Directives: map[string]string{},
		},
	})
}

func TestStatementObserver_InterceptorHandledStatementIsObservedOnce(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_handled.up.sql": &fstest.MapFile{
			Data: []byte("EXECUTE THROUGH INTERCEPTOR;\n"),
		},
		"0000000001_handled.down.sql": &fstest.MapFile{
			Data: []byte("SELECT 1;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	interceptor := &statementObserverInterceptor{handled: true}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementInterceptor(interceptor),
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)

	c.Assert(migrations[0].Up(context.Background(), nil), qt.IsNil)

	c.Assert(interceptor.statements, qt.DeepEquals, []string{"EXECUTE THROUGH INTERCEPTOR"})
	c.Assert(observer.events, qt.DeepEquals, []migrator.StatementEvent{
		{
			SourcePath: "0000000001_handled.up.sql",
			Statement:  "EXECUTE THROUGH INTERCEPTOR",
			Index:      1,
			Total:      1,
			Directives: map[string]string{},
		},
	})
}

func TestStatementObserverFunc_AdaptsFunction(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_adapter.up.sql": &fstest.MapFile{
			Data: []byte("EXECUTE THROUGH INTERCEPTOR;\n"),
		},
		"0000000001_adapter.down.sql": &fstest.MapFile{
			Data: []byte("SELECT 1;\n"),
		},
	}
	var events []migrator.StatementEvent
	observer := migrator.StatementObserverFunc(func(
		_ context.Context,
		event migrator.StatementEvent,
	) error {
		events = append(events, event)
		return nil
	})
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementInterceptor(&statementObserverInterceptor{handled: true}),
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)

	c.Assert(migrations[0].Up(context.Background(), nil), qt.IsNil)

	c.Assert(events, qt.DeepEquals, []migrator.StatementEvent{
		{
			SourcePath: "0000000001_adapter.up.sql",
			Statement:  "EXECUTE THROUGH INTERCEPTOR",
			Index:      1,
			Total:      1,
			Directives: map[string]string{},
		},
	})
}

func TestStatementObserver_DirectiveValidationFailureIsNotObserved(t *testing.T) {
	c := qt.New(t)
	validationErr := errors.New("invalid observer test directive")
	fsys := fstest.MapFS{
		"0000000001_validation.up.sql": &fstest.MapFile{
			Data: []byte("-- +ptah mode=invalid\nSELECT 1;\n"),
		},
		"0000000001_validation.down.sql": &fstest.MapFile{
			Data: []byte("SELECT 1;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementInterceptor(&rejectingStatementInterceptor{err: validationErr}),
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)

	err = migrations[0].Up(context.Background(), nil)

	c.Assert(err, qt.ErrorIs, validationErr)
	c.Assert(observer.events, qt.HasLen, 0)
}

func TestStatementObserver_InterceptorExecutionFailureIsNotObserved(t *testing.T) {
	c := qt.New(t)
	interceptorErr := errors.New("interceptor execution failed")
	fsys := fstest.MapFS{
		"0000000001_interceptor_failure.up.sql": &fstest.MapFile{
			Data: []byte("EXECUTE THROUGH INTERCEPTOR;\n"),
		},
		"0000000001_interceptor_failure.down.sql": &fstest.MapFile{
			Data: []byte("SELECT 1;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementInterceptor(&failingStatementInterceptor{err: interceptorErr}),
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)

	err = migrations[0].Up(context.Background(), nil)

	var executionErr *migrator.MigrationExecutionError
	c.Assert(err, qt.ErrorAs, &executionErr)
	c.Assert(err, qt.ErrorIs, interceptorErr)
	c.Assert(executionErr.Statement, qt.Equals, "EXECUTE THROUGH INTERCEPTOR")
	c.Assert(executionErr.StatementIndex, qt.Equals, 1)
	c.Assert(executionErr.Total, qt.Equals, 1)
	c.Assert(observer.events, qt.HasLen, 0)
}

func TestStatementObserver_InterceptorDeclinedStatementIsObservedOnce(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_declined.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE interceptor_declined (id INTEGER PRIMARY KEY);\n"),
		},
		"0000000001_declined.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE interceptor_declined;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	interceptor := &statementObserverInterceptor{handled: false}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementInterceptor(interceptor),
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	conn := openStatementObserverSQLite(t)

	c.Assert(migrations[0].Up(context.Background(), conn), qt.IsNil)

	c.Assert(statementObserverTableCount(t, conn, "interceptor_declined"), qt.Equals, 1)
	c.Assert(interceptor.statements, qt.DeepEquals, []string{
		"CREATE TABLE interceptor_declined (id INTEGER PRIMARY KEY)",
	})
	c.Assert(observer.events, qt.DeepEquals, []migrator.StatementEvent{
		{
			SourcePath: "0000000001_declined.up.sql",
			Statement:  "CREATE TABLE interceptor_declined (id INTEGER PRIMARY KEY)",
			Index:      1,
			Total:      1,
			Directives: map[string]string{},
		},
	})
}

func TestStatementObserver_RunsAfterSuccessfulExecution(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_order.up.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE observation_order (id INTEGER PRIMARY KEY);\n" +
				"INSERT INTO observation_order (id) VALUES (1);\n",
		)},
		"0000000001_order.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE observation_order;\n"),
		},
	}
	conn := openStatementObserverSQLite(t)
	observer := &statementStateObserver{
		conn:  conn,
		query: "SELECT count(*) FROM observation_order",
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)

	c.Assert(migrations[0].Up(context.Background(), conn), qt.IsNil)

	c.Assert(observer.counts, qt.DeepEquals, []int{0, 1})
}

func TestStatementObserver_ExecutionFailureIsNotObserved(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_failure.up.sql": &fstest.MapFile{
			Data: []byte("INSERT INTO missing_table (id) VALUES (1);\n"),
		},
		"0000000001_failure.down.sql": &fstest.MapFile{
			Data: []byte("SELECT 1;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	conn := openStatementObserverSQLite(t)

	err = migrations[0].Up(context.Background(), conn)

	var executionErr *migrator.MigrationExecutionError
	c.Assert(err, qt.ErrorAs, &executionErr)
	c.Assert(executionErr.Statement, qt.Equals, "INSERT INTO missing_table (id) VALUES (1)")
	c.Assert(executionErr.StatementIndex, qt.Equals, 1)
	c.Assert(executionErr.Total, qt.Equals, 1)
	c.Assert(observer.events, qt.HasLen, 0)
}

func TestStatementObserver_FailureStopsMigrationWithStatementContext(t *testing.T) {
	c := qt.New(t)
	const sourcePath = "0000000001_observer_failure.up.sql"
	observerFailure := errors.New("statement observation failed")
	observationResults := []error{nil, observerFailure}
	fsys := fstest.MapFS{
		sourcePath: &fstest.MapFile{Data: []byte(
			"CREATE TABLE observer_failure (id INTEGER PRIMARY KEY);\n" +
				"INSERT INTO observer_failure (id) VALUES (1);\n" +
				"INSERT INTO observer_failure (id) VALUES (2);\n",
		)},
		"0000000001_observer_failure.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE observer_failure;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	observer.after = func(migrator.StatementEvent) error {
		return observationResults[len(observer.events)-1]
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	conn := openStatementObserverSQLite(t)

	err = migrations[0].Up(context.Background(), conn)

	var observationErr *migrator.StatementObservationError
	c.Assert(err, qt.ErrorAs, &observationErr)
	c.Assert(err, qt.ErrorIs, observerFailure)
	c.Assert(err.Error(), qt.Contains, sourcePath)
	c.Assert(observationErr.Event.SourcePath, qt.Equals, sourcePath)
	c.Assert(observationErr.Event.Statement, qt.Equals, "INSERT INTO observer_failure (id) VALUES (1)")
	c.Assert(observationErr.Event.Index, qt.Equals, 2)
	c.Assert(observationErr.Event.Total, qt.Equals, 3)
	c.Assert(observer.events, qt.HasLen, 2)
	c.Assert(statementObserverRowCount(t, conn, "observer_failure"), qt.Equals, 1)
}

func TestStatementObserver_DirectivesAreIsolatedCopies(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_directives.up.sql": &fstest.MapFile{Data: []byte(
			"-- +ptah scope=migration\n" +
				"FIRST OBSERVED STATEMENT;\n" +
				"SECOND OBSERVED STATEMENT;\n",
		)},
		"0000000001_directives.down.sql": &fstest.MapFile{
			Data: []byte("SELECT 1;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	observer.after = func(event migrator.StatementEvent) error {
		event.Directives["scope"] = "observer-mutated"
		event.Directives["observer"] = "added"
		return nil
	}
	interceptor := &statementObserverInterceptor{handled: true}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementInterceptor(interceptor),
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)

	c.Assert(migrations[0].Up(context.Background(), nil), qt.IsNil)

	c.Assert(observer.events, qt.HasLen, 2)
	c.Assert(observer.events[0].Directives, qt.DeepEquals, map[string]string{"scope": "migration"})
	c.Assert(observer.events[1].Directives, qt.DeepEquals, map[string]string{"scope": "migration"})
	c.Assert(interceptor.directives, qt.DeepEquals, []map[string]string{
		{"scope": "migration"},
		{"scope": "migration"},
	})
}

func TestStatementObserver_InterceptorMutationsDoNotChangeFileDirectives(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_interceptor_directives.up.sql": &fstest.MapFile{Data: []byte(
			"-- +ptah scope=migration\n" +
				"EXECUTE THROUGH INTERCEPTOR;\n",
		)},
		"0000000001_interceptor_directives.down.sql": &fstest.MapFile{
			Data: []byte("SELECT 1;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	interceptor := &mutatingStatementInterceptor{}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementInterceptor(interceptor),
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)

	c.Assert(migrations[0].Up(context.Background(), nil), qt.IsNil)

	c.Assert(interceptor.directives, qt.DeepEquals, map[string]string{
		"scope":      "migration",
		"validation": "mutated",
		"execution":  "mutated",
	})
	c.Assert(observer.events, qt.DeepEquals, []migrator.StatementEvent{
		{
			SourcePath: "0000000001_interceptor_directives.up.sql",
			Statement:  "EXECUTE THROUGH INTERCEPTOR",
			Index:      1,
			Total:      1,
			Directives: map[string]string{"scope": "migration"},
		},
	})
}

func TestStatementObserver_TransactionalFailureRollsBackAndRecordsZeroProgress(t *testing.T) {
	c := qt.New(t)
	const (
		migrationsTable = "schema_migrations_observer_transaction"
		sourcePath      = "0000000001_transaction.up.sql"
	)
	observerFailure := errors.New("transaction observation failed")
	observationResults := []error{nil, observerFailure}
	fsys := fstest.MapFS{
		sourcePath: &fstest.MapFile{Data: []byte(
			"CREATE TABLE observer_transaction (id INTEGER PRIMARY KEY);\n" +
				"INSERT INTO observer_transaction (id) VALUES (1);\n",
		)},
		"0000000001_transaction.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE observer_transaction;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	observer.after = func(migrator.StatementEvent) error {
		return observationResults[len(observer.events)-1]
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	conn := openStatementObserverSQLite(t)
	migrationRunner := migrator.NewMigrator(conn, provider).
		WithMigrationsTable("", migrationsTable)

	err = migrationRunner.MigrateUp(context.Background())

	var observationErr *migrator.StatementObservationError
	c.Assert(err, qt.ErrorAs, &observationErr)
	c.Assert(err, qt.ErrorIs, observerFailure)
	c.Assert(statementObserverTableCount(t, conn, "observer_transaction"), qt.Equals, 0)
	progress := readStatementObserverRevisionProgress(t, conn, migrationsTable, 1)
	c.Assert(progress.applied, qt.Equals, 0)
	c.Assert(progress.total, qt.Equals, 2)
	c.Assert(progress.errorStatement, qt.Equals, "INSERT INTO observer_transaction (id) VALUES (1)")
}

func TestStatementObserver_NoTransactionFailureRecordsAppliedStatement(t *testing.T) {
	c := qt.New(t)
	const (
		migrationsTable = "schema_migrations_observer_no_transaction"
		sourcePath      = "0000000001_observer_no_transaction.up.sql"
	)
	observerFailure := errors.New("no-transaction observation failed")
	observationResults := []error{nil, observerFailure}
	fsys := fstest.MapFS{
		sourcePath: &fstest.MapFile{Data: []byte(
			"-- +ptah no_transaction\n" +
				"CREATE TABLE observer_no_transaction (id INTEGER PRIMARY KEY);\n" +
				"INSERT INTO observer_no_transaction (id) VALUES (1);\n" +
				"INSERT INTO observer_no_transaction (id) VALUES (2);\n",
		)},
		"0000000001_observer_no_transaction.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE observer_no_transaction;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	observer.after = func(migrator.StatementEvent) error {
		return observationResults[len(observer.events)-1]
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	conn := openStatementObserverSQLite(t)
	migrationRunner := migrator.NewMigrator(conn, provider).
		WithMigrationsTable("", migrationsTable)

	err = migrationRunner.MigrateUp(context.Background())

	var observationErr *migrator.StatementObservationError
	c.Assert(err, qt.ErrorAs, &observationErr)
	c.Assert(err, qt.ErrorIs, observerFailure)
	c.Assert(statementObserverRowCount(t, conn, "observer_no_transaction"), qt.Equals, 1)
	progress := readStatementObserverRevisionProgress(t, conn, migrationsTable, 1)
	c.Assert(progress.applied, qt.Equals, 2)
	c.Assert(progress.total, qt.Equals, 3)
	c.Assert(progress.errorStatement, qt.Equals, "INSERT INTO observer_no_transaction (id) VALUES (1)")
}

func TestStatementObserver_NoTransactionExecutionIsObserved(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_no_transaction.up.sql": &fstest.MapFile{Data: []byte(
			"-- +ptah no_transaction\n" +
				"CREATE TABLE no_transaction_observed (id INTEGER PRIMARY KEY);\n" +
				"INSERT INTO no_transaction_observed (id) VALUES (1);\n",
		)},
		"0000000001_no_transaction.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE no_transaction_observed;\n"),
		},
	}
	observer := newStatementObserverRecorder()
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].UpTxMode, qt.Equals, migrator.MigrationFileTxModeNone)
	conn := openStatementObserverSQLite(t)

	c.Assert(migrations[0].Up(context.Background(), conn), qt.IsNil)

	c.Assert(statementObserverRowCount(t, conn, "no_transaction_observed"), qt.Equals, 1)
	c.Assert(observer.events, qt.DeepEquals, []migrator.StatementEvent{
		{
			SourcePath: "0000000001_no_transaction.up.sql",
			Statement:  "CREATE TABLE no_transaction_observed (id INTEGER PRIMARY KEY)",
			Index:      1,
			Total:      2,
			Directives: map[string]string{"no_transaction": "true"},
		},
		{
			SourcePath: "0000000001_no_transaction.up.sql",
			Statement:  "INSERT INTO no_transaction_observed (id) VALUES (1)",
			Index:      2,
			Total:      2,
			Directives: map[string]string{"no_transaction": "true"},
		},
	})
}
