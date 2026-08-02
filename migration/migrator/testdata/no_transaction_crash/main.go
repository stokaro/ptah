package main

import (
	"context"
	"fmt"
	"os"
	"testing/fstest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

const crashExitCode = 73

func main() {
	databasePath := os.Args[1]
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+databasePath)
	must(err)
	fsys := fstest.MapFS{
		"000001_create_users.up.sql": {
			Data: []byte("-- +ptah no_transaction\nCREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE posts (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_users.down.sql": {
			Data: []byte("-- +ptah no_transaction\nDROP TABLE posts;\nDROP TABLE users;"),
		},
	}
	revisionFormat := migrator.RevisionTableFormat(os.Args[2])
	switch os.Args[3] {
	case "up":
		crashDuringUp(conn, fsys, revisionFormat)
	case "down":
		crashDuringDown(conn, fsys, revisionFormat)
	default:
		panic("unknown crash direction")
	}
}

func crashDuringUp(
	conn *dbschema.DatabaseConnection,
	fsys fstest.MapFS,
	revisionFormat migrator.RevisionTableFormat,
) {
	provider, err := crashProvider(fsys, os.Args[4])
	must(err)
	mig := migrator.NewMigrator(conn, provider).
		WithRevisionTableFormat(revisionFormat)
	must(mig.MigrateUp(context.Background()))
	panic("migration completed without crashing")
}

func crashDuringDown(
	conn *dbschema.DatabaseConnection,
	fsys fstest.MapFS,
	revisionFormat migrator.RevisionTableFormat,
) {
	provider, err := migrator.NewFSMigrationProvider(fsys)
	must(err)
	mig := migrator.NewMigrator(conn, provider).WithRevisionTableFormat(revisionFormat)
	must(mig.MigrateUp(context.Background()))

	provider, err = crashProvider(fsys, os.Args[4])
	must(err)
	mig = migrator.NewMigrator(conn, provider).WithRevisionTableFormat(revisionFormat)
	must(mig.MigrateDownTo(context.Background(), 0))
	panic("rollback completed without crashing")
}

func crashProvider(fsys fstest.MapFS, crashPoint string) (*migrator.FSMigrationProvider, error) {
	switch crashPoint {
	case "after-checkpoint":
		return migrator.NewFSMigrationProvider(
			fsys,
			migrator.WithStatementObserver(migrator.StatementObserverFunc(crashAfterCheckpoint)),
		)
	case "after-execution":
		return migrator.NewFSMigrationProvider(
			fsys,
			migrator.WithStatementInterceptor(crashAfterExecutionInterceptor{}),
		)
	default:
		panic("unknown crash point")
	}
}

type crashAfterExecutionInterceptor struct{}

func (crashAfterExecutionInterceptor) ValidateDirectives(map[string]string) error {
	return nil
}

func (crashAfterExecutionInterceptor) ExecuteStatement(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	statement string,
	_ map[string]string,
) (bool, error) {
	_, err := conn.ExecContext(ctx, statement)
	must(err)
	os.Exit(crashExitCode)
	return true, nil
}

func crashAfterCheckpoint(context.Context, migrator.StatementEvent) error {
	os.Exit(crashExitCode)
	return nil
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
