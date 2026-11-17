package migraterun_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/internal/migraterun"
	"ptah.run/internal/dburldisplay"
	"ptah.run/migration/migrator"
)

// answeringInterceptor answers every statement with answer, as a server would.
type answeringInterceptor struct{ answer error }

func (answeringInterceptor) ValidateDirectives(map[string]string) error { return nil }

func (i answeringInterceptor) ExecuteStatement(
	context.Context, *dbschema.DatabaseConnection, string, map[string]string,
) (bool, error) {
	return true, i.answer
}

// failedRun is the error of an up run whose one statement the server answered
// with answer.
func failedRun(c *qt.C, answer error) error {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "run.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	fsys := fstest.MapFS{
		"0000000001_rows.up.sql":   {Data: []byte("INSERT INTO accounts (id) VALUES (1);\n")},
		"0000000001_rows.down.sql": {Data: []byte("DELETE FROM accounts;\n")},
	}
	mig, err := migrator.NewFSMigrator(conn, fsys, migrator.WithStatementInterceptor(answeringInterceptor{answer: answer}))
	c.Assert(err, qt.IsNil)
	runErr := mig.MigrateUp(c.Context())
	c.Assert(runErr, qt.IsNotNil)
	return runErr
}

// The details lose the secrets of every address the command opened, as its
// error line does: a server can quote one back, in a value or a message.
func TestServerDetailsRedactTheCommandsAddresses(t *testing.T) {
	c := qt.New(t)
	ctx := dburldisplay.WithErrorRedaction(c.Context())
	dburldisplay.RecordErrorAddress(ctx, "postgres://app:Pa55wordX9@localhost/app")
	runErr := failedRun(c, &pgconn.PgError{Code: "23505", Detail: "Key (token)=(Pa55wordX9) already exists."})

	text := migraterun.NewServerDetails(ctx, true).Text(runErr)

	c.Assert(text, qt.Contains, "  detail: Key (token)=(***) already exists.")
	c.Assert(text, qt.Not(qt.Contains), "Pa55wordX9")
}

// Explain keeps the error the run failed with: the exit status and every
// errors.Is the command asks read through it.
func TestServerDetailsExplain_HappyPath(t *testing.T) {
	c := qt.New(t)
	answer := &pgconn.PgError{Code: "23503", Detail: "Key (parent)=(4242) is not present."}
	runErr := failedRun(c, answer)

	explained := migraterun.NewServerDetails(c.Context(), true).Explain(runErr)

	c.Assert(explained.Error(), qt.Equals, runErr.Error()+"\n"+
		"server details (may contain application data):\n"+
		"  failed at: statement 1 of 1 of migration 1 (0000000001_rows.up.sql)\n"+
		"  sqlstate: 23503\n"+
		"  detail: Key (parent)=(4242) is not present.")
	c.Assert(explained, qt.ErrorIs, runErr)
	c.Assert(explained, qt.ErrorIs, answer)
}

// Without the choice the error is returned as it is.
func TestServerDetailsExplainWithoutTheChoice(t *testing.T) {
	c := qt.New(t)
	runErr := failedRun(c, &pgconn.PgError{Code: "23503", Detail: "Key (parent)=(4242) is not present."})

	c.Assert(migraterun.NewServerDetails(c.Context(), false).Explain(runErr), qt.Equals, runErr)
}

// An error that stopped at no statement or commit has nothing to explain.
func TestServerDetailsExplainOfNoStatement(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "an error that names no migration", err: errors.New("could not take the migration lock")},
		{name: "no error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migraterun.NewServerDetails(c.Context(), true).Explain(test.err), qt.Equals, test.err)
		})
	}
}
