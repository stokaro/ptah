package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/rolepassword"
)

// passwordRunner records the statements a writer sends and answers each with
// refusal, which an error from the server stands for.
type passwordRunner struct {
	sent    []string
	refusal error
}

func (r *passwordRunner) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error) {
	return nil, errors.New("no transactions")
}
func (r *passwordRunner) Exec(query string, args ...any) (sql.Result, error) {
	return r.ExecContext(context.Background(), query, args...)
}
func (r *passwordRunner) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	r.sent = append(r.sent, query)
	return nil, r.refusal
}
func (r *passwordRunner) Query(string, ...any) (*sql.Rows, error) {
	return nil, errors.New("no queries")
}
func (r *passwordRunner) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("no queries")
}
func (r *passwordRunner) QueryRow(string, ...any) *sql.Row                         { return nil }
func (r *passwordRunner) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }

func passwordWriter(runner *passwordRunner) *postgres.PostgreSQLWriter {
	return postgres.NewPostgreSQLWriterForRunnerWithCapabilities(runner, "public", platform.Postgres, capability.Postgres18())
}

// The statement names the role's variable, and the server receives the
// password the run declared, or the one the environment holds, in its place,
// as a literal whose backslashes and quotes read the same whatever
// standard_conforming_strings is; ADR 0071.
func TestWriterSendsTheDeclaredPassword_HappyPath(t *testing.T) {
	t.Setenv("PTAH_SECRET_PASSWORD_OPS", "from-env")
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{name: "a created role", statement: "CREATE ROLE app WITH LOGIN PASSWORD $PTAH_SECRET_PASSWORD_APP NOSUPERUSER;",
			want: `CREATE ROLE app WITH LOGIN PASSWORD E'a\'b\\c' NOSUPERUSER;`},
		{name: "an altered role, in lower case", statement: "alter role app password $PTAH_SECRET_PASSWORD_APP",
			want: `alter role app password E'a\'b\\c'`},
		{name: "a variable the run did not declare comes from the environment", statement: "ALTER USER ops PASSWORD $PTAH_SECRET_PASSWORD_OPS;",
			want: `ALTER USER ops PASSWORD E'from-env';`},
		{name: "a statement that names no password is sent as written", statement: "SELECT '$PTAH_SECRET_PASSWORD_APP';",
			want: "SELECT '$PTAH_SECRET_PASSWORD_APP';"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			runner := &passwordRunner{}
			ctx := rolepassword.With(c.Context(), map[string]string{"PTAH_SECRET_PASSWORD_APP": `a'b\c`})

			err := passwordWriter(runner).ExecuteSQL(ctx, tc.statement)

			c.Assert(err, qt.IsNil)
			c.Assert(runner.sent, qt.DeepEquals, []string{tc.want})
		})
	}
}

func TestWriterSendsTheDeclaredPassword_FailurePath(t *testing.T) {
	statements := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{name: "a variable nothing sets", statement: "CREATE ROLE app PASSWORD $PTAH_SECRET_PASSWORD_MISSING;",
			wantErr: "a role's password comes from environment variable PTAH_SECRET_PASSWORD_MISSING, which is not set"},
		{name: "a reference outside the password", statement: "SELECT $PTAH_SECRET_PASSWORD_APP;",
			wantErr: `a password variable is referred to outside the password of CREATE ROLE or ALTER ROLE: \$PTAH_SECRET_PASSWORD_APP at offset 7; .*`},
		{name: "a reference in another case", statement: "CREATE ROLE app PASSWORD $ptah_secret_password_app;",
			wantErr: `.*\$ptah_secret_password_app is not spelled with the PTAH_SECRET_ prefix in upper case`},
	}
	for _, tc := range statements {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			runner := &passwordRunner{}
			ctx := rolepassword.With(c.Context(), map[string]string{"PTAH_SECRET_PASSWORD_APP": "pw-SENTINEL"})

			err := passwordWriter(runner).ExecuteSQL(ctx, tc.statement)

			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*SENTINEL.*`)
			c.Assert(runner.sent, qt.HasLen, 0)
		})
	}
}

// A refusal the server answers with loses the password, in each form the
// statement held it in, and still unwraps to the server's error; the SQL the
// error shows is the statement as written, with the variable.
func TestWriterKeepsThePasswordOutOfARefusal_FailurePath(t *testing.T) {
	c := qt.New(t)
	refusal := errors.New(`ERROR: syntax error at or near "pw'SENTINEL" (also E'pw\'SENTINEL')`)
	runner := &passwordRunner{refusal: refusal}
	ctx := rolepassword.With(c.Context(), map[string]string{"PTAH_SECRET_PASSWORD_APP": "pw'SENTINEL"})

	err := passwordWriter(runner).ExecuteSQL(ctx, "CREATE ROLE app PASSWORD $PTAH_SECRET_PASSWORD_APP;")

	c.Assert(err, qt.ErrorIs, refusal)
	c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*SENTINEL.*`)
	c.Assert(err, qt.ErrorMatches, `(?s)SQL execution failed: .*\[secret\].*SQL: CREATE ROLE app PASSWORD \$PTAH_SECRET_PASSWORD_APP;`)
	c.Assert(runner.sent, qt.HasLen, 1)
}
