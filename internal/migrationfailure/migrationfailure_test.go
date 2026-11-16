package migrationfailure_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/dbschema"
	"ptah.run/internal/migrationfailure"
	"ptah.run/internal/serverdiag"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// answeringInterceptor answers the statement that starts with failing with
// answer, as a server would, and lets every other statement run. Its zero
// value lets every statement run.
type answeringInterceptor struct {
	failing string
	answer  error
}

func (answeringInterceptor) ValidateDirectives(map[string]string) error { return nil }

func (i answeringInterceptor) ExecuteStatement(
	_ context.Context, _ *dbschema.DatabaseConnection, statement string, _ map[string]string,
) (bool, error) {
	if i.failing != "" && strings.HasPrefix(statement, i.failing) {
		return true, i.answer
	}
	return false, nil
}

// redactingError stands for a writer that wrote a password into the statement
// it sent.
type redactingError struct{ err error }

func (e redactingError) Error() string                       { return e.err.Error() }
func (e redactingError) Unwrap() error                       { return e.err }
func (e redactingError) RedactServerText(text string) string { return text }

// The second migration's failing statement is the third line of its file, and
// starts in the second column.
const failingBody = "-- notes\nCREATE TABLE notes (id INTEGER PRIMARY KEY);\n INSERT INTO notes (id)\nVALUES (lower(42));\n"

// fixture is a directory whose second migration fails at its second statement.
var fixture = fstest.MapFS{
	"0000000001_init.up.sql":    {Data: []byte("CREATE TABLE seen (id INTEGER PRIMARY KEY);\n")},
	"0000000001_init.down.sql":  {Data: []byte("DROP TABLE seen;\n")},
	"0000000002_notes.up.sql":   {Data: []byte(failingBody)},
	"0000000002_notes.down.sql": {Data: []byte("DROP TABLE notes;\n")},
}

// runError is the error an up run over fsys on a new SQLite database fails
// with, its statements answered by interceptor.
func runError(c *qt.C, fsys fstest.MapFS, interceptor answeringInterceptor, options ...migrator.FSProviderOption) error {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "failure.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	mig, err := migrator.NewFSMigrator(conn, fsys, append(options, migrator.WithStatementInterceptor(interceptor))...)
	c.Assert(err, qt.IsNil)
	runErr := mig.MigrateUp(c.Context())
	c.Assert(runErr, qt.IsNotNil)
	return runErr
}

// functionAnswer is what PostgreSQL answers the failing statement with: a hint
// and a position, and no detail or object.
func functionAnswer() *pgconn.PgError {
	return &pgconn.PgError{
		Severity: "ERROR",
		Code:     "42883",
		Message:  "function lower(integer) does not exist",
		Hint:     "No function matches the given name and argument types.",
		Position: 32,
	}
}

func TestNew_HappyPath(t *testing.T) {
	answer := &serverdiag.Answer{
		Severity: "ERROR",
		SQLState: "42883",
		Message:  "function lower(integer) does not exist",
		Hint:     "No function matches the given name and argument types.",
		Position: 32,
	}
	statementFailure := migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 2, Statements: 2, SQLState: "42883"}
	tests := []struct {
		name    string
		answer  error
		options migrationfailure.Options
		want    migrationfailure.Details
	}{
		{
			name:    "the position is placed in the statement and in the file",
			answer:  functionAnswer(),
			options: migrationfailure.Options{Source: fixture},
			want: migrationfailure.Details{
				Failure:  statementFailure,
				File:     "0000000002_notes.up.sql",
				Location: &migrationfailure.Location{Line: 2, Column: 9, FileLine: 4, FileColumn: 9},
				Server:   answer,
			},
		},
		{
			name:   "without the directory the position is placed in the statement alone",
			answer: functionAnswer(),
			want: migrationfailure.Details{
				Failure:  statementFailure,
				File:     "0000000002_notes.up.sql",
				Location: &migrationfailure.Location{Line: 2, Column: 9},
				Server:   answer,
			},
		},
		{
			name:    "a renamed file is named and read as the directory holds it",
			answer:  functionAnswer(),
			options: migrationfailure.Options{Source: fstest.MapFS{"V2__notes.sql": {Data: []byte(failingBody)}}, Renamed: map[string]string{"0000000002_notes.up.sql": "V2__notes.sql"}},
			want: migrationfailure.Details{
				Failure:  statementFailure,
				File:     "V2__notes.sql",
				Location: &migrationfailure.Location{Line: 2, Column: 9, FileLine: 4, FileColumn: 9},
				Server:   answer,
			},
		},
		{
			name:    "a statement the writer changed before sending it has no location",
			answer:  redactingError{err: functionAnswer()},
			options: migrationfailure.Options{Source: fixture},
			want:    migrationfailure.Details{Failure: statementFailure, File: "0000000002_notes.up.sql", Server: answer},
		},
		{
			name:    "a statement no server answered has no server details and no location",
			answer:  errors.New("unexpected EOF"),
			options: migrationfailure.Options{Source: fixture},
			want: migrationfailure.Details{
				Failure: migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 2, Statements: 2},
				File:    "0000000002_notes.up.sql",
			},
		},
		{
			name:    "the caller's redaction reaches the server's fields",
			answer:  &pgconn.PgError{Code: "08001", Message: "password=hunter2 refused"},
			options: migrationfailure.Options{Redact: func(text string) string { return strings.ReplaceAll(text, "hunter2", "***") }},
			want: migrationfailure.Details{
				Failure: migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 2, Statements: 2, SQLState: "08001"},
				File:    "0000000002_notes.up.sql",
				Server:  &serverdiag.Answer{SQLState: "08001", Message: "password=*** refused"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := runError(c, fixture, answeringInterceptor{failing: "INSERT INTO notes", answer: test.answer})

			got := migrationfailure.New(err, test.options)

			c.Assert(got, qt.DeepEquals, &test.want)
		})
	}
}

// A deferred constraint fails at the commit, after every statement ran, so the
// details name the commit and no statement, file or position.
func TestNewOfACommit_HappyPath(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_init.up.sql": {Data: []byte(
			"CREATE TABLE parents (id INTEGER PRIMARY KEY);\n" +
				"CREATE TABLE children (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parents (id) DEFERRABLE INITIALLY DEFERRED);\n",
		)},
		"0000000001_init.down.sql": {Data: []byte("DROP TABLE children;\nDROP TABLE parents;\n")},
		"0000000002_rows.up.sql": {Data: []byte(
			"INSERT INTO children (id, parent) VALUES (1, 999);\nINSERT INTO children (id, parent) VALUES (2, 998);\n",
		)},
		"0000000002_rows.down.sql": {Data: []byte("DELETE FROM children;\n")},
	}
	err := runError(c, fsys, answeringInterceptor{})

	got := migrationfailure.New(err, migrationfailure.Options{Source: fsys})

	c.Assert(got, qt.DeepEquals, &migrationfailure.Details{
		Failure: migrator.RunFailure{Version: 2, VersionKey: "2", Statements: 2, Commit: true},
	})
}

// A statement of an Atlas txtar file comes from the file's migration.sql
// section. The details name the file the directory holds and place the
// position in the statement alone: the statement appears once in the file,
// but the section is not the file, so no line of the file is named.
func TestNewOfATxtarMigration_HappyPath(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"20260801000001_init.sql":  {Data: []byte("CREATE TABLE notes (id INTEGER PRIMARY KEY);\n")},
		"20260801000002_notes.sql": {Data: []byte("-- atlas:txtar\n\n-- migration.sql --\nINSERT INTO notes (id)\nVALUES (lower(42));\n")},
	}
	err := runError(c, fsys, answeringInterceptor{failing: "INSERT INTO notes", answer: functionAnswer()},
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))

	got := migrationfailure.New(err, migrationfailure.Options{Source: fsys})

	c.Assert(got, qt.DeepEquals, &migrationfailure.Details{
		Failure: migrator.RunFailure{
			Version: 20260801000002, VersionKey: "20260801000002", Statement: 1, Statements: 1, SQLState: "42883",
		},
		File:     "20260801000002_notes.sql",
		Location: &migrationfailure.Location{Line: 2, Column: 9},
		Server: &serverdiag.Answer{
			Severity: "ERROR", SQLState: "42883", Message: "function lower(integer) does not exist",
			Hint: "No function matches the given name and argument types.", Position: 32,
		},
	})
}

// A run that stopped at something other than a statement or a commit has no
// details: no statement was sent for the server to answer.
func TestNew_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "no error"},
		{name: "an error that names no migration", err: errors.New("could not take the migration lock")},
		{name: "a statement refused before it was sent", err: &migrator.FailedMigrationError{
			Version: 2, VersionKey: "2", Err: &migrator.MigrationExecutionError{Err: errors.New("refused"), Statement: "VACUUM", StatementIndex: 1, Total: 1},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationfailure.New(test.err, migrationfailure.Options{Source: fixture}), qt.IsNil)
		})
	}
}

func TestLocate_HappyPath(t *testing.T) {
	statement := "INSERT INTO notes (id)\nVALUES (lower(42))"
	tests := []struct {
		name      string
		statement string
		position  int
		source    fstest.MapFS
		file      string
		want      migrationfailure.Location
	}{
		{name: "the first character", statement: statement, position: 1, want: migrationfailure.Location{Line: 1, Column: 1}},
		{name: "a later line", statement: statement, position: 32, want: migrationfailure.Location{Line: 2, Column: 9}},
		{name: "one past the last character, where the end of the input is", statement: "SELECT", position: 7, want: migrationfailure.Location{Line: 1, Column: 7}},
		{name: "a column counts characters, not bytes", statement: "SELECT 'é', lower(1)", position: 13, want: migrationfailure.Location{Line: 1, Column: 13}},
		{
			name: "the statement appears once in the file", statement: statement, position: 32,
			source: fstest.MapFS{"a.sql": {Data: []byte(failingBody)}}, file: "a.sql",
			want: migrationfailure.Location{Line: 2, Column: 9, FileLine: 4, FileColumn: 9},
		},
		{
			name: "a file column counts from the start of the file's line", statement: "SELECT lower(1)", position: 8,
			source: fstest.MapFS{"a.sql": {Data: []byte("-- é\r\n  SELECT lower(1);\n")}}, file: "a.sql",
			want: migrationfailure.Location{Line: 1, Column: 8, FileLine: 2, FileColumn: 10},
		},
		{
			name: "a statement that appears twice has no place in the file", statement: "SELECT lower(1)", position: 8,
			source: fstest.MapFS{"a.sql": {Data: []byte("SELECT lower(1);\nSELECT lower(1);\n")}}, file: "a.sql",
			want: migrationfailure.Location{Line: 1, Column: 8},
		},
		{
			name: "a statement the file does not hold as sent has no place in it", statement: "SELECT lower(1)", position: 8,
			source: fstest.MapFS{"a.sql": {Data: []byte("SELECT /* kept */ lower(1);\n")}}, file: "a.sql",
			want: migrationfailure.Location{Line: 1, Column: 8},
		},
		{
			name: "a file the directory does not hold has no place either", statement: "SELECT lower(1)", position: 8,
			source: fstest.MapFS{"a.sql": {Data: []byte("SELECT lower(1);\n")}}, file: "a.sql#migration",
			want: migrationfailure.Location{Line: 1, Column: 8},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := migrationfailure.Locate(test.statement, test.position, test.source, test.file)
			c.Assert(got, qt.DeepEquals, &test.want)
		})
	}
}

// A position that does not fall in the statement places nothing.
func TestLocate_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		position  int
	}{
		{name: "no position", statement: "SELECT 1", position: 0},
		{name: "a negative position", statement: "SELECT 1", position: -3},
		{name: "past the end of the input", statement: "SELECT 1", position: 10},
		{name: "a statement that is not text", statement: "SELECT '\xff'", position: 2},
		{name: "no statement, as for a commit", statement: "", position: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationfailure.Locate(test.statement, test.position, fixture, "0000000002_notes.up.sql"), qt.IsNil)
		})
	}
}

func TestDetailsText(t *testing.T) {
	tests := []struct {
		name    string
		details migrationfailure.Details
		want    string
	}{
		{
			name: "a statement the server answered",
			details: migrationfailure.Details{
				Failure:  migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 1, Statements: 3},
				File:     "0000000002_orders.up.sql",
				Location: &migrationfailure.Location{Line: 1, Column: 13, FileLine: 4, FileColumn: 13},
				Server: &serverdiag.Answer{
					Severity: "ERROR", SQLState: "23503", Message: "violates foreign key constraint",
					Detail: "Key (customer)=(42) is not present.", Hint: "Insert the customer first.", Position: 13,
					Where: "PL/pgSQL function load() line 3\nat SQL statement", Schema: "shop", Table: "orders",
					Column: "customer", DataType: "integer", Constraint: "orders_customer_fkey", Truncated: true,
				},
			},
			want: "server details (may contain application data):\n" +
				"  failed at: statement 1 of 3 of migration 2 (0000000002_orders.up.sql)\n" +
				"  sqlstate: 23503\n" +
				"  severity: ERROR\n" +
				"  message: violates foreign key constraint\n" +
				"  detail: Key (customer)=(42) is not present.\n" +
				"  hint: Insert the customer first.\n" +
				"  position: 13 (statement line 1, column 13; 0000000002_orders.up.sql line 4, column 13)\n" +
				"  where: PL/pgSQL function load() line 3\n    at SQL statement\n" +
				"  schema: shop\n" +
				"  table: orders\n" +
				"  column: customer\n" +
				"  data type: integer\n" +
				"  constraint: orders_customer_fkey\n" +
				"  note: a field longer than 1024 bytes is cut",
		},
		{
			name: "a position with no place in the file",
			details: migrationfailure.Details{
				Failure:  migrator.RunFailure{Version: 3, VersionKey: "20240103000000", Statement: 2, Statements: 2},
				Location: &migrationfailure.Location{Line: 2, Column: 1},
				Server:   &serverdiag.Answer{SQLState: "42601", Message: "syntax error", Position: 9},
			},
			want: "server details (may contain application data):\n" +
				"  failed at: statement 2 of 2 of migration 20240103000000\n" +
				"  sqlstate: 42601\n" +
				"  message: syntax error\n" +
				"  position: 9 (statement line 2, column 1)",
		},
		{
			name: "a commit",
			details: migrationfailure.Details{
				Failure: migrator.RunFailure{Version: 2, VersionKey: "2", Statements: 2, Commit: true},
				Server:  &serverdiag.Answer{SQLState: "23503", Detail: "Key (parent)=(999) is not present."},
			},
			want: "server details (may contain application data):\n" +
				"  failed at: the commit of the transaction that ran migration 2\n" +
				"  sqlstate: 23503\n" +
				"  detail: Key (parent)=(999) is not present.",
		},
		{
			name: "control characters the server quotes are escaped, but a tab and a newline are kept",
			details: migrationfailure.Details{
				Failure: migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 1, Statements: 1},
				Server: &serverdiag.Answer{
					SQLState: "23505",
					Detail:   "Key (note)=(\x1b[31mred\x1b[0m\rover\x7f\u009b) already exists.",
					Where:    "line 1\tcolumn 2\nline 2",
				},
			},
			want: "server details (may contain application data):\n" +
				"  failed at: statement 1 of 1 of migration 2\n" +
				"  sqlstate: 23505\n" +
				`  detail: Key (note)=(\x1b[31mred\x1b[0m\x0dover\x7f\u009b) already exists.` + "\n" +
				"  where: line 1\tcolumn 2\n    line 2",
		},
		{
			name: "format characters, the line and paragraph separators and a byte that is not UTF-8 are escaped",
			details: migrationfailure.Details{
				Failure: migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 1, Statements: 1},
				Server: &serverdiag.Answer{
					SQLState: "23505",
					Detail:   "Key (name)=(\u202egnp.exe\u200b\u2028\u2029\ufeff\U000e0001\xff) already exists.",
				},
			},
			want: "server details (may contain application data):\n" +
				"  failed at: statement 1 of 1 of migration 2\n" +
				"  sqlstate: 23505\n" +
				`  detail: Key (name)=(\u202egnp.exe\u200b\u2028\u2029\ufeff\U000e0001\xff) already exists.`,
		},
		{
			name: "a 1024-byte run of control characters is cut to 1024 bytes once escaped",
			details: migrationfailure.Details{
				Failure: migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 1, Statements: 1},
				Server:  &serverdiag.Answer{SQLState: "23505", Detail: strings.Repeat("\x1b", serverdiag.MaxFieldBytes)},
			},
			want: "server details (may contain application data):\n" +
				"  failed at: statement 1 of 1 of migration 2\n" +
				"  sqlstate: 23505\n" +
				"  detail: " + strings.Repeat(`\x1b`, serverdiag.MaxFieldBytes/4) + "\n" +
				"  note: a field longer than 1024 bytes is cut",
		},
		{
			name: "no answer",
			details: migrationfailure.Details{
				Failure: migrator.RunFailure{Version: 1, VersionKey: "1", Statement: 2, Statements: 2},
				File:    "0000000001_init.up.sql",
			},
			want: "server details (may contain application data):\n" +
				"  failed at: statement 2 of 2 of migration 1 (0000000001_init.up.sql)\n" +
				"  server: no details reported",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.details.Text(), qt.Equals, test.want)
		})
	}
}

// TestDetailsTextFieldBound measures the bytes a field takes in the text form,
// escapes and continuation indentation included, against the bound the JSON
// form holds the raw field to.
func TestDetailsTextFieldBound(t *testing.T) {
	tests := []struct {
		name      string
		detail    string
		wantBytes int
		wantNote  string
	}{
		{
			name:      "a run of ESC is cut between two escapes at the bound",
			detail:    strings.Repeat("\x1b", serverdiag.MaxFieldBytes),
			wantBytes: serverdiag.MaxFieldBytes,
			wantNote:  "a field longer than 1024 bytes is cut",
		},
		{
			name:      "an escape that would cross the bound is left out whole",
			detail:    "a" + strings.Repeat("\x1b", serverdiag.MaxFieldBytes-1),
			wantBytes: serverdiag.MaxFieldBytes - 3,
			wantNote:  "a field longer than 1024 bytes is cut",
		},
		{
			name:      "a run of C1 characters grows three times and is cut",
			detail:    strings.Repeat("\u009b", serverdiag.MaxFieldBytes/2),
			wantBytes: serverdiag.MaxFieldBytes / 6 * 6,
			wantNote:  "a field longer than 1024 bytes is cut",
		},
		{
			name:      "the indentation of continuation lines counts",
			detail:    strings.Repeat("\n", 300),
			wantBytes: serverdiag.MaxFieldBytes / 5 * 5,
			wantNote:  "a field longer than 1024 bytes is cut",
		},
		{
			name:      "a field of 1024 printable bytes is kept whole",
			detail:    strings.Repeat("a", serverdiag.MaxFieldBytes),
			wantBytes: serverdiag.MaxFieldBytes,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			details := migrationfailure.Details{
				Failure: migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 1, Statements: 1},
				Server:  &serverdiag.Answer{Detail: test.detail},
			}
			_, field, found := strings.Cut(details.Text(), "\n  detail: ")
			c.Assert(found, qt.IsTrue)
			field, note, _ := strings.Cut(field, "\n  note: ")
			c.Assert(field, qt.HasLen, test.wantBytes)
			c.Assert(note, qt.Equals, test.wantNote)
		})
	}
}
