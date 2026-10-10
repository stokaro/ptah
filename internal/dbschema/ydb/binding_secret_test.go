package ydb_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	ydbschema "ptah.run/internal/dbschema/ydb"
	"ptah.run/internal/ydbsecretvalue"
)

// errServerRefused stands in for a refusal the server answers a statement with.
var errServerRefused = errors.New("server refused")

// queryConn stands in for a ydb-go-sdk connection that records the text each
// statement reached it with, and, when refuse is set, answers with an error
// that repeats that text, as a server's parse error can repeat a token.
type queryConn struct {
	recordingConn
	sent   *[]string
	refuse bool
}

func (q queryConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	*q.sent = append(*q.sent, query)
	if q.refuse {
		return nil, fmt.Errorf("%w near %q", errServerRefused, query)
	}
	return driver.RowsAffected(0), nil
}

func (q queryConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	*q.sent = append(*q.sent, query)
	return nil, fmt.Errorf("%w near %q", errServerRefused, query)
}

// queryConnector hands out queryConn.
type queryConnector struct {
	sent   *[]string
	refuse bool
}

func (q queryConnector) Connect(context.Context) (driver.Conn, error) {
	return queryConn{sent: q.sent, refuse: q.refuse}, nil
}
func (queryConnector) Driver() driver.Driver { return nil }

// openQueryPool opens a pool over queryConn and returns the texts its
// statements reach the connection with.
func openQueryPool(c *qt.C, refuse bool) (*sql.DB, *[]string) {
	c.Helper()
	var sent []string
	db := sql.OpenDB(ydbschema.NewBindingConnector(queryConnector{sent: &sent, refuse: refuse}, nil))
	c.Cleanup(func() { _ = db.Close() })
	return db, &sent
}

// A statement that refers to a secret value reaches the server with the value
// defined in front of it, read from the environment as the statement runs;
// the text the caller passed holds only the variable's name.
func TestBindingConnector_DefinesASecretValue(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SECRET_BINDING_PW", "pw-SENTINEL")
	db, sent := openQueryPool(c, false)

	_, err := db.ExecContext(context.Background(), "CREATE SECRET `pw` WITH (value = $PTAH_SECRET_BINDING_PW)")

	c.Assert(err, qt.IsNil)
	c.Assert(*sent, qt.DeepEquals, []string{
		"$PTAH_SECRET_BINDING_PW = 'pw-SENTINEL';\nCREATE SECRET `pw` WITH (value = $PTAH_SECRET_BINDING_PW)",
	})
}

// A refusal the server answers a statement with reaches the caller without
// the value it defined, whichever way the statement was sent, and still
// matches the error it wraps.
func TestBindingConnector_RedactsTheValueFromARefusal(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SECRET_BINDING_PW", "pw-SENTINEL")
	db, _ := openQueryPool(c, true)
	statement := "ALTER SECRET `pw` WITH (value = $PTAH_SECRET_BINDING_PW)"

	_, execErr := db.ExecContext(context.Background(), statement)
	queryErr := db.QueryRowContext(context.Background(), statement).Err()

	for _, err := range []error{execErr, queryErr} {
		c.Assert(err, qt.ErrorIs, errServerRefused)
		c.Assert(err, qt.ErrorMatches, `(?s)server refused near "\$PTAH_SECRET_BINDING_PW = '\[secret\]';.*`)
		c.Assert(err, qt.Not(qt.ErrorMatches), "(?s).*SENTINEL.*")
	}
}

// The driver may quote the expanded query in an error. Quotes, backslashes
// and control characters in a value must stay hidden in that representation.
func TestBindingConnector_RedactsEscapedSecretValues(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SECRET_BINDING_PW", "pw-SENTINEL 'quoted' \\ slash\nline")
	db, _ := openQueryPool(c, true)
	statement := "ALTER SECRET `pw` WITH (value = $PTAH_SECRET_BINDING_PW)"

	_, execErr := db.ExecContext(context.Background(), statement)
	queryErr := db.QueryRowContext(context.Background(), statement).Err()

	for _, err := range []error{execErr, queryErr} {
		c.Assert(err, qt.ErrorIs, errServerRefused)
		c.Assert(err, qt.Not(qt.ErrorMatches), "(?s).*SENTINEL.*")
		c.Assert(err.Error(), qt.Contains, "[secret]")
	}
}

// A statement that would carry a value anywhere but into a secret, or whose
// variable is not set, never reaches the server; neither does one prepared
// for later, which would keep its value in a text the connection holds on to.
func TestBindingConnector_FailurePath_RefusesASecretValueBeforeSendingIt(t *testing.T) {
	t.Setenv("PTAH_SECRET_BINDING_PW", "pw-SENTINEL")
	tests := []struct {
		name    string
		run     func(*sql.DB) error
		wantErr string
	}{
		{
			name: "a value read back",
			run: func(db *sql.DB) error {
				_, err := db.ExecContext(context.Background(), "UPSERT INTO t (v) VALUES ($PTAH_SECRET_BINDING_PW)")
				return err
			},
			wantErr: `ydb: a secret value is referred to outside the value of CREATE SECRET or ALTER SECRET: .*`,
		},
		{
			name: "a variable that is not set",
			run: func(db *sql.DB) error {
				_, err := db.ExecContext(context.Background(), "CREATE SECRET pw WITH (value = $PTAH_SECRET_BINDING_UNSET)")
				return err
			},
			wantErr: "ydb: a secret's value comes from environment variable PTAH_SECRET_BINDING_UNSET, which is not set",
		},
		{
			name: "a prepared statement",
			run: func(db *sql.DB) error {
				_, err := db.PrepareContext(context.Background(), "CREATE SECRET pw WITH (value = $PTAH_SECRET_BINDING_PW)")
				return err
			},
			wantErr: "ydb: a prepared statement cannot take a secret's value; run the statement that refers to " +
				`\$PTAH_SECRET_BINDING_PW directly`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, sent := openQueryPool(c, false)

			err := test.run(db)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(*sent, qt.HasLen, 0)
		})
	}
	t.Run("the reference error is the package's sentinel", func(t *testing.T) {
		c := qt.New(t)
		db, _ := openQueryPool(c, false)
		_, err := db.ExecContext(context.Background(), "SELECT $PTAH_SECRET_BINDING_PW")
		c.Assert(err, qt.ErrorIs, ydbsecretvalue.ErrReference)
	})
}
