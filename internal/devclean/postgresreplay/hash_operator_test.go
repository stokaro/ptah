package postgresreplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/postgresreplay"
	"ptah.run/internal/devclean/replay"
)

// guardFor returns the replay guard of the PostgreSQL-family service named dialect.
func guardFor(c *qt.C, dialect string) replay.Guard {
	c.Helper()
	for _, service := range postgresreplay.Services() {
		if service.Dialect == dialect {
			return service.Guard
		}
	}
	c.Fatalf("no replay service for %s", dialect)
	return nil
}

// # is an operator character on PostgreSQL, so the rest of its line is SQL the
// server runs and the guard has to read. A guard that read it as a comment
// would accept these statements.
func TestGuard_HashOperatorDoesNotHideSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{"terminate a backend", "SELECT 1 # pg_terminate_backend(1)"},
		{"dblink call in a subselect", "SELECT 1 # (SELECT dblink_exec('x','y'))"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := guardFor(c, "postgres").Validate(test.statement, replay.Scope{})

			c.Assert(err, qt.IsNotNil)
		})
	}
}

// The same operator in a statement with nothing to refuse is accepted.
func TestGuard_HashOperator_HappyPath(t *testing.T) {
	c := qt.New(t)

	err := guardFor(c, "postgres").Validate("SELECT 5 # 3", replay.Scope{})

	c.Assert(err, qt.IsNil)
}
