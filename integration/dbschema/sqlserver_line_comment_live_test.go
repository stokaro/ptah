//go:build integration

package dbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestSQLServerLiveEndsALineCommentAtACarriageReturn pins the fact the probe
// text check reads SQL Server by: a line comment ends at a carriage return,
// as it ends at a line feed, so the statement after one runs. The probe text
// check reads T-SQL by the shared lexer's rules alone, which end a comment at
// either. The control has no break, and the comment takes the statement with
// it.
func TestSQLServerLiveEndsALineCommentAtACarriageReturn(t *testing.T) {
	for _, test := range []struct {
		name  string
		batch string
		want  int
	}{
		{name: "a carriage return", batch: "DECLARE @x int = 1; -- c\rSET @x = 2;\nSELECT @x", want: 2},
		{name: "a line feed", batch: "DECLARE @x int = 1; -- c\nSET @x = 2;\nSELECT @x", want: 2},
		{name: "no break", batch: "DECLARE @x int = 1; -- c SET @x = 2;\nSELECT @x", want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := probeTransactionConnection(c, dbtarget.SQLServer)
			var got int

			err := conn.QueryRowContext(c.Context(), test.batch).Scan(&got)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
