package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestYDBStreamingCheckpointLint(t *testing.T) {
	for _, test := range []struct {
		name, sql string
		want      []string
	}{
		{"drop", "DROP STREAMING QUERY q;", []string{"0001_q.up.sql:1:DS107"}},
		{"replace", "CREATE OR REPLACE STREAMING QUERY q AS DO BEGIN SELECT 1; END DO;", []string{"0001_q.up.sql:1:YD160"}},
		{"change body", "ALTER STREAMING QUERY q SET (FORCE = TRUE) AS DO BEGIN SELECT 2; END DO;", []string{"0001_q.up.sql:1:YD160"}},
		{"guarded replacement", "CREATE OR REPLACE STREAMING QUERY IF NOT EXISTS q AS DO BEGIN SELECT 1; END DO;", make([]string, 0)},
		{"stop", "ALTER STREAMING QUERY q SET (RUN = FALSE);", make([]string, 0)},
		{"create", "CREATE STREAMING QUERY q AS DO BEGIN SELECT 1; END DO;", make([]string, 0)},
		{"literal", "SELECT 'DROP STREAMING QUERY q';", make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbLint(c, map[string]string{"0001_q.up.sql": test.sql}, ""), qt.ContentEquals, test.want)
		})
	}
}
