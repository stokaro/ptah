package lintdialect_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/lintdialect"
)

func TestValid_HappyPath(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{
		"",
		"postgres",
		"mysql",
		"mariadb",
		"sqlite",
		"clickhouse",
		"cockroachdb",
		"yugabytedb",
		"spanner",
	} {
		c.Run(dialect, func(c *qt.C) {
			c.Assert(lintdialect.Valid(dialect), qt.IsTrue)
		})
	}
}

func TestValid_FailurePath(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{"oracle", "POSTGRES", " postgres"} {
		c.Run(dialect, func(c *qt.C) {
			c.Assert(lintdialect.Valid(dialect), qt.IsFalse)
		})
	}
}
