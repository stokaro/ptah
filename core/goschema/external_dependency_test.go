package goschema_test

import (
	"bytes"
	"log/slog"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

func TestFinalize_DoesNotReportExternalDependencyAsCycle(t *testing.T) {
	c := qt.New(t)
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})
	database := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Order", Name: "orders"}},
		Fields: []goschema.Field{{
			StructName: "Order",
			Name:       "user_id",
			Type:       "INTEGER",
			Foreign:    "users(id)",
		}},
	}

	goschema.Finalize(database)

	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Name, qt.Equals, "orders")
	c.Assert(logs.String(), qt.Not(qt.Contains), "Circular dependency detected")
}
