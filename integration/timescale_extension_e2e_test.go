//go:build integration

package integration_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
)

// TestTimescaleHypertableKeepsItsExtensionE2E pins that a declared hypertable
// counts as needing timescaledb. The Go source declares a hypertable and no
// extension, and a Go source claims to describe every extension, so without
// the rule the plan drops timescaledb on every run: the server refuses the
// drop while the hypertable depends on it, and the plan never converges.
func TestTimescaleHypertableKeepsItsExtensionE2E(t *testing.T) {
	c := qt.New(t)
	ctx, conn, schemaName := timescaleSchemaFixture(c, "ts_needs")
	source := fmt.Sprintf("package models\n\n"+
		"//ptah:schema:table name=\"readings\" schema=%q\ntype Reading struct {\n"+
		"\t//ptah:schema:field name=\"time\" type=\"TIMESTAMPTZ\" not_null=\"true\"\n\tTime string\n"+
		"\t//ptah:schema:field name=\"value\" type=\"INTEGER\" not_null=\"true\"\n\tValue int\n}\n\n"+
		"//ptah:schema:hypertable table=\"%s.readings\" column=\"time\"\ntype ReadingsHypertable struct{}\n", schemaName, schemaName)
	parsed, err := goschema.ParseSource(builtintest.Annotations(), "readings.go", source)
	c.Assert(err, qt.IsNil)
	declared := &parsed
	declared.Schemas = []schemamodel.Schema{{Name: schemaName}}

	statements := applyTimescale(ctx, c, conn, declared, schemaName)

	script := strings.Join(statements, "\n")
	c.Assert(script, qt.Contains, "SELECT create_hypertable(")
	c.Assert(script, qt.Not(qt.Contains), "EXTENSION")
	c.Assert(planTimescale(c, conn, declared, schemaName), qt.HasLen, 0)
}
