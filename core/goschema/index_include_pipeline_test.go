package goschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
)

func TestIndexIncludeAnnotationRendersPostgreSQLCoveringIndex(t *testing.T) {
	c := qt.New(t)

	database := mustParseSource(c.TB, "accounts.go", `package models

//ptah:schema:table name="accounts"
type Account struct {
	//ptah:schema:field name="email" type="TEXT" not_null="true"
	Email string

	//ptah:schema:field name="display_name" type="TEXT" not_null="true"
	DisplayName string

	//ptah:schema:field name="created_at" type="TIMESTAMPTZ" not_null="true"
	CreatedAt string

	//ptah:schema:index name="idx_accounts_email" fields="email" include=" display_name, created_at "
	_ int
}
`)

	statements, err := renderer.GetOrderedCreateStatements(&database, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(
		strings.Join(statements, "\n"),
		qt.Contains,
		`CREATE INDEX IF NOT EXISTS "idx_accounts_email" ON "accounts" ("email") INCLUDE ("display_name", "created_at");`,
	)
}
