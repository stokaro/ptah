package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// An index annotation's comment is the index's comment on every dialect. An
// index reads only platform.* attributes as source properties, so the
// MySQL-family comment shortcut a table annotation has must not become an
// index property that no MySQL owner claims and the decoder then refuses.
func TestIndexAnnotationCommentRendersAndComparesOnTheMySQLFamily(t *testing.T) {
	const source = `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)"
	Email string

	//ptah:schema:index name="idx_email" fields="email" comment="Email lookup"
	_ int
}
`
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(builtintest.Annotations(), "models.go", source)
			c.Assert(err, qt.IsNil)
			c.Assert(db.Indexes, qt.HasLen, 1)
			c.Assert(db.Indexes[0].Overrides, qt.IsNil)
			statements, err := builtin.GetOrderedCreateStatements(&db, dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, "Email lookup")
			diff, err := schemadiff.CompareWithDialect(t.Context(), &db, &catalog.Database{}, dialect, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.IsTrue)
		})
	}
}
