package dbexprprobe_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/internal/dbexprprobe"
	"ptah.run/internal/dbschema/dbtest"
)

type exclusionSession struct{ db *sql.DB }

func (s exclusionSession) Info() catalog.ServerInfo { return catalog.ServerInfo{Dialect: "postgres"} }

func (s exclusionSession) WithRolledBackTransaction(ctx context.Context, _ string, body func(context.Context, *sql.Tx) error) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	err = body(ctx, tx)
	return true, errors.Join(err, tx.Rollback())
}

// A response the parser cannot represent is an error for the whole batch. An
// earlier successful response must not escape as a usable normalization map.
func TestResolveExcludeExpressions_UnreadDefinitionDiscardsBatch(t *testing.T) {
	c := qt.New(t)
	definitions := []string{"EXCLUDE USING btree (r WITH =)", "EXCLUDE USING btree (r WITH =) INCLUDE (s)"}
	calls := 0
	db := dbtest.Open(c, func(_ string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		definition := definitions[calls]
		calls++
		return dbtest.QueryResult{Columns: []string{"definition"}, Rows: [][]driver.Value{{definition}}}, nil
	})
	columns := []dbexprprobe.CheckProbeColumn{{Name: "r", Type: "integer"}}
	got, err := dbexprprobe.ResolveExcludeExpressions(c.Context(), exclusionSession{db.SQL}, []dbexprprobe.ExcludeExpressionProbe{
		{Key: "first", Columns: columns, UsingMethod: "btree", Elements: "r WITH ="},
		{Key: "second", Columns: columns, UsingMethod: "btree", Elements: "r WITH ="},
	})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*read normalized EXCLUDE constraint "second": .*clause INCLUDE`)
	c.Assert(got, qt.IsNil)
	c.Assert(calls, qt.Equals, 2)
}
