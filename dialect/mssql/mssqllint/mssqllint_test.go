package mssqllint_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqllint"
	"ptah.run/internal/lintscan"
)

// sqlServerMigration holds both PolyBase drops among statements the owner
// does not judge, in T-SQL's spelling.
const sqlServerMigration = "CREATE EXTERNAL TABLE [dbo].[events] ([id] INT) WITH (LOCATION = '/events/', DATA_SOURCE = [lake], FILE_FORMAT = [parquet]);\n" +
	"DROP EXTERNAL TABLE [dbo].[events];\n" +
	"drop external data source [lake];\n" +
	"DROP EXTERNAL FILE FORMAT [parquet];\n" +
	"DROP TABLE [dbo].[orders];\n" +
	"SELECT 'DROP EXTERNAL TABLE [x]';\n"

// The owner judges the two PolyBase drops as behavioral, with its own
// reasons, and no other statement: a created external table, another
// external object, an ordinary drop the shared words read, and the words in a
// string literal.
func TestSplitter_JudgesPolyBaseDrops(t *testing.T) {
	c := qt.New(t)
	request := lintext.SplitRequest{Target: platform.SQLServer, Sources: []lintext.Source{{Name: "1.up.sql", SQL: sqlServerMigration}}}

	result, err := mssqllint.Splitter{}.SplitStatements(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(lintext.ValidateSplit(request, result), qt.IsNil)
	c.Assert(result.Judged, qt.DeepEquals, []lintext.Fact{lintext.FactSafetyEffect})
	var effects [][]lintext.Effect
	for _, statement := range result.Files[0] {
		effects = append(effects, statement.Effects)
	}
	c.Assert(effects, qt.DeepEquals, [][]lintext.Effect{
		nil,
		{{Fact: lintext.FactSafetyEffect, Impact: schemaext.Behavioral, Reason: mssqllint.DropExternalTableReason}},
		{{Fact: lintext.FactSafetyEffect, Impact: schemaext.Behavioral, Reason: mssqllint.DropExternalDataSourceReason}},
		nil,
		nil,
		nil,
	})
}

// The owner splits a migration exactly as the lint scanner splits it for the
// sqlserver target, so a lint run reads the same statements it read before
// the owner judged them.
func TestSplitter_SplitsAsTheScannerDoes(t *testing.T) {
	c := qt.New(t)
	request := lintext.SplitRequest{Target: platform.SQLServer, Sources: []lintext.Source{
		{Name: "1.up.sql", SQL: sqlServerMigration},
		{Name: "2.up.sql", SQL: "CREATE TABLE [t] ([a] INT);\nGO\nDROP EXTERNAL TABLE [t2];"},
	}}

	owned, err := mssqllint.Splitter{}.SplitStatements(c.Context(), request)
	c.Assert(err, qt.IsNil)
	scanned, err := lintscan.Splitter{Dialect: platform.SQLServer}.SplitStatements(c.Context(), request)
	c.Assert(err, qt.IsNil)

	for _, file := range owned.Files {
		for j := range file {
			file[j].Effects = nil
		}
	}
	c.Assert(owned.Files, qt.DeepEquals, scanned.Files)
}

func TestSplitter_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name    string
		ctx     context.Context
		target  string
		wantIs  error
		wantErr string
	}{
		{name: "another target", ctx: context.Background(), target: platform.Postgres, wantIs: ptaherr.ErrUnsupportedDialect,
			wantErr: `.*the "sqlserver" statement splitter on "postgres"`},
		{name: "no context", target: platform.SQLServer, wantIs: lintext.ErrInvalid, wantErr: `.*splitting requires a context`},
		{name: "canceled", ctx: canceled, target: platform.SQLServer, wantIs: context.Canceled, wantErr: `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := mssqllint.Splitter{}.SplitStatements(test.ctx, lintext.SplitRequest{Target: test.target,
				Sources: []lintext.Source{{Name: "1.up.sql", SQL: "DROP EXTERNAL TABLE [t];"}}})
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, lintext.SplitResult{})
		})
	}
}
