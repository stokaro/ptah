package mssqllint_test

import (
	"context"
	"fmt"

	"ptah.run/core/lintext"
	"ptah.run/dialect/mssql/mssqllint"
)

// ExampleSplitter shows the SQL Server owner splitting a migration and
// judging its PolyBase drop, which a safety assessment reads in place of the
// statement's words. The ordinary drop beside it is left to those words.
func ExampleSplitter() {
	request := lintext.SplitRequest{Target: "sqlserver", Sources: []lintext.Source{{
		Name: "0000000002_lake.up.sql",
		SQL:  "DROP EXTERNAL TABLE [dbo].[events];\nDROP TABLE [dbo].[orders];\n",
	}}}
	result, err := mssqllint.Splitter{}.SplitStatements(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, statement := range result.Files[0] {
		fmt.Println(statement.Line, statement.Canonical, len(statement.Effects))
		for _, effect := range statement.Effects {
			fmt.Printf("  %s %s\n", effect.Fact, effect.Impact)
		}
	}
	// Output:
	// 1 DROP EXTERNAL TABLE [DBO].[EVENTS] 1
	//   safety-effect behavioral
	// 2 DROP TABLE [DBO].[ORDERS] 0
}
