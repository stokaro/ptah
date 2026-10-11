package mysqllint_test

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/dialect/mysql/mysqllint"
)

// ExampleService shows the MySQL owner answering one lint run: a host reads
// the rules as data, here the online-mode rule, then asks for the findings of
// the codes the run enables over every file in one call.
func ExampleService() {
	for _, rule := range mysqllint.Rules() {
		if strings.HasPrefix(rule.Code, "ON") {
			fmt.Println(rule.Code, rule.Severity, rule.Targets, rule.Title)
		}
	}

	request := lintext.CheckRequest{
		Target: lintext.Target{Name: "mariadb"},
		Codes:  []string{"ON102"},
		Files: []lintext.File{{Name: "0000000001_add.up.sql", Version: 1, Up: true, Statements: []lintext.Statement{
			{Start: 0, End: 30, Line: 1, SQL: "ALTER TABLE t ADD COLUMN c int", Words: []string{"ALTER", "TABLE", "T", "ADD", "COLUMN", "C", "INT"}},
		}}},
	}
	result, err := mysqllint.Service{}.CheckMigrations(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, finding := range result.Findings {
		fmt.Println(finding.Code, request.Files[finding.File].Name, "statement", finding.Statement)
	}
	// Output:
	// ON102 error [mysql mariadb] statement does not ask MySQL to apply it online
	// ON102 0000000001_add.up.sql statement 0
}
