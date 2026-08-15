package dialectlexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/lexer"
)

func TestOptions_HappyPath(t *testing.T) {

	tests := []struct {
		name    string
		dialect string
		want    lexer.Options
	}{
		{
			name:    "mysql",
			dialect: "mysql",
			want: lexer.Options{
				StandardStrings:                true,
				BackslashEscapes:               true,
				RequireWhitespaceAfterDashDash: true,
				ExecutableComments:             lexer.ExecutableCommentsMySQL,
			},
		},
		{
			name:    "mariadb",
			dialect: "mariadb",
			want: lexer.Options{
				StandardStrings:                true,
				BackslashEscapes:               true,
				RequireWhitespaceAfterDashDash: true,
				ExecutableComments:             lexer.ExecutableCommentsMariaDB,
			},
		},
		{
			name:    "postgres",
			dialect: "postgresql",
			want: lexer.Options{
				StandardStrings:         true,
				PostgreSQLEscapeStrings: true,
			},
		},
		{
			name:    "sql server",
			dialect: "mssql",
			want: lexer.Options{
				StandardStrings:     true,
				BracketIdentifiers:  true,
				DisableHashComments: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := dialectlexer.Options(test.dialect)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
