package lexer_test

import (
	"os"
	"testing"

	"ptah.run/internal/lexer"
)

func FuzzLexer(f *testing.F) {
	for _, path := range []string{
		"../../integration/internal/fixtures/migrations/basic/0000000001_create_users_table.up.sql",
		"../../integration/internal/fixtures/migrations/basic/0000000002_create_posts_table.up.sql",
		"../../integration/internal/fixtures/migrations/basic/0000000003_create_comments_table.up.sql",
		"../../integration/internal/fixtures/migrations/basic_mysql/0000000001_create_users_table.up.sql",
		"../../integration/internal/fixtures/migrations/basic_mysql/0000000002_create_posts_table.up.sql",
	} {
		addLexerSeedFile(f, path)
	}

	for _, seed := range []string{
		"CREATE TABLE café (id INT, naïve TEXT);",
		"CREATE TABLE таблица (ключ INT);",
		"$тег$héllo$тег$",
		"SELECT 'héllo 🚀';",
		"--!syntax_v1\nSELECT @@a@@@@b@@j, \"x\\\"y\"u, `t\\`n`;",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		for _, options := range []lexer.Options{{}, {YQL: true}} {
			l := lexer.NewLexerWithOptions(input, options)
			reached := false
			for tokens := 0; tokens <= len(input)+1 && !reached; tokens++ {
				reached = l.NextToken().Type == lexer.TokenEOF
			}
			if !reached {
				t.Fatalf("lexer with options %+v did not reach EOF after %d tokens", options, len(input)+1)
			}
		}
	})
}

func addLexerSeedFile(f *testing.F, path string) {
	f.Helper()

	seed, err := os.ReadFile(path)
	if err != nil {
		f.Fatalf("read fuzz seed %s: %v", path, err)
	}
	f.Add(string(seed))
}
