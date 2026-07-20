package lexer_test

import (
	"os"
	"testing"

	"github.com/stokaro/ptah/internal/lexer"
)

func FuzzLexer(f *testing.F) {
	for _, path := range []string{
		"../../integration/fixtures/migrations/basic/0000000001_create_users_table.up.sql",
		"../../integration/fixtures/migrations/basic/0000000002_create_posts_table.up.sql",
		"../../integration/fixtures/migrations/basic/0000000003_create_comments_table.up.sql",
		"../../integration/fixtures/migrations/basic_mysql/0000000001_create_users_table.up.sql",
		"../../integration/fixtures/migrations/basic_mysql/0000000002_create_posts_table.up.sql",
	} {
		addLexerSeedFile(f, path)
	}

	for _, seed := range []string{
		"CREATE TABLE café (id INT, naïve TEXT);",
		"CREATE TABLE таблица (ключ INT);",
		"$тег$héllo$тег$",
		"SELECT 'héllo 🚀';",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		l := lexer.NewLexer(input)
		for tokens := 0; tokens <= len(input)+1; tokens++ {
			if l.NextToken().Type == lexer.TokenEOF {
				return
			}
		}
		t.Fatalf("lexer did not reach EOF after %d tokens", len(input)+1)
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
