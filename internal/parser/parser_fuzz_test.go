package parser_test

import (
	"os"
	"testing"
	"time"

	"github.com/stokaro/ptah/internal/parser"
)

func FuzzParseSQL(f *testing.F) {
	for _, path := range []string{
		"../../integration/fixtures/migrations/basic/0000000001_create_users_table.up.sql",
		"../../integration/fixtures/migrations/basic/0000000002_create_posts_table.up.sql",
		"../../integration/fixtures/migrations/basic/0000000003_create_comments_table.up.sql",
		"../../integration/fixtures/migrations/basic_mysql/0000000001_create_users_table.up.sql",
		"../../integration/fixtures/migrations/basic_mysql/0000000002_create_posts_table.up.sql",
		"../../integration/fixtures/migrations/basic_mysql/0000000003_create_comments_table.up.sql",
	} {
		addParserSeedFile(f, path)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		_, _ = parser.NewParser(sql, parser.WithTimeout(time.Second)).Parse()
	})
}

func addParserSeedFile(f *testing.F, path string) {
	f.Helper()

	seed, err := os.ReadFile(path)
	if err != nil {
		f.Fatalf("read fuzz seed %s: %v", path, err)
	}
	f.Add(string(seed))
}
