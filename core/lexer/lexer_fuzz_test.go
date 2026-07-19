package lexer_test

import (
	"testing"

	"github.com/stokaro/ptah/core/lexer"
)

func FuzzLexerUTF8(f *testing.F) {
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
