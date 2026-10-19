package parser

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/lexer"
)

// wordOption is a table option a dialect's descriptor spells as words, such
// as SQLite's WITHOUT ROWID: the option as the parser reports it, and its
// words.
type wordOption struct {
	option string
	words  []string
}

// wordOptions are the table options the descriptors of the process registry
// spell as words, each once, in registry order.
//
// Every dialect reads every one of them, and so does a parse with no dialect.
// A file is parsed with no dialect on paths that only classify it, and at this
// position, after the column list, no grammar this parser reads spells these
// words as anything else. Reading only the document's dialect's options would
// refuse a valid file for want of a dialect.
var wordOptions = descriptortable.NewIndex(func(dialects []descriptor.Dialect) []wordOption {
	var options []wordOption
	for _, described := range dialects {
		for _, option := range described.TableOptions {
			if len(option.Words) == 0 {
				continue
			}
			spelled := wordOption{option: option.Option, words: option.Words}
			if !slices.ContainsFunc(options, spelled.equal) {
				options = append(options, spelled)
			}
		}
	}
	return options
})

func (o wordOption) equal(other wordOption) bool {
	return o.option == other.option && slices.Equal(o.words, other.words)
}

// spells reports whether upcoming, the words from the cursor, open with the
// option's words.
func (o wordOption) spells(upcoming []string) bool {
	return len(upcoming) >= len(o.words) && slices.EqualFunc(o.words, upcoming[:len(o.words)], strings.EqualFold)
}

// handleWordTableOption reads a table option a descriptor spells as words and
// reports whether option, the word at the cursor, opens one. The option is
// set to true. Where option opens several, the one whose words all follow is
// read, or else the first, and its first missing word is the error.
func (p *Parser) handleWordTableOption(table *ast.CreateTableNode, option string) (bool, error) {
	var opened []wordOption
	longest := 0
	for _, spelled := range wordOptions.Get() {
		if strings.EqualFold(spelled.words[0], option) {
			opened = append(opened, spelled)
			longest = max(longest, len(spelled.words))
		}
	}
	if len(opened) == 0 {
		return false, nil
	}
	upcoming := p.upcomingWords(longest)
	read := opened[0]
	if index := slices.IndexFunc(opened, func(spelled wordOption) bool { return spelled.spells(upcoming) }); index >= 0 {
		read = opened[index]
	}

	p.advance()
	for i, word := range read.words[1:] {
		if err := p.expect(lexer.TokenIdentifier, word); err != nil {
			return true, fmt.Errorf("expected %s after %s: %w", word, read.words[i], err)
		}
	}
	table.SetOption(read.option, "true")
	return true, nil
}
