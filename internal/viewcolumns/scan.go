package viewcolumns

import "strings"

// depthScanner walks SQL text tracking what would hide a comma or a keyword
// from a reader: nesting, string literals, quoted identifiers and comments.
//
// It is deliberately not a SQL parser. It answers one question -- is this
// position at the top level of the text -- which is what separating a select
// list needs, and it is the same shape the SQLite reader uses to split a table
// body.
type depthScanner struct {
	depth     int
	inSingle  bool
	inDouble  bool
	inLine    bool
	inBlock   bool
	blockPrev rune
}

// top reports whether the scanner is currently outside every nesting and quote.
func (s *depthScanner) top() bool {
	return s.depth == 0 && !s.inSingle && !s.inDouble && !s.inLine && !s.inBlock
}

// step advances the scanner over one rune, given the one after it, and reports
// whether the position it just left was at the top level.
func (s *depthScanner) step(current, next rune) bool {
	wasTop := s.top()
	s.advance(current, next)
	return wasTop
}

func (s *depthScanner) advance(current, next rune) {
	switch {
	case s.inLine:
		s.leaveLineComment(current)
	case s.inBlock:
		s.leaveBlockComment(current)
	case s.inSingle:
		s.inSingle = current != '\''
	case s.inDouble:
		s.inDouble = current != '"'
	default:
		s.advanceOutsideQuotes(current, next)
	}
}

func (s *depthScanner) leaveLineComment(current rune) {
	s.inLine = current != '\n'
}

func (s *depthScanner) leaveBlockComment(current rune) {
	s.inBlock = s.blockPrev != '*' || current != '/'
	s.blockPrev = current
}

func (s *depthScanner) advanceOutsideQuotes(current, next rune) {
	switch {
	case current == '-' && next == '-':
		s.inLine = true
	case current == '/' && next == '*':
		s.inBlock = true
		s.blockPrev = 0
	case current == '\'':
		s.inSingle = true
	case current == '"':
		s.inDouble = true
	case current == '(' || current == '[':
		s.depth++
	case current == ')' || current == ']':
		s.depth--
	}
}

// splitTopLevel splits text on commas that sit at the top level.
func splitTopLevel(text string) []string {
	runes := []rune(text)
	var scanner depthScanner
	var parts []string
	start := 0
	for i, r := range runes {
		wasTop := scanner.step(r, runeAt(runes, i+1))
		if wasTop && r == ',' {
			parts = append(parts, string(runes[start:i]))
			start = i + 1
		}
	}
	return appendTail(parts, string(runes[start:]))
}

// appendTail adds the last part, and answers with no parts at all for text that
// holds nothing but separators and space.
func appendTail(parts []string, tail string) []string {
	if strings.TrimSpace(tail) == "" && len(parts) == 0 {
		return nil
	}
	return append(parts, tail)
}

// topLevelKeyword returns the offset of the first top-level occurrence of the
// keyword as a whole word, or -1.
func topLevelKeyword(text, keyword string) int {
	runes := []rune(text)
	lowered := []rune(strings.ToLower(text))
	target := []rune(strings.ToLower(keyword))
	var scanner depthScanner
	for i := range runes {
		wasTop := scanner.step(runes[i], runeAt(runes, i+1))
		if wasTop && matchesWord(lowered, target, i) {
			return len(string(runes[:i]))
		}
	}
	return -1
}

// matchesWord reports whether target sits at i as a whole word.
func matchesWord(text, target []rune, i int) bool {
	if i+len(target) > len(text) {
		return false
	}
	if string(text[i:i+len(target)]) != string(target) {
		return false
	}
	return !isWordRune(runeAt(text, i-1)) && !isWordRune(runeAt(text, i+len(target)))
}

func isWordRune(r rune) bool {
	return r == '_' || r == '$' ||
		(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// runeAt returns the rune at i, or 0 when i is outside the text.
func runeAt(runes []rune, i int) rune {
	if i < 0 || i >= len(runes) {
		return 0
	}
	return runes[i]
}
