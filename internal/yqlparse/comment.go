package yqlparse

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
	"ptah.run/internal/ydbcomment"
)

// comment uses the same grammar and value limits as the statement executor.
// The schema converter resolves the target against this document's objects.
func (p *parser) comment() *ast.CommentNode {
	start, end := p.peek().Start, p.peek().Start
	for !p.done() && p.peek().Type != lexer.TokenSemicolon {
		end = p.peek().End
		p.pos++
	}
	text := p.text[start:end]
	query, recognized, err := ydbcomment.Recognize(text)
	switch {
	case err != nil:
		p.failf("%v", err)
	case !recognized:
		p.failf("unsupported YDB comment declaration")
	case strings.HasPrefix(query.Path, "/"):
		p.failf("desired YQL schema paths must be database-relative")
	}
	return ast.NewComment(text)
}
