package sqlreach

import (
	"slices"
	"strings"

	"ptah.run/internal/lexer"
)

// yqlTranslationSetting matches a --! comment at the head of a YQL text, which
// the YQL lexer emits as one TokenUnknown so it survives as a significant
// token.
func yqlTranslationSetting() tokenMatcher {
	return func(ctx scanContext) bool {
		for _, token := range ctx.tokens {
			if token.Type == lexer.TokenUnknown && strings.HasPrefix(token.Value, "--!") {
				return true
			}
		}
		return false
	}
}

// yqlNamespaceCall matches `::`, which in YQL only ever separates a UDF
// module from its function. Two colons with only whitespace or a comment
// between them are matched too: no YQL construct writes them, so refusing
// them costs nothing and asks no question of the grammar.
func yqlNamespaceCall() tokenMatcher {
	return func(ctx scanContext) bool {
		for i := 0; i+1 < len(ctx.tokens); i++ {
			if ctx.tokens[i].MatchOperatorValue(":") && ctx.tokens[i+1].MatchOperatorValue(":") {
				return true
			}
		}
		return false
	}
}

// yqlFileFunctions are the YQL builtins that read a file attached to the
// query (FileContent, FilePath, Files, FolderPath, ParseFile), list a folder
// (the FOLDER and WalkFolders table functions), or read a secret
// (SecureParam). The list is read off YQL's builtin table,
// yql/essentials/sql/v1/translation/builtin.cpp in ydb-platform/ydb at
// 707eb94859da0a77296b93daf42c687ddbc5abec. Measured on YDB 26.2.1.14,
// FileContent('x') answers `File not found: x`, Files('dir') answers an empty
// list, and SecureParam('token:default') answers `unknown token id`: each is
// live, and only what the query has attached stands between it and a value.
func yqlFileFunctions() []string {
	return []string{
		"FILECONTENT", "FILEPATH", "FILES", "FOLDERPATH", "PARSEFILE",
		"FOLDER", "WALKFOLDERS", "SECUREPARAM",
	}
}

// yqlCodeFunctions are the YQL builtins that call a UDF by name or mark an
// expression as having side effects. The builtins that evaluate or build code
// are a class, matched by [yqlCodeCall]: every Evaluate* (EvaluateCode
// answered 1 for EvaluateCode(QuoteCode(1)) on YDB 26.2.1.14) and every
// *Code (QuoteCode, FuncCode, LambdaCode, ListCode, AtomCode, ReprCode,
// FormatCode, WorldCode).
func yqlCodeFunctions() []string {
	return []string{"UDF", "SCRIPTUDF", "WITHSIDEEFFECTS", "WITHSIDEEFFECTSMODE"}
}

// yqlCallAnyOf matches a call to any of names: a name, bare or backticked, in
// any letter case, followed by an opening parenthesis. Unlike [calledFunction]
// it exempts no position: in YQL a call after ON is a join condition, not an
// object name, and a builtin cannot be declared under another name.
func yqlCallAnyOf(names []string) tokenMatcher {
	return yqlCallMatching(func(name string) bool { return slices.Contains(names, name) })
}

// yqlCallWithPrefix matches a call to any name starting with prefix.
func yqlCallWithPrefix(prefix string) tokenMatcher {
	return yqlCallMatching(func(name string) bool { return strings.HasPrefix(name, prefix) })
}

// yqlCodeCall matches a call that evaluates or builds code, or calls a UDF by
// name; see [yqlCodeFunctions].
func yqlCodeCall() tokenMatcher {
	names := yqlCodeFunctions()
	return yqlCallMatching(func(name string) bool {
		return slices.Contains(names, name) || strings.HasPrefix(name, "EVALUATE") || strings.HasSuffix(name, "CODE")
	})
}

func yqlCallMatching(matches func(string) bool) tokenMatcher {
	return func(ctx scanContext) bool {
		for i := 0; i+1 < len(ctx.tokens); i++ {
			if !ctx.tokens[i+1].MatchOperatorValue("(") {
				continue
			}
			if name, ok := callableName(ctx.tokens[i]); ok && matches(name) {
				return true
			}
		}
		return false
	}
}

// yqlSourceAnchors are the words a YQL source follows: FROM and JOIN in a
// select, ANY in front of a source, PROCESS and REDUCE, and COMBINE with its
// second source after WITH.
var yqlSourceAnchors = []string{"FROM", "JOIN", "ANY", "PROCESS", "REDUCE", "COMBINE", "WITH"}

// yqlSourceListEnds are the words that end a list of sources written with
// commas, at the depth the list began. ON and USING do not end one: a comma
// after a join condition joins another source.
var yqlSourceListEnds = []string{
	"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "WINDOW", "UNION", "INTERSECT",
	"EXCEPT", "INTO", "SELECT", "ASSUME", "PRESORT",
}

// yqlDottedSource matches a source written with a dot or a colon: the
// `cluster.table` and `source.path` forms YQL reads as something other than a
// table of this database, and `cluster:dir.table`, which the grammar's
// cluster_expr allows. Measured on YDB 26.2.1.14, FROM `dir`.`t` answers
// `Unknown cluster: dir`, while a table in a directory is one path, `dir/t`.
//
// A source is anything at a source position: after one of
// [yqlSourceAnchors], after STREAM there, and after a comma in a list
// of sources, which the grammar reads as a join (join_op: COMMA). Default
// deny: the name there is refused when a dot or a colon follows it, whatever
// the name is.
func yqlDottedSource() tokenMatcher {
	return func(ctx scanContext) bool {
		depth := 0
		lists := make(map[int]bool)
		for i, token := range ctx.tokens {
			switch {
			case token.MatchOperatorValue("("):
				depth++
				continue
			case token.MatchOperatorValue(")"):
				delete(lists, depth)
				depth--
				continue
			case token.Type == lexer.TokenSemicolon:
				clear(lists)
				continue
			case token.Type == lexer.TokenIdentifier && slices.Contains(yqlSourceListEnds, strings.ToUpper(token.Value)):
				delete(lists, depth)
			}
			anchor := token.Type == lexer.TokenIdentifier && slices.Contains(yqlSourceAnchors, strings.ToUpper(token.Value))
			if anchor && !IsKeyword(token, "ANY") && !IsKeyword(token, "WITH") {
				lists[depth] = true
			}
			if (anchor || token.MatchOperatorValue(",") && lists[depth]) && yqlDottedNameAt(ctx.tokens, i+1) {
				return true
			}
		}
		return false
	}
}

// yqlDottedNameAt reports whether the source starting at i is a name, or the
// asterisk the grammar allows for a cluster, followed by a dot or a colon.
// PROCESS STREAM puts STREAM in front of the source, and it is skipped; ANY
// needs no skip, because it is an anchor of its own.
func yqlDottedNameAt(tokens []lexer.Token, i int) bool {
	if i < len(tokens) && IsKeyword(tokens[i], "STREAM") {
		i++
	}
	if i+1 >= len(tokens) {
		return false
	}
	name := tokens[i].Type == lexer.TokenIdentifier || tokens[i].Type == lexer.TokenString ||
		tokens[i].MatchOperatorValue("*")
	return name && (tokens[i+1].MatchOperatorValue(".") || tokens[i+1].MatchOperatorValue(":"))
}
