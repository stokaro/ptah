// Package yql holds the reach rules of YDB's query language: the constructs
// of a YQL text that reach outside the database, or that change how the
// server reads the text so that what it runs cannot be judged. They serve the
// YQL lexer, which a dialect's descriptor names as descriptor.LexerYQL, so
// internal/sqlreach asks them of any dialect whose text is YQL.
package yql

import (
	"slices"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/lexer"
	"ptah.run/internal/reachext"
)

// Services returns the YQL lexer's reach services. YQL reads backslash
// escapes in every string; its one switch is a --!ansi_lexer translation
// setting, which the first rule refuses.
func Services() reachext.Services {
	return reachext.Services{
		Lexers:           []string{descriptor.LexerYQL},
		BackslashEscapes: true,
		Rules:            rules(),
	}
}

// rules are the YQL constructs, in the order a scan asks them.
func rules() []reachext.Rule {
	return []reachext.Rule{
		{
			// YQL reads translation settings from the head of the query, and
			// --!ansi_lexer changes how every string, name and comment after it
			// is read: a doubled quote becomes an escape and a backslash stops
			// being one, so `'a\'; DELETE ...` is a string to this scanner and
			// two statements to the server. The lexer models the default mode
			// only, so a setting is refused rather than read past.
			Construct: "YQL translation setting",
			Reach:     "changes how the server reads the text after it, so what the statement runs cannot be judged",
			Match:     translationSetting,
		},
		{
			// File, Folder, Library, Package and Udf fetch code or data from a
			// URL, and every other pragma changes how the rest of the query
			// resolves names or runs. A pragma is refused by name wherever it
			// appears, including inside an action body, where it starts no
			// statement this scanner would see.
			Construct: "PRAGMA",
			Reach:     "changes how the server resolves or runs the query, and several pragmas fetch code or data from a URL",
			Match:     containsKeyword("PRAGMA"),
		},
		{
			// Module::Function is how YQL calls a UDF, and the module may be a
			// script UDF (Python, JavaScript) or a native one that reaches the
			// network. The name of a module says nothing reliable about what it
			// does, so every call is refused: measured on YDB 26.2.1.14, the
			// module may also be written `String`::Contains.
			Construct: "YQL UDF call",
			Reach:     "runs a user-defined function module, and a module may execute a script or reach outside the database",
			Match:     namespaceCall,
		},
		{
			// These read files attached to the query, list folders, or read a
			// secret; PRAGMA File and PRAGMA Library are how a file is attached
			// from a URL. See fileFunctions for the list and its source.
			Construct: "YQL file or secret function",
			Reach:     "reads a file attached to the query, a folder listing or a secret",
			Match:     callAnyOf(fileFunctions()),
		},
		{
			Construct: "YQL code or UDF function",
			Reach:     "evaluates code built at run time or calls a user-defined function by name",
			Match:     codeCall(),
		},
		{
			// PgCall runs a PostgreSQL function inside YDB: measured on
			// 26.2.1.14, PgCall('version') answers `PostgreSQL 16.10 ...`, and
			// PgCall('pg_read_file', ...) answers `No access to proc:
			// pg_read_file`. Which functions the server allows is its decision,
			// so every Pg call is refused rather than a list of the dangerous
			// ones.
			Construct: "YQL PostgreSQL function",
			Reach:     "runs a PostgreSQL function inside YDB, and the server alone decides which functions may run",
			Match:     callWithPrefix("PG"),
		},
		{
			// A dotted source is a cluster or an external data source, never a
			// table of this database: measured on YDB 26.2.1.14, FROM `dir`.`t`
			// answers `Unknown cluster: dir`, while a table in a directory is
			// one path, `dir/t`. An external data source reads object storage
			// or another database over the network.
			Construct: "YQL external source",
			Reach:     "reads a cluster or an external data source: object storage or another database",
			Match:     dottedSource,
		},
	}
}

// translationSetting matches a --! comment at the head of a YQL text, which
// the YQL lexer emits as one TokenUnknown so it survives as a significant
// token.
func translationSetting(tokens []lexer.Token) bool {
	for _, token := range tokens {
		if token.Type == lexer.TokenUnknown && strings.HasPrefix(token.Value, "--!") {
			return true
		}
	}
	return false
}

// containsKeyword matches a bare keyword anywhere in the statement.
func containsKeyword(keyword string) func([]lexer.Token) bool {
	return func(tokens []lexer.Token) bool {
		for _, token := range tokens {
			if token.MatchIdentifierValue(keyword) {
				return true
			}
		}
		return false
	}
}

// namespaceCall matches `::`, which in YQL only ever separates a UDF module
// from its function. Two colons with only whitespace or a comment between
// them are matched too: no YQL construct writes them, so refusing them costs
// nothing and asks no question of the grammar.
func namespaceCall(tokens []lexer.Token) bool {
	for i := 0; i+1 < len(tokens); i++ {
		if tokens[i].MatchOperatorValue(":") && tokens[i+1].MatchOperatorValue(":") {
			return true
		}
	}
	return false
}

// fileFunctions are the YQL builtins that read a file attached to the query
// (FileContent, FilePath, Files, FolderPath, ParseFile), list a folder (the
// FOLDER and WalkFolders table functions), or read a secret (SecureParam).
// The list is read off YQL's builtin table,
// yql/essentials/sql/v1/translation/builtin.cpp in ydb-platform/ydb at
// 707eb94859da0a77296b93daf42c687ddbc5abec. Measured on YDB 26.2.1.14,
// FileContent('x') answers `File not found: x`, Files('dir') answers an empty
// list, and SecureParam('token:default') answers `unknown token id`: each is
// live, and only what the query has attached stands between it and a value.
func fileFunctions() []string {
	return []string{
		"FILECONTENT", "FILEPATH", "FILES", "FOLDERPATH", "PARSEFILE",
		"FOLDER", "WALKFOLDERS", "SECUREPARAM",
	}
}

// codeFunctions are the YQL builtins that call a UDF by name or mark an
// expression as having side effects. The builtins that evaluate or build code
// are a class, matched by [codeCall]: every Evaluate* (EvaluateCode answered
// 1 for EvaluateCode(QuoteCode(1)) on YDB 26.2.1.14) and every *Code
// (QuoteCode, FuncCode, LambdaCode, ListCode, AtomCode, ReprCode, FormatCode,
// WorldCode).
func codeFunctions() []string {
	return []string{"UDF", "SCRIPTUDF", "WITHSIDEEFFECTS", "WITHSIDEEFFECTSMODE"}
}

// callAnyOf matches a call to any of names: a name, bare or backticked, in any
// letter case, followed by an opening parenthesis. It exempts no position: in
// YQL a call after ON is a join condition, not an object name, and a builtin
// cannot be declared under another name.
func callAnyOf(names []string) func([]lexer.Token) bool {
	return callMatching(func(name string) bool { return slices.Contains(names, name) })
}

// callWithPrefix matches a call to any name starting with prefix.
func callWithPrefix(prefix string) func([]lexer.Token) bool {
	return callMatching(func(name string) bool { return strings.HasPrefix(name, prefix) })
}

// codeCall matches a call that evaluates or builds code, or calls a UDF by
// name; see [codeFunctions].
func codeCall() func([]lexer.Token) bool {
	names := codeFunctions()
	return callMatching(func(name string) bool {
		return slices.Contains(names, name) || strings.HasPrefix(name, "EVALUATE") || strings.HasSuffix(name, "CODE")
	})
}

func callMatching(matches func(string) bool) func([]lexer.Token) bool {
	return func(tokens []lexer.Token) bool {
		for i := 0; i+1 < len(tokens); i++ {
			if !tokens[i+1].MatchOperatorValue("(") {
				continue
			}
			if name, ok := reachext.CallableName(tokens[i]); ok && matches(name) {
				return true
			}
		}
		return false
	}
}

// sourceAnchors are the words a YQL source follows: FROM and JOIN in a
// select, ANY in front of a source, PROCESS and REDUCE, and COMBINE with its
// second source after WITH.
var sourceAnchors = []string{"FROM", "JOIN", "ANY", "PROCESS", "REDUCE", "COMBINE", "WITH"}

// sourceListEnds are the words that end a list of sources written with
// commas, at the depth the list began. ON and USING do not end one: a comma
// after a join condition joins another source.
var sourceListEnds = []string{
	"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "WINDOW", "UNION", "INTERSECT",
	"EXCEPT", "INTO", "SELECT", "ASSUME", "PRESORT",
}

// SourceListEnds returns the words that end a YQL list of sources written
// with commas, at the depth the list began, as a new slice. ON and USING do
// not end one: a comma after a join condition joins another source.
//
// It is exported because internal/yqlreach walks the same lists to name what
// a query reads, and two lists would agree only until one of them was
// extended.
func SourceListEnds() []string {
	return slices.Clone(sourceListEnds)
}

// dottedSource matches a source written with a dot or a colon: the
// `cluster.table` and `source.path` forms YQL reads as something other than a
// table of this database, and `cluster:dir.table`, which the grammar's
// cluster_expr allows. Measured on YDB 26.2.1.14, FROM `dir`.`t` answers
// `Unknown cluster: dir`, while a table in a directory is one path, `dir/t`.
//
// A source is anything at a source position: after one of [sourceAnchors],
// after STREAM there, and after a comma in a list of sources, which the
// grammar reads as a join (join_op: COMMA). Default deny: the name there is
// refused when a dot or a colon follows it, whatever the name is.
func dottedSource(tokens []lexer.Token) bool {
	depth := 0
	lists := make(map[int]bool)
	for i, token := range tokens {
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
		case token.Type == lexer.TokenIdentifier && slices.Contains(sourceListEnds, strings.ToUpper(token.Value)):
			delete(lists, depth)
		}
		anchor := token.Type == lexer.TokenIdentifier && slices.Contains(sourceAnchors, strings.ToUpper(token.Value))
		if anchor && !token.MatchIdentifierValue("ANY") && !token.MatchIdentifierValue("WITH") {
			lists[depth] = true
		}
		if (anchor || token.MatchOperatorValue(",") && lists[depth]) && dottedNameAt(tokens, i+1) {
			return true
		}
	}
	return false
}

// dottedNameAt reports whether the source starting at i is a name, or the
// asterisk the grammar allows for a cluster, followed by a dot or a colon.
// PROCESS STREAM puts STREAM in front of the source, and it is skipped; ANY
// needs no skip, because it is an anchor of its own.
func dottedNameAt(tokens []lexer.Token, i int) bool {
	if i < len(tokens) && tokens[i].MatchIdentifierValue("STREAM") {
		i++
	}
	if i+1 >= len(tokens) {
		return false
	}
	name := tokens[i].Type == lexer.TokenIdentifier || tokens[i].Type == lexer.TokenString ||
		tokens[i].MatchOperatorValue("*")
	return name && (tokens[i+1].MatchOperatorValue(".") || tokens[i+1].MatchOperatorValue(":"))
}
