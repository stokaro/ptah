package lint

import (
	"fmt"
	"slices"
	"strings"
)

// A routine that builds a statement at run time and executes it is the one
// place a migration can introduce SQL injection: a value that reaches the
// routine unquoted becomes part of the statement text. Atlas SA101 reports
// "unsafe dynamic SQL patterns, such as string concatenation or variable
// interpolation", found in EXEC and EXECUTE statements that concatenate
// strings into SQL text. What Ptah can observe is the same thing and no
// more: the tokenized body of a routine a migration defines, which is what
// [Statement.Routine] carries once a dialect names the body's language. A
// value's origin outside the routine, and a routine defined outside the
// migration directory, are not visible and are not claimed.
//
// Three spellings build a statement from a value, one per body language:
//
//	PL/pgSQL   EXECUTE <expression> [INTO ...] [USING ...]
//	MySQL      PREPARE name FROM <expression>
//	T-SQL      EXEC (<expression>)  and  EXEC[UTE] sp_executesql <expression>
//
// The expression is safe when every part of it is a string literal or a
// call that quotes its argument -- quote_ident(), quote_literal(),
// quote_nullable(), and format() whose specifiers are all %I, %L or %%. It
// is reported when a part is anything else: an identifier or variable, a
// concatenation with || or +, CONCAT(), or a format() that interpolates a
// value with %s. Values passed through PL/pgSQL's USING and T-SQL's
// sp_executesql parameters never touch the text, so those forms are safe by
// construction when the text itself is a literal.
//
// A T-SQL EXEC that names a procedure, and a MySQL EXECUTE that runs a
// prepared name, run what is already written down and are not sites.

// injectionSite is one statement in a body that builds and runs SQL.
type injectionSite struct {
	// form is the spelling the message names.
	form string
	// operand is the first unsafe part of the expression, as written.
	operand string
	// how says what makes it unsafe.
	how string
}

// injectionSites scans a routine body for the three spellings.
func injectionSites(dialect string, body []string) []injectionSite {
	var sites []injectionSite
	for i := range body {
		switch {
		case body[i] == "EXECUTE" && dialect == "postgres":
			if site, ok := unsafeExpression("EXECUTE", body, i+1, expressionEnd(body, i+1, "INTO", "USING")); ok {
				sites = append(sites, site)
			}
		case body[i] == "PREPARE" && i+2 < len(body) && body[i+2] == "FROM" && (dialect == "mysql" || dialect == "mariadb"):
			if site, ok := unsafeExpression("PREPARE ... FROM", body, i+3, expressionEnd(body, i+3)); ok {
				sites = append(sites, site)
			}
		case (body[i] == "EXEC" || body[i] == "EXECUTE") && dialect == "sqlserver":
			if site, ok := sqlServerInjectionSite(body, i); ok {
				sites = append(sites, site)
			}
		}
	}
	return sites
}

// sqlServerInjectionSite reads EXEC (<expression>) and EXEC sp_executesql
// <expression>. EXEC name runs a procedure and is not a site.
func sqlServerInjectionSite(body []string, i int) (injectionSite, bool) {
	j := i + 1
	if j >= len(body) {
		return injectionSite{}, false
	}
	switch body[j] {
	case "(":
		return unsafeExpression("EXEC (...)", body, j+1, matchingParen(body, j))
	case "SP_EXECUTESQL":
		return unsafeExpression("EXEC sp_executesql", body, j+1, expressionEnd(body, j+1, ","))
	}
	return injectionSite{}, false
}

// expressionEnd finds where an expression ends: at a top-level `;`, at one of
// the stop words, or at the end of the body.
func expressionEnd(body []string, start int, stops ...string) int {
	depth := 0
	for k := start; k < len(body); k++ {
		switch body[k] {
		case "(":
			depth++
		case ")":
			if depth == 0 {
				return k
			}
			depth--
		case ";":
			if depth == 0 {
				return k
			}
		default:
			if depth == 0 && slices.Contains(stops, body[k]) {
				return k
			}
		}
	}
	return len(body)
}

// matchingParen returns the index of the parenthesis closing body[open].
func matchingParen(body []string, open int) int {
	depth := 0
	for k := open; k < len(body); k++ {
		switch body[k] {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return k
			}
		}
	}
	return len(body)
}

// unsafeExpression judges the expression body[start:end] and reports the
// first part that can carry a value into the statement text.
func unsafeExpression(form string, body []string, start, end int) (injectionSite, bool) {
	if start >= end {
		return injectionSite{}, false
	}
	for _, part := range concatenationParts(body[start:end]) {
		if operand, how, unsafe := unsafePart(part); unsafe {
			return injectionSite{form: form, operand: operand, how: how}, true
		}
	}
	return injectionSite{}, false
}

// concatenationParts splits an expression at its top-level || and +
// operators.
func concatenationParts(expr []string) [][]string {
	var parts [][]string
	depth := 0
	start := 0
	for k := 0; k < len(expr); k++ {
		switch expr[k] {
		case "(":
			depth++
		case ")":
			depth--
		case "+":
			if depth == 0 {
				parts = append(parts, expr[start:k])
				start = k + 1
			}
		case "|":
			if depth == 0 && k+1 < len(expr) && expr[k+1] == "|" {
				parts = append(parts, expr[start:k])
				k++
				start = k + 1
			}
		}
	}
	return append(parts, expr[start:])
}

// quotingCalls are the calls whose result is safe to place in a statement.
var quotingCalls = map[string]bool{"QUOTE_IDENT": true, "QUOTE_LITERAL": true, "QUOTE_NULLABLE": true, "QUOTENAME": true}

// unsafePart judges one operand of a concatenation.
func unsafePart(part []string) (operand, how string, unsafe bool) {
	words := trimOuterParens(part)
	if len(words) == 0 {
		return "", "", false
	}
	if len(words) == 1 && isStringLiteral(words[0]) {
		return "", "", false
	}
	// T-SQL's N'...' is a literal with a prefix word.
	if len(words) == 2 && words[0] == "N" && isStringLiteral(words[1]) {
		return "", "", false
	}
	if len(words) > 1 && words[1] == "(" {
		switch {
		case quotingCalls[words[0]]:
			return "", "", false
		case words[0] == "FORMAT":
			if spec, ok := unquotedFormatSpecifier(words); ok {
				return spelled(words), fmt.Sprintf("format() interpolates a value with %s, which places it in the text unquoted", spec), true
			}
			return "", "", false
		case words[0] == "CONCAT":
			return spelled(words), "CONCAT() joins a value into the text unquoted", true
		}
		return spelled(words), "a call whose result is placed in the text unquoted", true
	}
	return spelled(words), "a value placed in the text unquoted", true
}

// unquotedFormatSpecifier finds a %s in format()'s first argument, the one
// specifier that interpolates without quoting; %I quotes an identifier, %L
// a literal, and %% is a percent sign.
func unquotedFormatSpecifier(words []string) (string, bool) {
	for _, word := range words {
		if !isStringLiteral(word) {
			continue
		}
		text := word
		for {
			i := strings.IndexByte(text, '%')
			if i < 0 || i+1 >= len(text) {
				return "", false
			}
			spec := text[i : i+2]
			switch spec {
			case "%I", "%L", "%%":
				text = text[i+2:]
				continue
			}
			return "%" + strings.TrimLeft(text[i+1:i+2], "0123456789-"), true
		}
	}
	return "", false
}

func trimOuterParens(words []string) []string {
	for len(words) >= 2 && words[0] == "(" && matchingParen(words, 0) == len(words)-1 {
		words = words[1 : len(words)-1]
	}
	return words
}

func isStringLiteral(word string) bool {
	return len(word) >= 2 && word[0] == '\'' && word[len(word)-1] == '\''
}

// spelled joins words back into readable text for the message.
func spelled(words []string) string {
	var b strings.Builder
	for i, word := range words {
		if i > 0 && word != "(" && word != ")" && word != "," && words[i-1] != "(" && words[i-1] != "@" {
			b.WriteString(" ")
		}
		b.WriteString(strings.ToLower(word))
	}
	return b.String()
}

// injectionAdvice is per language, because the safe form is.
func injectionAdvice(dialect string) string {
	switch dialect {
	case "postgres":
		return "quote an identifier with quote_ident() or format('%I'), a literal with quote_literal() or format('%L'), and pass values through USING with $1 placeholders"
	case "mysql", "mariadb":
		return "prepare a literal statement with ? placeholders and pass the values through EXECUTE ... USING; an identifier that must vary has no placeholder and needs a whitelist"
	case "sqlserver":
		return "pass values as parameters of sp_executesql and wrap an identifier in QUOTENAME() before it joins the text"
	default:
		return "quote every identifier and literal that joins the text, and pass values as parameters"
	}
}

func injectionRules() []Rule {
	return []Rule{sqlInjectionRule()}
}

func sqlInjectionRule() Rule {
	return Rule{
		Code:     "SA101",
		Title:    "dynamic SQL built from a value",
		Severity: SeverityWarning,
		Input:    InputRoutineBody,
		CheckFile: func(file *File) []Finding {
			var findings []Finding
			for index := range file.Statements {
				stmt := &file.Statements[index]
				if stmt.Routine == nil {
					continue
				}
				for _, site := range injectionSites(file.dialect, stmt.Routine.BodyWords) {
					message := fmt.Sprintf("%s builds its statement from %s: %s, so a value that reaches the routine can rewrite the statement (SQL injection). "+
						"In this routine, %s", site.form, site.operand, site.how, injectionAdvice(file.dialect))
					findings = append(findings, Finding{
						Rule:     "SA101",
						Title:    "dynamic SQL built from a value",
						Severity: SeverityWarning,
						File:     file.Path,
						Line:     stmt.Line,
						Message:  message,
						Context:  statementFindingContext(index),
					})
				}
			}
			return findings
		},
		AppliesToDown: true,
	}
}
