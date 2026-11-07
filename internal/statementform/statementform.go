// Package statementform reads the written form of a statement a dialect
// descriptor states, such as `SAVEPOINT <name>`, for the shared SQL parser to read
// statements against.
//
// A form is written as an engine's manual writes the statement. A word in capitals
// is that keyword, `<name>` is a possibly qualified name, `<names>` a comma-separated
// list of them, `[a|b]` is at most one of its alternatives, `{a|b}` exactly one, and
// `(a b),...` is a comma-separated list of the sequence in the parentheses.
//
// A form is read one token at a time with no backtracking, so each alternative opens
// with a keyword no other alternative of its group opens with, which [Parse] holds a
// form to. A form opens with a keyword, which is how a parser finds it.
package statementform

import (
	"errors"
	"fmt"
	"strings"
)

// Kind is what a [Term] matches.
type Kind uint8

const (
	// Word is the keyword in [Term.Word].
	Word Kind = iota
	// Name is a possibly qualified name.
	Name
	// List is the sequence in the only entry of [Term.Choices], repeated with a
	// comma between.
	List
	// Optional is at most one of [Term.Choices].
	Optional
	// Choice is exactly one of [Term.Choices].
	Choice
)

// Term is one step of a form.
type Term struct {
	Kind    Kind
	Word    string
	Choices [][]Term
}

// Parse reads form and returns its terms. It refuses a form a parser could not read
// one token at a time.
func Parse(form string) ([]Term, error) {
	r := reader{tokens: tokenize(form)}
	terms, err := r.sequence()
	if err != nil {
		return nil, fmt.Errorf("statement form %q: %w", form, err)
	}
	if !r.done() {
		return nil, fmt.Errorf("statement form %q: unexpected %q", form, r.peek())
	}
	if len(terms) == 0 || terms[0].Kind != Word {
		return nil, fmt.Errorf("statement form %q: a form opens with a keyword", form)
	}
	return terms, nil
}

// Opening returns the keyword form opens with, or the empty string for a form
// [Parse] refuses.
func Opening(form string) string {
	terms, err := Parse(form)
	if err != nil {
		return ""
	}
	return terms[0].Word
}

// tokenize splits a form on spaces, and around each character that is a token of
// its own.
func tokenize(form string) []string {
	spaced := strings.NewReplacer(
		"[", " [ ", "]", " ] ", "{", " { ", "}", " } ", "|", " | ", "(", " ( ", "),...", " ),... ",
	).Replace(form)
	return strings.Fields(spaced)
}

type reader struct {
	tokens []string
	at     int
}

func (r *reader) done() bool { return r.at >= len(r.tokens) }

func (r *reader) peek() string {
	if r.done() {
		return ""
	}
	return r.tokens[r.at]
}

func (r *reader) next() string {
	token := r.peek()
	r.at++
	return token
}

// sequence reads terms up to what closes the group they are in.
func (r *reader) sequence() ([]Term, error) {
	var terms []Term
	for !r.done() {
		switch r.peek() {
		case "]", "}", "|", "),...":
			return terms, nil
		}
		term, err := r.term()
		if err != nil {
			return nil, err
		}
		terms = append(terms, term)
	}
	return terms, nil
}

func (r *reader) term() (Term, error) {
	token := r.next()
	switch token {
	case "<name>":
		return Term{Kind: Name}, nil
	case "<names>":
		return Term{Kind: List, Choices: [][]Term{{{Kind: Name}}}}, nil
	case "[":
		return r.group(Optional, "]")
	case "{":
		return r.group(Choice, "}")
	case "(":
		terms, err := r.sequence()
		if err != nil {
			return Term{}, err
		}
		if r.next() != "),..." {
			return Term{}, errors.New("a list closes with ),... and this one does not")
		}
		if len(terms) == 0 {
			return Term{}, errors.New("a list repeats something")
		}
		return Term{Kind: List, Choices: [][]Term{terms}}, nil
	}
	if token != strings.ToUpper(token) || strings.ContainsAny(token, "<>") {
		return Term{}, fmt.Errorf("%q is neither a keyword in capitals nor a name", token)
	}
	return Term{Kind: Word, Word: token}, nil
}

// group reads the alternatives of a bracket up to closing.
func (r *reader) group(kind Kind, closing string) (Term, error) {
	group := Term{Kind: kind}
	seen := make(map[string]bool)
	for {
		terms, err := r.sequence()
		if err != nil {
			return Term{}, err
		}
		if len(terms) == 0 || terms[0].Kind != Word {
			return Term{}, fmt.Errorf("an alternative in %s opens with a keyword", closing)
		}
		if seen[terms[0].Word] {
			return Term{}, fmt.Errorf("two alternatives open with %s", terms[0].Word)
		}
		seen[terms[0].Word] = true
		group.Choices = append(group.Choices, terms)
		switch r.next() {
		case "|":
		case closing:
			return group, nil
		default:
			return Term{}, fmt.Errorf("a group closes with %s", closing)
		}
	}
}
