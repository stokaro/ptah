// Package pgpolicysource holds the source grammar of PostgreSQL row-security
// declarations that more than one frontend shares, for the owner of package
// pgpolicy: how a role list is spelled in a Go annotation or a YAML value.
// A reader and the writer that exports to the same format use it, so a
// written list reads back as the same selectors. [Annotations] is the owner's
// side of the Go annotation frontend, which hands it the row-level security
// declarations their target scope makes the owner's.
package pgpolicysource

import (
	"fmt"
	"slices"
	"strings"
	"unicode"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// keywords are the role keywords a list may spell bare, in any letter case.
var keywords = []pgpolicy.RoleKeyword{pgpolicy.Public, pgpolicy.CurrentRole, pgpolicy.CurrentUser, pgpolicy.SessionUser}

// ParseRoleList reads a role list as a TO clause spells it. Entries are
// separated by commas outside double quotes. A bare entry that spells a role
// keyword, in any letter case, is that keyword; any other bare entry is a role
// name folded to lower case, as PostgreSQL folds an unquoted identifier, and
// must be one. A double-quoted entry is the name exactly, with "" for a quote
// inside it. An empty entry, an unterminated quote and text after a closing
// quote are refused, the last two as an entry that is not one quoted name. The text must name at least one role; leaving the TO
// clause out is the caller's to represent, as nil selectors.
func ParseRoleList(text string) ([]pgpolicy.RoleSelector, error) {
	entries, err := splitRoleList(text)
	if err != nil {
		return nil, err
	}
	roles := make([]pgpolicy.RoleSelector, 0, len(entries))
	for _, entry := range entries {
		role, err := parseRole(entry)
		if err != nil {
			return nil, err
		}
		roles = append(roles, role)
	}
	return roles, nil
}

// FormatRoleList writes roles in their canonical order as [ParseRoleList]
// reads them back: a keyword bare, a name bare where PostgreSQL would fold the
// bare spelling to the name itself, and double-quoted otherwise.
func FormatRoleList(roles []pgpolicy.RoleSelector) string {
	parts := make([]string, 0, len(roles))
	for _, role := range pgpolicy.CanonicalRoles(roles) {
		switch {
		case role.Keyword != "":
			parts = append(parts, string(role.Keyword))
		case bareName(role.Name):
			parts = append(parts, role.Name)
		default:
			parts = append(parts, `"`+strings.ReplaceAll(role.Name, `"`, `""`)+`"`)
		}
	}
	return strings.Join(parts, ", ")
}

// splitRoleList splits text at the commas outside double quotes and trims
// each entry.
func splitRoleList(text string) ([]string, error) {
	var entries []string
	var current strings.Builder
	quoted := false
	for _, char := range text {
		switch {
		case char == '"':
			quoted = !quoted
			current.WriteRune(char)
		case char == ',' && !quoted:
			entries = append(entries, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteRune(char)
		}
	}
	entries = append(entries, strings.TrimSpace(current.String()))
	if slices.Contains(entries, "") {
		return nil, fmt.Errorf("%w: role list %q has an empty entry", schemaext.ErrInvalidValue, text)
	}
	return entries, nil
}

func parseRole(entry string) (pgpolicy.RoleSelector, error) {
	if strings.HasPrefix(entry, `"`) {
		if len(entry) < 2 || !strings.HasSuffix(entry, `"`) || strings.Contains(strings.ReplaceAll(entry[1:len(entry)-1], `""`, ""), `"`) {
			return pgpolicy.RoleSelector{}, fmt.Errorf("%w: role %s is not one double-quoted name", schemaext.ErrInvalidValue, entry)
		}
		return pgpolicy.RoleSelector{Name: strings.ReplaceAll(entry[1:len(entry)-1], `""`, `"`)}, nil
	}
	for _, keyword := range keywords {
		if strings.EqualFold(entry, string(keyword)) {
			return pgpolicy.RoleSelector{Keyword: keyword}, nil
		}
	}
	if !plainIdentifier(entry) {
		return pgpolicy.RoleSelector{}, fmt.Errorf("%w: role %q must be double-quoted to be a name", schemaext.ErrInvalidValue, entry)
	}
	return pgpolicy.RoleSelector{Name: lowerASCII(entry)}, nil
}

// SpellsKeyword reports a role name spelled like a role keyword in some
// letter case, which a format that writes keywords and names alike cannot
// keep apart.
func SpellsKeyword(name string) bool {
	for _, keyword := range keywords {
		if strings.EqualFold(name, string(keyword)) {
			return true
		}
	}
	return false
}

// bareName reports a name PostgreSQL reads back unchanged when it is written
// without quotes: an identifier in lower case that spells no keyword.
func bareName(name string) bool {
	return plainIdentifier(name) && lowerASCII(name) == name && !SpellsKeyword(name)
}

// plainIdentifier reports an unquoted PostgreSQL identifier: a letter or an
// underscore, then letters, digits, underscores and dollar signs.
func plainIdentifier(text string) bool {
	for i, char := range text {
		letter := unicode.IsLetter(char) || char == '_'
		if !letter && (i == 0 || (!unicode.IsDigit(char) && char != '$')) {
			return false
		}
	}
	return text != ""
}

// lowerASCII folds ASCII letters only, as PostgreSQL folds an unquoted
// identifier in a multibyte encoding.
func lowerASCII(text string) string {
	return strings.Map(func(char rune) rune {
		if char >= 'A' && char <= 'Z' {
			return char + ('a' - 'A')
		}
		return char
	}, text)
}
