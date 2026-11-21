// Package parseutils implements the low-level annotation text parsing shared by
// goschema: splitting key="value" comment options into maps and extracting
// platform-specific override groups.
package parseutils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/annotationmeta"
	"ptah.run/internal/descriptortable"
)

var keyValuePairRe = regexp.MustCompile(`(\w+(?:\.\w+)*)=("(?:\\.|[^"\\])*"|[^\s]+)`)
var bareWordRe = regexp.MustCompile(`\b(\w+(?:\.\w+)*)\b`)

// KeyValueParser splits annotation comment options into maps. It reads the
// directives of one catalog: which attributes a directive declares, which it
// declares as bare booleans, and which it retires.
type KeyValueParser struct {
	catalog annotationmeta.Catalog
}

// NewKeyValueParser returns a parser for the directives of catalog.
func NewKeyValueParser(catalog annotationmeta.Catalog) KeyValueParser {
	return KeyValueParser{catalog: catalog}
}

// BareValueError reports a bare word that names an attribute taking a value.
// Dropping the word would discard what its author wrote, and reading it as
// true would give the attribute a value nobody wrote.
type BareValueError struct {
	// Directive is the directive of the comment, without its leading slashes.
	Directive string
	// Attribute is the attribute the bare word names.
	Attribute string
}

func (e *BareValueError) Error() string {
	return fmt.Sprintf("%s on //%s takes a value and is written bare; write %s=\"...\"", e.Attribute, e.Directive, e.Attribute)
}

// ParseKeyValueComment splits comment into its attributes. A quoted value is
// unquoted, and a key written with a value wins over the same key written
// bare. A bare word after the directive name reads as true when the directive
// declares it a bare boolean, and also when the directive does not declare it
// or retired it, so that validation sees and refuses the last two by name. A
// bare word naming any other attribute the directive accepts is refused with
// a *BareValueError.
func (p KeyValueParser) ParseKeyValueComment(comment string) (map[string]string, error) {
	result := make(map[string]string)
	for _, match := range keyValuePairRe.FindAllStringSubmatch(comment, -1) {
		key := match[1]
		value := match[2]
		if strings.HasPrefix(value, `"`) {
			unquoted, err := strconv.Unquote(value)
			if err == nil {
				value = unquoted
			} else {
				value = strings.Trim(value, `"`)
			}
		}
		result[key] = value
	}

	directive, matched := p.catalog.MatchCommentDirective(comment)
	bare := withoutDirectiveName(keyValuePairRe.ReplaceAllString(comment, ""))
	for _, match := range bareWordRe.FindAllStringSubmatch(bare, -1) {
		word := match[1]
		if matched && p.takesValue(directive.Name, word) {
			return nil, &BareValueError{Directive: directive.Name, Attribute: word}
		}
		if _, exists := result[word]; !exists {
			result[word] = "true"
		}
	}
	return result, nil
}

// takesValue reports whether word names an attribute of directive that is
// written with a value: one the directive accepts, a platform override
// included, that is neither declared a bare boolean nor retired. A retired
// attribute is read as true for the reason an unknown one is: it has to reach
// validation to be refused with its reason.
func (p KeyValueParser) takesValue(directive, word string) bool {
	if !p.catalog.AllowsAttribute(directive, word) {
		return false
	}
	if _, retired := p.catalog.RetiredAttribute(directive, word); retired {
		return false
	}
	declared, ok := p.catalog.Attribute(directive, word)
	return !ok || !declared.Boolean
}

// withoutDirectiveName drops the directive's own name from a comment, so a word
// of the name is never read as a bare attribute. A directive of an owner may be
// named with any word, `index` included, which is also an attribute name.
func withoutDirectiveName(comment string) string {
	trimmed := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(comment), "//"))
	if !strings.HasPrefix(trimmed, "ptah:") {
		return comment
	}
	end := strings.IndexAny(trimmed, " \t")
	if end < 0 {
		return ""
	}
	return trimmed[end:]
}

// propertyShortcutDialects names the dialects whose descriptor reads the
// `engine` shortcut. A schema's every table and field directive asks, so the
// names are derived once per registry rather than from a copy of every
// descriptor on each call.
var propertyShortcutDialects = descriptortable.NewIndex(func(dialects []descriptor.Dialect) []string {
	var names []string
	for _, described := range dialects {
		if described.Conversion.PropertyShortcuts {
			names = append(names, described.Name)
		}
	}
	return names
})

// ParsePlatformSpecific returns a table or field directive's platform
// overrides: the `engine` shortcut, for every dialect whose descriptor reads
// it (descriptor.Conversion.PropertyShortcuts: MySQL and MariaDB), then every
// `platform.<dialect>.<key>` attribute, which takes precedence.
//
// A plain `comment` is the comment of the table or the column on every
// engine, which the directive's Comment carries, and no property of one. A
// MySQL-family plan writes that comment where no `platform.mysql.comment`
// states another, so copying it here would change nothing there, and an
// engine whose provider receives no platform properties would refuse the
// copy.
func ParsePlatformSpecific(kv map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string)
	if engine := kv["engine"]; engine != "" {
		for _, dialect := range propertyShortcutDialects.Get() {
			out[dialect] = map[string]string{"engine": engine}
		}
	}

	addPlatformAttributes(out, kv)
	return out
}

// ParsePlatformProperties returns only the `platform.<dialect>.<key>`
// attributes of a directive, grouped by dialect, and nil when it has none. A
// directive whose common attributes carry no platform meaning uses it, so an
// index's `comment` stays the index comment rather than becoming a MySQL
// property no owner claims.
func ParsePlatformProperties(kv map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string)
	addPlatformAttributes(out, kv)
	if len(out) == 0 {
		return nil
	}
	return out
}

func addPlatformAttributes(out map[string]map[string]string, kv map[string]string) {
	for k, v := range kv {
		if !annotationmeta.IsPlatformAttribute(k) {
			continue
		}
		parts := strings.SplitN(k, ".", 3)
		dialect := parts[1]
		key := parts[2]
		if out[dialect] == nil {
			out[dialect] = make(map[string]string)
		}
		out[dialect][key] = v
	}
}
