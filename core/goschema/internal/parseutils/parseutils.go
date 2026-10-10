// Package parseutils implements the low-level annotation text parsing shared by
// goschema: splitting key="value" comment options into maps and extracting
// platform-specific override groups.
package parseutils

import (
	"maps"
	"regexp"
	"strconv"
	"strings"

	"ptah.run/internal/annotationmeta"
)

var keyValuePairRe = regexp.MustCompile(`(\w+(?:\.\w+)*)=("(?:\\.|[^"\\])*"|[^\s]+)`)
var boolRe = regexp.MustCompile(`\b(\w+(?:\.\w+)*)\b`)

// KeyValueParser splits annotation comment options into maps. It reads the
// directives of one catalog: which barewords are directive tokens, which are
// boolean attributes, and which attributes a directive declares or retires.
type KeyValueParser struct {
	catalog annotationmeta.Catalog
	// directiveTokens is the set of bareword tokens that appear in a
	// `//ptah:schema:<kind>` annotation header. They are never user-supplied
	// boolean attributes, so they are never auto-promoted to
	// `kv[token]="true"`.
	directiveTokens map[string]bool
	// indexDirectiveSkip is directiveTokens plus the bareword "index", used
	// when the comment line is a //ptah:schema:index header so the directive
	// token "index" isn't auto-promoted to kv["index"]="true".
	indexDirectiveSkip map[string]bool
	// booleanAttrs is the set of bareword keys that, when written without
	// `=`, are auto-promoted to `kv[name]="true"`.
	booleanAttrs map[string]bool
}

// NewKeyValueParser returns a parser for the directives of catalog.
func NewKeyValueParser(catalog annotationmeta.Catalog) KeyValueParser {
	tokens := catalog.DirectiveTokens()
	tokens["embed"] = true
	delete(tokens, "index")
	skip := make(map[string]bool, len(tokens)+1)
	maps.Copy(skip, tokens)
	skip["index"] = true
	return KeyValueParser{catalog: catalog, directiveTokens: tokens, indexDirectiveSkip: skip, booleanAttrs: catalog.BooleanAttributes()}
}

// ParseKeyValueComment splits comment into its attributes. A quoted value is
// unquoted. A bareword is promoted to "true" when it is a boolean attribute,
// one the directive does not declare, or one the directive retired, so that
// validation sees and refuses the last two.
func (p KeyValueParser) ParseKeyValueComment(comment string) map[string]string {
	result := make(map[string]string)

	// First, handle key=value pairs (quoted and unquoted)
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

	// Build the set of barewords to skip for this specific directive line.
	// All directive tokens are always skipped; "index" is additionally
	// skipped when this line IS the //ptah:schema:index header, because
	// otherwise the directive token itself would be auto-promoted to
	// kv["index"]="true" and trip the strict-unknown-key validator.
	skip := p.directiveTokens
	if isIndexDirectiveHeader(comment) {
		skip = p.indexDirectiveSkip
	}

	// Then, handle standalone boolean attributes (no =value)
	cleanComment := keyValuePairRe.ReplaceAllString(comment, "")
	for _, match := range boolRe.FindAllStringSubmatch(cleanComment, -1) {
		attr := match[1]
		if skip[attr] {
			continue
		}
		// A retired attribute is promoted for the same reason an unknown one
		// is: it has to reach validation to be refused. Without this it is in
		// no map at all -- never validated, never refused, dropped without a
		// word -- which is exactly the silence retiring it was meant to end
		// (stokaro/ptah#1625).
		if !p.isAutoPromotedBoolean(attr, skip) &&
			!p.isUnknownAttribute(comment, attr) &&
			!p.isRetiredAttribute(comment, attr) {
			continue
		}
		// Only set if not already set by key=value parsing
		if _, exists := result[attr]; !exists {
			result[attr] = "true"
		}
	}

	return result
}

// isRetiredAttribute reports whether the directive recognizes this attribute
// and refuses it.
func (p KeyValueParser) isRetiredAttribute(comment, attr string) bool {
	directive, ok := p.catalog.MatchCommentDirective(comment)
	if !ok {
		return false
	}
	_, retired := p.catalog.RetiredAttribute(directive.Name, attr)
	return retired
}

func (p KeyValueParser) isUnknownAttribute(comment, attr string) bool {
	directive, ok := p.catalog.MatchCommentDirective(comment)
	return ok && !p.catalog.AllowsAttribute(directive.Name, attr)
}

// isIndexDirectiveHeader reports whether `comment` is the
// //ptah:schema:index directive header (as opposed to e.g. a field line
// that happens to contain that substring). The check tolerates leading
// whitespace inside the comment but anchors on the `//ptah:schema:index`
// prefix followed by either end-of-string or a space — so substrings like
// `//ptah:schema:indexed` (hypothetical) would not match.
func isIndexDirectiveHeader(comment string) bool {
	c := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(comment), "//"))
	const prefix = "ptah:schema:index"
	if !strings.HasPrefix(c, prefix) {
		return false
	}
	rest := c[len(prefix):]
	return rest == "" || rest[0] == ' ' || rest[0] == '\t'
}

// isAutoPromotedBoolean reports whether a bareword `attr` (a word appearing
// in a directive line without an `=value`) should be promoted to
// `kv[attr]="true"`. Tokens in `skip` are excluded; everything else has to
// be a known boolean attribute or follow a naming convention.
func (p KeyValueParser) isAutoPromotedBoolean(attr string, skip map[string]bool) bool {
	if skip[attr] {
		return false
	}
	return p.booleanAttrs[attr] ||
		strings.HasSuffix(attr, "_null") ||
		strings.HasPrefix(attr, "is_") ||
		strings.HasPrefix(attr, "has_")
}

// ParsePlatformSpecific returns a table or field directive's platform
// overrides: the MySQL-family `engine` and `comment` shortcuts, then every
// `platform.<dialect>.<key>` attribute, which takes precedence.
func ParsePlatformSpecific(kv map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string)
	for _, dialect := range []string{"mysql", "mariadb"} {
		if engine := kv["engine"]; engine != "" {
			out[dialect] = map[string]string{"engine": engine}
		}
		if comment := kv["comment"]; comment != "" {
			if out[dialect] == nil {
				out[dialect] = make(map[string]string)
			}
			out[dialect]["comment"] = comment
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
