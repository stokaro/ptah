// Package postgrescomparison holds the rules PostgreSQL gives the shared
// schema comparison ([schemacomparison.Rules]): how an index definition and a
// generated column's expression read on PostgreSQL. The built-in runtime
// registers them on its PostgreSQL target, so the shared comparison holds no
// PostgreSQL knowledge of its own.
package postgrescomparison

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/normalize"
)

// Rules returns PostgreSQL's comparison rules.
func Rules() schemacomparison.Rules {
	return schemacomparison.Rules{Indexes: Indexes{}, GeneratedExpressions: GeneratedExpressions{}}
}

// Indexes compares PostgreSQL index definitions.
//
// Registering the rules has the shared comparison compare the definition the
// reader reports: the uniqueness, each key's column or expression and its
// direction, the INCLUDE columns, and the options below. Measured on
// PostgreSQL 17.10 against the pinned community binary v1.3.0, each of these
// pairs made `schema diff` report "Schemas are synced" while comparing the
// name alone, where the pinned binary planned DROP INDEX and CREATE INDEX
// (stokaro/ptah#1272):
//
//	USING btree (value)        -> USING hash (value)
//	USING gin (tsv)            -> USING gist (tsv)
//	(value)                    -> (value text_pattern_ops)   and the reverse
//	(value)                    -> (value DESC)
//	(value)                    -> (value NULLS FIRST)
//	(value DESC)               -> (value DESC NULLS LAST)
//	(a) INCLUDE (b)            -> (a) INCLUDE (c), added, and removed
//	(a)                        -> (lower(a))                 and the reverse
//	(lower(a))                 -> (upper(a))
//	(value)                    -> UNIQUE (value)
//	(a)                        -> (b)
//
// PostgreSQL alters none of them in place: there is no ALTER INDEX form for
// the access method, the key list, an operator class, a key's sort or NULLS
// ordering, or the INCLUDE payload. A changed definition is therefore a DROP
// INDEX and a CREATE INDEX, which is the transition the pinned binary plans
// and the one the server accepts.
type Indexes struct{}

// OptionsEqual reports whether a PostgreSQL index has the access method, the
// storage parameters, and each key the operator class and NULLS ordering,
// declared for it.
func (Indexes) OptionsEqual(desired schemamodel.Index, current catalog.Index) bool {
	if accessMethod(desired.Type) != accessMethod(current.Method) ||
		storageParamsChanged(desired.StorageParams, current.StorageParams) {
		return false
	}
	desiredKeys := declaredKeys(desired)
	currentKeys := reportedKeys(current)
	if len(desiredKeys) != len(currentKeys) {
		// A different number of keys is the shared comparison's to report.
		return true
	}
	for position, key := range desiredKeys {
		if key != currentKeys[position] {
			return false
		}
	}
	return true
}

// RenameCarries reports that a PostgreSQL index carries no facet through a
// rename, so an index with a facet is dropped and built again rather than
// renamed.
func (Indexes) RenameCarries(schemaext.Kind) bool {
	return false
}

// defaultAccessMethod is the method PostgreSQL uses when CREATE INDEX carries
// no USING clause.
const defaultAccessMethod = "btree"

// accessMethod reduces an access method to a comparison key.
//
// The two sides spell it differently by construction: an annotation or HCL
// source writes BTREE/GIN, while the reader reports pg_am.amname verbatim,
// which PostgreSQL spells btree/gin. An absent method is the default one, so
// `USING btree (x)` and `(x)` are the same index and must not churn.
func accessMethod(method string) string {
	normalized := strings.ToLower(strings.TrimSpace(method))
	if normalized == "" {
		return defaultAccessMethod
	}
	return normalized
}

// storageParamsChanged compares the WITH (...) storage parameters.
//
// Only the parameters the reader records reach here, and it records only the
// ones every surface in the chain can carry back out again -- see
// postgresRoundTrippableIndexStorageParams in the PostgreSQL reader. That is
// what keeps this from churning: a parameter one surface drops would arrive
// on one side and not the other on every run, and the rebuild it planned would
// drop the parameter it was meant to protect.
//
// The values are compared as the two sides spell them, with case and space
// normalized. PostgreSQL reports pg_class.reloptions as `pages_per_range=32`
// and every writing surface in the chain spells integers the same way, so there
// is no unit or default resolution to do here -- an absent parameter means the
// server's own default, and both sides say so by omitting it.
func storageParamsChanged(desired, current map[string]string) bool {
	if len(desired) != len(current) {
		return true
	}
	for name, value := range desired {
		currentValue, ok := current[name]
		if !ok {
			return true
		}
		if !strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(currentValue)) {
			return true
		}
	}
	return false
}

// keyOptions is the operator class and the NULLS ordering of one key, in the
// form the two sides are compared in. It is comparable, so a key is one `!=`
// rather than a chain of comparisons a later option could be forgotten from.
type keyOptions struct {
	operator   string
	nullsFirst bool
}

// declaredKeys reduces the keys of a declared index. A key takes the operator
// class the renderer writes for it: its own, or the index's where it has none.
func declaredKeys(index schemamodel.Index) []keyOptions {
	parts := index.Parts
	if len(parts) == 0 {
		parts = make([]schemamodel.IndexPart, len(index.Fields))
		for position, field := range index.Fields {
			parts[position] = schemamodel.IndexPart{Name: field}
		}
	}
	keys := make([]keyOptions, len(parts))
	for position, part := range parts {
		keys[position] = keyOptions{
			operator:   operatorClass(part.Operator, index.Operator),
			nullsFirst: nullsFirst(part.NullsOrder, part.Desc),
		}
	}
	return keys
}

// reportedKeys reduces the keys of an index the server reports. The catalog
// has no index-level operator class, so every class it reports is per key, and
// a reader that reported columns alone reported no option for any of them.
func reportedKeys(index catalog.Index) []keyOptions {
	parts := index.Parts
	if len(parts) == 0 {
		parts = make([]catalog.IndexPart, len(index.Columns))
		for position, column := range index.Columns {
			parts[position] = catalog.IndexPart{Name: column}
		}
	}
	keys := make([]keyOptions, len(parts))
	for position, part := range parts {
		keys[position] = keyOptions{
			operator:   operatorClass(part.Operator, ""),
			nullsFirst: nullsFirst(part.NullsOrder, part.Desc),
		}
	}
	return keys
}

// operatorClass reduces an operator class to a comparison key.
//
// Only case and surrounding space are normalized. PostgreSQL reports an
// operator class in pg_index.indclass for every key, and the reader records
// only the ones that are not the key type's default (stokaro/ptah#1271), so an
// introspected index names a class exactly when the choice was not the default
// and both sides of an inspect/replay round trip agree.
//
// A hand-written source that spells the default out -- `ops = text_ops` on a
// text column -- is the one case where this diverges from the pinned community
// binary v1.3.0, which resolves type defaults and reported "Schemas are synced"
// for that fixture on PostgreSQL 17.10 where Ptah plans a rebuild. Ptah does
// not resolve default operator classes, and inventing the resolution from a
// version-pinned catalog table would be worse than the churn: the alternative
// is to ignore a named class, which would silently accept a real
// operator-class change. Neither binary's `schema inspect` emits a default
// class, so no round trip reaches this. Recorded in docs/conformance.md.
func operatorClass(partOperator, indexOperator string) string {
	operator := strings.TrimSpace(partOperator)
	if operator == "" {
		operator = strings.TrimSpace(indexOperator)
	}
	return strings.ToLower(operator)
}

// nullsFirst resolves a key's NULLS ordering to the one the server applies:
// whether NULLs come first.
//
// The default depends on the direction -- NULLS LAST for ASC, NULLS FIRST for
// DESC -- so an omitted ordering and an explicit spelling of that direction's
// default are one key. The reader records only an ordering against the
// default (stokaro/ptah#1271), so resolving both sides is what keeps `ASC` and
// `ASC NULLS LAST` from planning a rebuild against each other while `ASC` and
// `ASC NULLS FIRST` still report one.
func nullsFirst(order string, desc bool) bool {
	switch strings.ToUpper(strings.TrimSpace(order)) {
	case catalog.NullsOrderFirst:
		return true
	case catalog.NullsOrderLast:
		return false
	default:
		return desc
	}
}

// GeneratedExpressions reads the expression of a PostgreSQL generated column.
//
// PostgreSQL stores a parse tree and prints it back: with the casts its parser
// inserted, each name parenthesized or quoted as it chooses, and its own
// spacing. A declaration is written without them, so both sides are reduced
// to the expression with the casts removed and the catalog's spelling folded.
type GeneratedExpressions struct{}

// Spelling returns expression without its `::type` casts, folded the way a
// catalog reprints an expression ([normalize.CatalogExpression]). Only a
// single quote opens a string on PostgreSQL; a double-quoted token is a name.
func (GeneratedExpressions) Spelling(expression string) string {
	return normalize.CatalogExpression(stripTypeCasts(expression), "'")
}

// stripTypeCasts removes every `::type` cast outside quoted text, the type's
// own parenthesized arguments included.
func stripTypeCasts(expression string) string {
	var b strings.Builder
	inString := false
	inIdentifier := false
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if ch == '\'' && !inIdentifier {
			b.WriteByte(ch)
			if inString && i+1 < len(expression) && expression[i+1] == '\'' {
				i++
				b.WriteByte('\'')
				continue
			}
			inString = !inString
			continue
		}
		if ch == '"' && !inString {
			b.WriteByte(ch)
			if inIdentifier && i+1 < len(expression) && expression[i+1] == '"' {
				i++
				b.WriteByte('"')
				continue
			}
			inIdentifier = !inIdentifier
			continue
		}
		if !inString && !inIdentifier && i+1 < len(expression) && ch == ':' && expression[i+1] == ':' {
			i = skipTypeCast(expression, i)
			i--
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

// skipTypeCast returns the position after the cast that starts at
// expression[start], the `::` included.
func skipTypeCast(expression string, start int) int {
	i := start + 2
	for i < len(expression) && isTypeCastCharacter(expression[i]) {
		i++
	}
	if i >= len(expression) || expression[i] != '(' {
		return i
	}
	depth := 0
	for i < len(expression) {
		switch expression[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
		i++
	}
	return i
}

// isTypeCastCharacter reports whether ch can be part of the type name a cast
// names, a multi-word name such as `character varying` included.
func isTypeCastCharacter(ch byte) bool {
	return ch == ' ' || ch == '.' || ch == '_' ||
		(ch >= '0' && ch <= '9') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z')
}
