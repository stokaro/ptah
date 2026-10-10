package mysqlrender

import (
	"fmt"
	"strings"
	"sync"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/mysql/mysqlast"
	"ptah.run/internal/mysqlindex"
	"ptah.run/internal/tableref"
)

// QuoteIdentifier writes one identifier in backticks, doubling a backtick
// inside it. An identifier already quoted in backticks, double quotes or
// brackets is unquoted first, so a quoted spelling is not quoted twice.
func QuoteIdentifier(identifier string) string {
	return quote(unquote(identifier))
}

// QuoteQualifiedIdentifier writes a name that may be schema-qualified, such as
// `app.users`, quoting each part as [QuoteIdentifier] does. A name that does
// not parse as a table reference is quoted whole.
func QuoteQualifiedIdentifier(identifier string) string {
	ref, ok := tableref.Parse(identifier)
	switch {
	case !ok:
		return quote(identifier)
	case !ref.Qualified:
		return quote(ref.Name)
	default:
		return quote(ref.Schema) + "." + quote(ref.Name)
	}
}

// QuoteString writes a string literal in single quotes, doubling a single
// quote inside it.
func QuoteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// HiddenIndexWord is how dialect says whether the optimizer uses an index:
// MySQL's INVISIBLE and VISIBLE, MariaDB's IGNORED and NOT IGNORED. Each
// engine answers ERROR 1064 to the other's words.
func HiddenIndexWord(dialect string, invisible bool) string {
	if platform.NormalizeDialect(dialect) == platform.MariaDB {
		return map[bool]string{true: "IGNORED", false: "NOT IGNORED"}[invisible]
	}
	return map[bool]string{true: "INVISIBLE", false: "VISIBLE"}[invisible]
}

func quote(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func unquote(identifier string) string {
	if len(identifier) < 2 {
		return identifier
	}
	switch {
	case identifier[0] == '`' && identifier[len(identifier)-1] == '`':
		return strings.ReplaceAll(identifier[1:len(identifier)-1], "``", "`")
	case identifier[0] == '"' && identifier[len(identifier)-1] == '"':
		return strings.ReplaceAll(identifier[1:len(identifier)-1], `""`, `"`)
	case identifier[0] == '[' && identifier[len(identifier)-1] == ']':
		return strings.ReplaceAll(identifier[1:len(identifier)-1], "]]", "]")
	}
	return identifier
}

// IndexDefinition is index from its kind to its options, as CREATE INDEX and
// ALTER TABLE ... ADD INDEX both write it on dialect, one token group per
// element. placement is what stands between the name and the key parts: `ON
// table` for CREATE INDEX, and empty for ADD INDEX. It is the one writer of a
// MySQL-family index definition: the common renderer and the owner's index
// replacement both use it.
func IndexDefinition(dialect string, index mysqlast.Index, placement string) []string {
	var parts []string
	if index.Unique {
		parts = append(parts, "UNIQUE")
	}
	if prefix := mysqlindex.KindOf(index.Type).Prefix(); prefix != "" {
		parts = append(parts, prefix)
	}
	parts = append(parts, "INDEX", QuoteIdentifier(index.Name))
	if placement != "" {
		parts = append(parts, placement)
	}
	keys := make([]string, 0, len(index.Parts))
	for _, part := range index.Parts {
		keys = append(keys, keyPart(part))
	}
	columns := "(" + strings.Join(keys, ", ") + ")"
	if index.Parser != "" {
		columns += fmt.Sprintf(" /*!50100 WITH PARSER %s */", QuoteIdentifier(index.Parser))
	}
	parts = append(parts, columns)
	// After the column list, which is where both engines take it: measured
	// 2026-09-03, MySQL 8.4.11 and MariaDB 11.8.9 each accept
	// `CREATE INDEX k ON t (a) USING HASH`, the UNIQUE form of it, and the
	// spelling that puts the clause before ON. Emitting it is what keeps a
	// declared HASH from being dropped on the way out (stokaro/ptah#2825).
	if method := mysqlindex.Method(index.Type); method != "" {
		parts = append(parts, "USING", method)
	}
	// The hint, the comment and the visibility follow, as MySQL 8.4.11 and
	// MariaDB 11.8.9 print them in SHOW CREATE TABLE; both keep the comment
	// in STATISTICS.INDEX_COMMENT (stokaro/ptah#3853).
	if index.KeyBlockSize != 0 {
		parts = append(parts, fmt.Sprintf("KEY_BLOCK_SIZE=%d", index.KeyBlockSize))
	}
	if index.Comment != "" {
		parts = append(parts, "COMMENT", QuoteString(index.Comment))
	}
	if index.Invisible {
		parts = append(parts, HiddenIndexWord(dialect, true))
	}
	return parts
}

func keyPart(part mysqlast.IndexPart) string {
	if part.Expression != "" {
		spec := "(" + part.Expression + ")"
		if part.Descending {
			spec += " DESC"
		}
		return spec
	}
	spec := QuoteQualifiedIdentifier(part.Column)
	if part.Prefix != "" {
		spec += " (" + part.Prefix + ")"
	}
	if part.Descending {
		spec += " DESC"
	}
	return spec
}

// Registry returns the handlers of the owner's operations, built once. A
// registry holds nothing a render changes, so every render shares it.
var Registry = sync.OnceValues(func() (renderer.Extensions, error) {
	return renderer.NewExtensions(Handlers()...)
})

// Handlers returns the handler of [mysqlast.ReplaceIndex] in an ALTER TABLE:
// `ALTER TABLE t DROP INDEX k, ADD <definition>`, with ALGORITHM=COPY when the
// payload asks for it and the online clause the parent asks for otherwise.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&mysqlast.ReplaceIndex{}, ast.AlterExtension, validateReplaceIndex, renderReplaceIndex),
	}
}

func validateReplaceIndex(ctx renderer.ExtensionContext, op *mysqlast.ReplaceIndex) error {
	switch platform.NormalizeDialect(ctx.Target) {
	case platform.MySQL, platform.MariaDB:
	default:
		return fmt.Errorf("%w: MySQL index replacement on %q", ptaherr.ErrUnsupportedDialect, ctx.Target)
	}
	if op.TableCopy && !ctx.Capabilities.Has(capability.AlterTableAlgorithmLock) {
		return fmt.Errorf("%w: replacing index %q needs ALGORITHM=COPY to store its block size, which requires target capability %s",
			ptaherr.ErrUnsupportedFeature, op.Index.Name, capability.AlterTableAlgorithmLock)
	}
	if err := mysqlindex.ValidateBlockSize(ctx.Target, op.Index.KeyBlockSize); err != nil {
		return fmt.Errorf("%w: index %q: %w", ptaherr.ErrInvalidSchemaDiff, op.Index.Name, err)
	}
	return op.Validate()
}

func renderReplaceIndex(ctx renderer.ExtensionContext, op *mysqlast.ReplaceIndex) ([]string, error) {
	if ctx.Parent == nil {
		return nil, fmt.Errorf("%w: MySQL index replacement %q has no ALTER TABLE parent", ptaherr.ErrInvalidSchemaDiff, op.Index.Name)
	}
	algorithm := ctx.Parent.Algorithm
	if op.TableCopy {
		algorithm = "COPY"
	}
	statement := fmt.Sprintf("ALTER TABLE %s DROP INDEX %s, ADD %s", QuoteQualifiedIdentifier(ctx.Parent.Name),
		QuoteIdentifier(op.Index.Name), strings.Join(IndexDefinition(ctx.Target, op.Index, ""), " "))
	if algorithm != "" && ctx.Capabilities.Has(capability.AlterTableAlgorithmLock) {
		statement += ", ALGORITHM=" + strings.ToUpper(algorithm)
	}
	if ctx.Parent.Lock != "" && ctx.Capabilities.Has(capability.AlterTableAlgorithmLock) {
		statement += ", LOCK=" + strings.ToUpper(ctx.Parent.Lock)
	}
	return []string{statement + ";"}, nil
}
