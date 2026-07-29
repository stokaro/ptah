package atlasmigrate

import (
	"fmt"
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/tableref"
)

// Qualifier is the typed model for Atlas's `migrate diff --qualifier` value.
//
// Atlas OSS semantics (pinned at ariga/atlas v1.2.0): when the dev connection
// is bound to a single schema, `--qualifier` sets a custom schema qualifier
// for every object named by the generated migration statements, so the file
// can be applied to a schema other than the one it was planned against. The
// plan must stay scoped to one schema; Atlas's sqlx.CheckChangesScope rejects
// plans that touch several schemas. An unset qualifier leaves Ptah's default
// naming untouched.
//
// The zero value means "no qualifier".
type Qualifier struct {
	name string
	// label is the flag spelling used in diagnostics. Empty keeps the
	// historical Atlas-compatible spelling, so the compat surface's error
	// text is unchanged; the native command tree overrides it with
	// WithErrorLabel.
	label string
}

// defaultQualifierLabel is the historical Atlas-compatible diagnostic prefix.
const defaultQualifierLabel = "atlas migrate diff --qualifier"

// WithErrorLabel returns a copy of the qualifier whose diagnostics name the
// given flag spelling (for example "--qualifier" on the native command tree).
func (q Qualifier) WithErrorLabel(label string) Qualifier {
	q.label = label
	return q
}

func (q Qualifier) errLabel() string {
	if q.label == "" {
		return defaultQualifierLabel
	}
	return q.label
}

// ParseQualifier validates and builds a Qualifier from the raw --qualifier
// flag value. An empty (or blank) value returns the zero Qualifier. Values
// that cannot be represented as a single plain schema identifier fail
// explicitly so the command aborts before any file or checksum is written.
func ParseQualifier(raw string) (Qualifier, error) {
	name := strings.TrimSpace(raw)
	if name == "" {
		return Qualifier{}, nil
	}
	for _, r := range name {
		switch {
		case r < 0x20 || r == 0x7f:
			return Qualifier{}, fmt.Errorf("invalid --qualifier %q: control characters are not allowed", raw)
		case strings.ContainsRune(".\"`[]", r):
			return Qualifier{}, fmt.Errorf("invalid --qualifier %q: character %q is not allowed in a schema qualifier", raw, r)
		}
	}
	return Qualifier{name: name}, nil
}

// IsZero reports whether the qualifier is unset.
func (q Qualifier) IsZero() bool {
	return q.name == ""
}

// Name returns the schema qualifier name, or "" when unset.
func (q Qualifier) Name() string {
	return q.name
}

// String implements fmt.Stringer.
func (q Qualifier) String() string {
	return q.name
}

// ValidateScope rejects qualifier use outside the supported single-schema
// planning scope before any migration state is touched. It mirrors Atlas's
// behavior of applying the qualifier only when working on a single schema.
func (q Qualifier) ValidateScope(dialect string, schemas []string) error {
	if q.IsZero() {
		return nil
	}
	if !qualifierSupportsDialect(dialect) {
		return fmt.Errorf("%s is not supported for dialect %q", q.errLabel(), dialect)
	}
	if len(schemas) > 1 {
		return fmt.Errorf("%s %q requires a single schema scope, got --schema %q", q.errLabel(), q.name, strings.Join(schemas, ","))
	}
	return nil
}

func qualifierSupportsDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

// ApplyToPlan rewrites the planned migration AST in place so that every object
// the plan touches is qualified with the custom schema qualifier, mirroring
// Atlas's PlanWithSchemaQualifier. Statement kinds Ptah cannot yet qualify
// safely fail explicitly, before any migration file or checksum is written.
// desired supplies the enum-type catalog used to refuse enum-typed columns,
// whose rendered type references cannot be re-qualified yet.
func (q Qualifier) ApplyToPlan(dialect string, desired *goschema.Database, nodes []ast.Node) error {
	if q.IsZero() {
		return nil
	}
	if !qualifierSupportsDialect(dialect) {
		return fmt.Errorf("%s is not supported for dialect %q", q.errLabel(), dialect)
	}
	state := &qualifyState{
		qualifier: q,
		enums:     enumTypeNames(dialect, desired),
		schemas:   map[string]struct{}{},
	}
	for _, node := range nodes {
		if err := state.rewriteNode(node); err != nil {
			return err
		}
	}
	return state.checkSingleSchemaScope()
}

// qualifyState carries the qualifier application over one planned node list.
type qualifyState struct {
	qualifier Qualifier
	enums     map[string]struct{}
	schemas   map[string]struct{}
}

func (s *qualifyState) rewriteNode(node ast.Node) error {
	switch n := node.(type) {
	case *ast.CommentNode:
		return nil // informational only; embedded names are not SQL
	case *ast.CreateTableNode:
		return s.rewriteCreateTable(n)
	case *ast.AlterTableNode:
		return s.rewriteAlterTable(n)
	case *ast.IndexNode:
		return s.rewriteStringName(&n.Table)
	case *ast.DropIndexNode:
		return s.rewriteDropIndex(n)
	case *ast.DropTableNode:
		return s.rewriteDropTable(n)
	default:
		return fmt.Errorf("%s %q does not support %T statements yet", s.qualifier.errLabel(), s.qualifier.name, node)
	}
}

func (s *qualifyState) rewriteCreateTable(node *ast.CreateTableNode) error {
	originalName := node.Name
	if err := s.rewriteStringName(&node.Name); err != nil {
		return err
	}
	for _, column := range node.Columns {
		if err := s.rewriteColumn(originalName, column); err != nil {
			return err
		}
	}
	for _, constraint := range node.Constraints {
		if err := s.rewriteConstraint(constraint); err != nil {
			return err
		}
	}
	return nil
}

func (s *qualifyState) rewriteAlterTable(node *ast.AlterTableNode) error {
	originalName := node.Name
	if err := s.rewriteStringName(&node.Name); err != nil {
		return err
	}
	for _, operation := range node.Operations {
		if err := s.rewriteAlterOperation(originalName, operation); err != nil {
			return err
		}
	}
	return nil
}

func (s *qualifyState) rewriteAlterOperation(tableName string, operation ast.AlterOperation) error {
	switch op := operation.(type) {
	case *ast.AddColumnOperation:
		return s.rewriteColumn(tableName, op.Column)
	case *ast.ModifyColumnOperation:
		return s.rewriteColumn(tableName, op.Column)
	case *ast.AddConstraintOperation:
		return s.rewriteConstraint(op.Constraint)
	case *ast.DropColumnOperation, *ast.DropConstraintOperation,
		*ast.RenameColumnOperation, *ast.AlterGeneratedColumnExpressionOperation:
		return nil // column- and constraint-name only; no table references
	default:
		return fmt.Errorf("%s %q does not support %T alter operations yet", s.qualifier.errLabel(), s.qualifier.name, operation)
	}
}

func (s *qualifyState) rewriteColumn(tableName string, column *ast.ColumnNode) error {
	if column == nil {
		return nil
	}
	if columnUsesEnumType(s.enums, column.Type) {
		return fmt.Errorf(
			"%s %q: table %q column %q uses enum type %q; qualifying enum type references is not supported yet",
			s.qualifier.errLabel(), s.qualifier.name, tableName, column.Name, column.Type)
	}
	if column.ForeignKey == nil {
		return nil
	}
	return s.rewriteStringName(&column.ForeignKey.Table)
}

func (s *qualifyState) rewriteConstraint(constraint *ast.ConstraintNode) error {
	if constraint == nil || constraint.Reference == nil {
		return nil
	}
	return s.rewriteStringName(&constraint.Reference.Table)
}

func (s *qualifyState) rewriteDropIndex(node *ast.DropIndexNode) error {
	if strings.TrimSpace(node.Table) == "" {
		return fmt.Errorf(
			"%s %q cannot qualify DROP INDEX %q without its owning table", s.qualifier.errLabel(), s.qualifier.name, node.Name)
	}
	return s.rewriteStringName(&node.Table)
}

func (s *qualifyState) rewriteDropTable(node *ast.DropTableNode) error {
	names := node.TableNames()
	for i := range names {
		if err := s.rewriteStringName(&names[i]); err != nil {
			return err
		}
	}
	node.SetNames(names)
	return nil
}

// rewriteStringName rewrites one possibly schema-qualified object name to the
// custom qualifier, recording any pre-existing schema for the scope check.
func (s *qualifyState) rewriteStringName(name *string) error {
	trimmed := strings.TrimSpace(*name)
	if trimmed == "" {
		return nil
	}
	ref, ok := tableref.Parse(trimmed)
	if !ok {
		return fmt.Errorf("%s %q cannot parse object name %q", s.qualifier.errLabel(), s.qualifier.name, *name)
	}
	if strings.ContainsAny(ref.Name, ".\"`[]") {
		return fmt.Errorf("%s %q does not support quoted object name %q yet", s.qualifier.errLabel(), s.qualifier.name, *name)
	}
	if ref.Qualified {
		s.schemas[ref.Schema] = struct{}{}
	}
	*name = s.qualifier.name + "." + ref.Name
	return nil
}

// checkSingleSchemaScope mirrors Atlas's sqlx.CheckChangesScope: a qualified
// plan must not span several schemas.
func (s *qualifyState) checkSingleSchemaScope() error {
	if len(s.schemas) <= 1 {
		return nil
	}
	names := make([]string, 0, len(s.schemas))
	for name := range s.schemas {
		names = append(names, name)
	}
	slices.Sort(names)
	return fmt.Errorf("found %d schemas when migration plan is scoped to one: %q", len(names), names)
}

// enumTypeNames collects the desired-state enum type names (bare, lowercased)
// for PostgreSQL-family dialects, where enum-typed columns reference a named
// type that the qualifier rewrite cannot reach yet.
func enumTypeNames(dialect string, desired *goschema.Database) map[string]struct{} {
	if desired == nil || !platform.IsPostgresFamily(dialect) {
		return nil
	}
	names := make(map[string]struct{}, len(desired.Enums))
	for _, enum := range desired.Enums {
		name := strings.ToLower(strings.TrimSpace(enum.Name))
		if name == "" {
			continue
		}
		names[name] = struct{}{}
		if ref, ok := tableref.Parse(name); ok {
			names[strings.ToLower(ref.Name)] = struct{}{}
		}
	}
	return names
}

func columnUsesEnumType(enums map[string]struct{}, columnType string) bool {
	if len(enums) == 0 {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(columnType))
	normalized = strings.TrimSuffix(normalized, "[]")
	normalized = strings.TrimSpace(normalized)
	if _, ok := enums[normalized]; ok {
		return true
	}
	ref, ok := tableref.Parse(normalized)
	if !ok {
		return false
	}
	_, ok = enums[strings.ToLower(ref.Name)]
	return ok
}
