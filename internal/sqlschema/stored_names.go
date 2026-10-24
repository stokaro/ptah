package sqlschema

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/ast"
)

// ErrStoredNameCollision is the refusal of a file in which two different names
// are stored as one, in a namespace where the server refuses the second.
var ErrStoredNameCollision = errors.New("two names the file writes are stored as one")

// StoredName is a name a statement wrote longer than the identifier limit of a
// server that cuts it, and the name that server keeps.
//
// The reader records objects under Stored, as the server does, so render,
// compare and planning all use the name the database will hold. Written is
// kept so a notice or a refusal can say what the file said.
type StoredName struct {
	// Kind is what the name names: "table", "column", "index", "view",
	// "materialized view", "sequence", or a constraint kind such as
	// "foreign key".
	Kind string
	// Scope is where the name has to be unique, such as `schema "public"` or
	// `table "public.orders"`.
	Scope string
	// Written is the name as the statement wrote it, case folded as the
	// server folds it.
	Written string
	// Stored is the name the server keeps.
	Stored string
	// Limit is the number of bytes the server keeps.
	Limit int
}

// String is the notice for one cut name.
func (n StoredName) String() string {
	return fmt.Sprintf("%s %q in %s is %d bytes; the server keeps its first %d: %q",
		n.Kind, n.Written, n.Scope, len(n.Written), len(n.Stored), n.Stored)
}

// storedNames follows, statement by statement, the names a document declares
// in each namespace of a server that cuts long names, so a name cut to one
// already in use is found before the conversion records both under it.
//
// PostgreSQL keeps these namespaces apart, measured on 15.19:
//
//   - tables, views, materialized views, sequences and indexes share one per
//     schema, and so does the index a PRIMARY KEY, UNIQUE or EXCLUDE
//     constraint builds, under the constraint's name;
//   - constraint names are unique per table, so two tables may each have a
//     foreign key of the same name;
//   - column names are unique per table.
//
// Only a collision that cutting causes is decided here: two different written
// names stored as one. Two declarations that write the same name are the rest
// of the reader's business, as they were before names were cut.
type storedNames struct {
	sourcePlatform string
	defaultSchema  string
	spaces         map[string]map[string]storedEntry
	cut            []StoredName
}

type storedEntry struct {
	kind    string
	written string
}

const (
	relationSpace   = "relation"
	constraintSpace = "constraint"
	columnSpace     = "column"
)

func newStoredNames(sourcePlatform string) *storedNames {
	return &storedNames{
		sourcePlatform: sourcePlatform,
		defaultSchema:  factsOf(sourcePlatform).defaultSchema,
		spaces:         make(map[string]map[string]storedEntry),
	}
}

// cutsNames reports whether the source dialect's server cuts long names, which
// is the only case in which two different names can be stored as one.
func cutsNames(sourcePlatform string) bool {
	limit := factsOf(sourcePlatform).identifierLimit
	return limit.Truncates && limit.Max > 0
}

// relation names a table-like object: its schema, as the server stores it with
// the default filled in, and its name as written and as stored.
type relation struct {
	schema  string
	written string
	stored  string
}

func (s *storedNames) relation(raw string) relation {
	parts := splitSQLIdentifier(strings.TrimSpace(raw))
	for i := range parts {
		parts[i] = foldedIdentifierPart(s.sourcePlatform, parts[i])
	}
	written := parts[len(parts)-1]
	schema := strings.Join(parts[:len(parts)-1], ".")
	if schema == "" {
		schema = s.defaultSchema
	}
	return relation{schema: storedIdentifier(s.sourcePlatform, schema), written: written, stored: storedIdentifier(s.sourcePlatform, written)}
}

func (r relation) tableKey() string { return r.schema + "." + r.stored }

func spaceKey(space, owner string) string { return space + ":" + owner }

func scopeLabel(space, owner string) string {
	if space == relationSpace {
		return fmt.Sprintf("schema %q", owner)
	}
	return fmt.Sprintf("table %q", owner)
}

// onExisting is what a statement does when its name is already stored.
type onExisting uint8

const (
	// refuseExisting is a statement the server refuses on a name in use.
	refuseExisting onExisting = iota
	// skipExisting is a statement guarded by IF NOT EXISTS, or a CREATE OR
	// REPLACE, which the server skips or applies to the object it finds.
	skipExisting
)

// existingRule is the rule of a statement whose IF NOT EXISTS, or OR REPLACE,
// is the key.
var existingRule = map[bool]onExisting{false: refuseExisting, true: skipExisting}

// declare records one name. existed reports that the namespace already held
// a name stored the same way.
func (s *storedNames) declare(space, owner, kind, raw string, rule onExisting) (existed bool, err error) {
	written := strings.TrimSpace(raw)
	if written == "" {
		return false, nil
	}
	written = foldedIdentifierPart(s.sourcePlatform, written)
	stored := storedIdentifier(s.sourcePlatform, written)
	if written != stored {
		s.cut = append(s.cut, StoredName{
			Kind: kind, Scope: scopeLabel(space, owner), Written: written, Stored: stored,
			Limit: factsOf(s.sourcePlatform).identifierLimit.Max,
		})
	}
	key := spaceKey(space, owner)
	names := s.spaces[key]
	if names == nil {
		names = make(map[string]storedEntry)
		s.spaces[key] = names
	}
	previous, found := names[stored]
	if !found {
		names[stored] = storedEntry{kind: kind, written: written}
		return false, nil
	}
	if previous.written == written || rule == skipExisting {
		return true, nil
	}
	return true, fmt.Errorf("%w: %s %q and %s %q in %s are both stored as %q, and the server refuses the second",
		ErrStoredNameCollision, previous.kind, previous.written, kind, written, scopeLabel(space, owner), stored)
}

func (s *storedNames) drop(space, owner, raw string) {
	name := foldedIdentifierPart(s.sourcePlatform, strings.TrimSpace(raw))
	delete(s.spaces[spaceKey(space, owner)], storedIdentifier(s.sourcePlatform, name))
}

func (s *storedNames) rename(space, owner, kind, from, to string) error {
	s.drop(space, owner, from)
	_, err := s.declare(space, owner, kind, to, refuseExisting)
	return err
}

// dropTable forgets a table and everything declared on it.
func (s *storedNames) dropTable(table relation) {
	delete(s.spaces[spaceKey(relationSpace, table.schema)], table.stored)
	delete(s.spaces, spaceKey(constraintSpace, table.tableKey()))
	delete(s.spaces, spaceKey(columnSpace, table.tableKey()))
}

// follow records the names one statement declares, drops or renames.
func (s *storedNames) follow(statement ast.Node) error {
	switch node := statement.(type) {
	case *ast.CreateTableNode:
		return s.createTable(node)
	case *ast.IndexNode:
		return s.index(node)
	case *ast.AlterTableNode:
		return s.alterTable(node)
	case *ast.CreateViewNode:
		return s.declareRelation("view", node.Name, existingRule[node.Replace])
	case *ast.CreateMaterializedViewNode:
		return s.declareRelation("materialized view", node.Name, refuseExisting)
	case *ast.CreateSequenceNode:
		name := node.Name
		if node.Schema != "" {
			name = node.Schema + "." + node.Name
		}
		return s.declareRelation("sequence", name, existingRule[node.IfNotExists])
	case *ast.DropTableNode:
		for _, name := range append([]string{node.Name}, node.Names...) {
			if strings.TrimSpace(name) != "" {
				s.dropTable(s.relation(name))
			}
		}
	case *ast.AlterIndexNode:
		if strings.TrimSpace(node.NewName) == "" {
			return nil
		}
		index := s.relation(node.Name)
		return s.rename(relationSpace, index.schema, "index", index.written, node.NewName)
	case *ast.DropIndexNode:
		s.dropRelation(node.Name)
	case *ast.DropViewNode:
		s.dropRelation(node.Name)
	case *ast.DropMaterializedViewNode:
		s.dropRelation(node.Name)
	case *ast.DropSequenceNode:
		name := node.Name
		if node.Schema != "" {
			name = node.Schema + "." + node.Name
		}
		s.dropRelation(name)
	}
	return nil
}

func (s *storedNames) declareRelation(kind, raw string, rule onExisting) error {
	name := s.relation(raw)
	_, err := s.declare(relationSpace, name.schema, kind, name.written, rule)
	return err
}

func (s *storedNames) dropRelation(raw string) {
	name := s.relation(raw)
	s.drop(relationSpace, name.schema, name.written)
}

func (s *storedNames) createTable(node *ast.CreateTableNode) error {
	table := s.relation(node.Name)
	existed, err := s.declare(relationSpace, table.schema, "table", table.written, existingRule[node.IfNotExists])
	if err != nil || existed {
		// A table declared again declares nothing new on it: the server either
		// skipped the statement or refused it, and the reader decides which.
		return err
	}
	for _, column := range node.Columns {
		if err := s.column(table, column, refuseExisting); err != nil {
			return err
		}
	}
	for _, constraint := range node.Constraints {
		if err := s.constraint(table, constraint); err != nil {
			return err
		}
	}
	for _, owned := range node.OwnedConstraints {
		if err := s.indexConstraint(table, "constraint", owned.Name); err != nil {
			return err
		}
	}
	for _, index := range node.Indexes {
		if _, err := s.declare(relationSpace, table.schema, "index", index.Name, existingRule[index.IfNotExists]); err != nil {
			return err
		}
	}
	return nil
}

func (s *storedNames) column(table relation, column *ast.ColumnNode, rule onExisting) error {
	existed, err := s.declare(columnSpace, table.tableKey(), "column", column.Name, rule)
	if err != nil || existed {
		return err
	}
	owner := table.tableKey()
	if _, err := s.declare(constraintSpace, owner, "check", column.CheckName, refuseExisting); err != nil {
		return err
	}
	if _, err := s.declare(constraintSpace, owner, "not-null constraint", column.NotNullConstraintName, refuseExisting); err != nil {
		return err
	}
	if column.ForeignKey != nil {
		if _, err := s.declare(constraintSpace, owner, "foreign key", column.ForeignKey.Name, refuseExisting); err != nil {
			return err
		}
	}
	return nil
}

func constraintKind(kind ast.ConstraintType) string {
	switch kind {
	case ast.PrimaryKeyConstraint:
		return "primary key"
	case ast.UniqueConstraint:
		return "unique constraint"
	case ast.ForeignKeyConstraint:
		return "foreign key"
	case ast.CheckConstraint:
		return "check"
	default:
		return "constraint"
	}
}

func (s *storedNames) constraint(table relation, constraint *ast.ConstraintNode) error {
	if constraint == nil {
		return nil
	}
	kind := constraintKind(constraint.Type)
	if constraint.Type == ast.PrimaryKeyConstraint || constraint.Type == ast.UniqueConstraint {
		return s.indexConstraint(table, kind, constraint.Name)
	}
	_, err := s.declare(constraintSpace, table.tableKey(), kind, constraint.Name, refuseExisting)
	return err
}

// indexConstraint records a constraint that builds an index of its own name,
// which lives in the schema's relation namespace as well as on the table.
func (s *storedNames) indexConstraint(table relation, kind, name string) error {
	if _, err := s.declare(constraintSpace, table.tableKey(), kind, name, refuseExisting); err != nil {
		return err
	}
	_, err := s.declare(relationSpace, table.schema, "index of "+kind, name, refuseExisting)
	return err
}

func (s *storedNames) index(node *ast.IndexNode) error {
	if strings.TrimSpace(node.Name) == "" {
		return nil
	}
	// An index lives in the schema of the table it is on.
	table := s.relation(node.Table)
	_, err := s.declare(relationSpace, table.schema, "index", node.Name, existingRule[node.IfNotExists])
	return err
}

func (s *storedNames) alterTable(node *ast.AlterTableNode) error {
	table := s.relation(node.Name)
	for _, operation := range node.Operations {
		if err := s.alterOperation(&table, operation); err != nil {
			return err
		}
	}
	return nil
}

func (s *storedNames) alterOperation(table *relation, operation ast.AlterOperation) error {
	owner := table.tableKey()
	switch op := operation.(type) {
	case *ast.AddColumnOperation:
		return s.column(*table, op.Column, existingRule[op.IfNotExists])
	case *ast.AddConstraintOperation:
		return s.constraint(*table, op.Constraint)
	case *ast.AddIndexOperation:
		if op.Index != nil && strings.TrimSpace(op.Index.Name) != "" {
			_, err := s.declare(relationSpace, table.schema, "index", op.Index.Name, existingRule[op.Index.IfNotExists])
			return err
		}
	case *ast.DropConstraintOperation:
		s.drop(constraintSpace, owner, op.ConstraintName)
		if !op.ForeignKey && !op.Check {
			s.drop(relationSpace, table.schema, op.ConstraintName)
		}
	case *ast.DropColumnOperation:
		s.drop(columnSpace, owner, op.ColumnName)
	case *ast.RenameColumnOperation:
		return s.rename(columnSpace, owner, "column", op.OldName, op.NewName)
	case *ast.RenameConstraintOperation:
		return s.rename(constraintSpace, owner, "constraint", op.From, op.To)
	case *ast.RenameIndexOperation:
		return s.rename(relationSpace, table.schema, "index", op.From, op.To)
	case *ast.RenameTableOperation:
		return s.renameTable(table, op.NewName)
	}
	return nil
}

func (s *storedNames) renameTable(table *relation, newName string) error {
	renamed := s.relation(newName)
	renamed.schema = table.schema
	if err := s.rename(relationSpace, table.schema, "table", table.written, renamed.written); err != nil {
		return err
	}
	for _, space := range []string{constraintSpace, columnSpace} {
		if names, ok := s.spaces[spaceKey(space, table.tableKey())]; ok {
			delete(s.spaces, spaceKey(space, table.tableKey()))
			s.spaces[spaceKey(space, renamed.tableKey())] = names
		}
	}
	*table = renamed
	return nil
}

// followStoredNames checks the names statements declare against the names the
// document already holds, on a dialect whose server cuts long names, and
// reports each name it cuts to notice. It runs before the conversion, which
// records only the stored names and could not tell a collision from a
// redeclaration.
func (d *Document) followStoredNames(statements *ast.StatementList, dialect string, notice func(StoredName)) error {
	if statements == nil || !cutsNames(dialect) {
		return nil
	}
	if d.names == nil || d.names.sourcePlatform != dialect {
		d.names = newStoredNames(dialect)
	}
	reported := len(d.names.cut)
	for _, statement := range statements.Statements {
		if err := d.names.follow(statement); err != nil {
			return err
		}
	}
	if notice != nil {
		for _, name := range d.names.cut[reported:] {
			notice(name)
		}
	}
	return nil
}
