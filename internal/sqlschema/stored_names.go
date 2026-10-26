package sqlschema

import (
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgname"
	"ptah.run/internal/routineargs"
	"ptah.run/internal/rundiag"
)

// ErrStoredNameCollision is the refusal of a file in which two different names
// are stored as one, in a namespace where the server refuses the second.
var ErrStoredNameCollision = errors.New("two names the file writes are stored as one")

// ErrNameInUse is the refusal of a file that declares a name already in use in
// a namespace where the server refuses the second declaration, such as an
// index named like a table, or a table created twice without IF NOT EXISTS.
var ErrNameInUse = errors.New("a name the file declares is already in use")

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

// StoredNameRecord is the run diagnostic for name, read from source: keyed by
// the object, so the same name read twice in one run counts once, and carrying
// the written and the stored name for the debug log.
func StoredNameRecord(source string, name StoredName) rundiag.Record {
	return rundiag.Record{
		Key:  name.Kind + "\x00" + name.Scope + "\x00" + name.Written,
		Line: source + ": " + name.String(),
		Attrs: []slog.Attr{
			slog.String("source", source), slog.String("kind", name.Kind), slog.String("scope", name.Scope),
			slog.String("written", name.Written), slog.String("stored", name.Stored), slog.Int("limit", name.Limit),
		},
	}
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
// A collision that cutting causes, two different written names stored as one,
// is refused at once. A name written twice is held until the conversion has
// run: the reader refuses some of those itself, in words of its own, such as
// two constraints of one CREATE TABLE, and the rest it would merge into one
// object, where the server refuses the second statement (stokaro/ptah#4914).
type storedNames struct {
	sourcePlatform string
	defaultSchema  string
	spaces         map[string]map[string]storedEntry
	cut            []StoredName
	// duplicate is the first name the current file declares again without
	// IF NOT EXISTS, refused once its conversion has run.
	duplicate error
	// file numbers the file being read, counted from one, so a name an
	// earlier file of a schema directory declared is told from this file's.
	file int
	// skipped are the statements the server skips: a CREATE ... IF NOT EXISTS
	// whose name its namespace already holds, measured on PostgreSQL 15.19.
	// The conversion skips exactly these; see [Document.skips].
	skipped map[ast.Node]bool
	// derived holds the names the statement being followed leaves to the
	// server, claimed once its written names are: the server derives a name
	// past every name the statement writes, wherever in it that name stands.
	derived []func()
}

type storedEntry struct {
	kind    string
	written string
	// file numbers the file of a schema directory that declared the name.
	file int
}

const (
	relationSpace   = "relation"
	constraintSpace = "constraint"
	columnSpace     = "column"
	// routineSpace holds functions and procedures, unique per schema and
	// argument types, as pg_proc keeps them.
	routineSpace = "routine"
	// triggerSpace holds triggers, unique per table.
	triggerSpace = "trigger"
)

func newStoredNames(sourcePlatform string) *storedNames {
	return &storedNames{
		sourcePlatform: sourcePlatform,
		defaultSchema:  factsOf(sourcePlatform).defaultSchema,
		spaces:         make(map[string]map[string]storedEntry),
		skipped:        make(map[ast.Node]bool),
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
	switch space {
	case relationSpace:
		return fmt.Sprintf("schema %q", owner)
	case routineSpace:
		schema, arguments, _ := strings.Cut(owner, "\x00")
		return fmt.Sprintf("schema %q with arguments (%s)", schema, arguments)
	default:
		return fmt.Sprintf("table %q", owner)
	}
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
	if strings.TrimSpace(raw) == "" {
		return false, nil
	}
	return s.declareName(space, owner, kind, foldedIdentifierPart(s.sourcePlatform, strings.TrimSpace(raw)), rule)
}

// declareName is [storedNames.declare] for a name already read the way the
// server reads it, as [storedNames.relation] reads one. A cut name is recorded
// for the declaration that takes it: one that finds its name taken, as a
// statement the server skips does, declares no object of its own.
func (s *storedNames) declareName(space, owner, kind, written string, rule onExisting) (existed bool, err error) {
	existed, err = s.claimName(space, owner, kind, written, rule)
	if stored := storedIdentifier(s.sourcePlatform, written); err == nil && !existed && written != stored {
		s.cut = append(s.cut, StoredName{
			Kind: kind, Scope: scopeLabel(space, owner), Written: written, Stored: stored,
			Limit: factsOf(s.sourcePlatform).identifierLimit.Max,
		})
	}
	return existed, err
}

// claim takes a name in a second namespace for an object [storedNames.declare]
// has already recorded, such as the index a PRIMARY KEY or UNIQUE constraint
// builds under the constraint's name. The statement wrote the name once, so a
// cut is reported once, for the declaration.
func (s *storedNames) claim(space, owner, kind, raw string, rule onExisting) (existed bool, err error) {
	if strings.TrimSpace(raw) == "" {
		return false, nil
	}
	return s.claimName(space, owner, kind, foldedIdentifierPart(s.sourcePlatform, strings.TrimSpace(raw)), rule)
}

// claimName enters written in a namespace and refuses a second declaration
// the server refuses. Two names written differently and stored as one are
// refused under IF NOT EXISTS and OR REPLACE too, where the server takes the
// second for the first and skips or replaces it: the file names two objects,
// and the server keeps one of them.
func (s *storedNames) claimName(space, owner, kind, written string, rule onExisting) (existed bool, err error) {
	stored := storedIdentifier(s.sourcePlatform, written)
	key := spaceKey(space, owner)
	names := s.spaces[key]
	if names == nil {
		names = make(map[string]storedEntry)
		s.spaces[key] = names
	}
	previous, found := names[stored]
	if !found {
		names[stored] = storedEntry{kind: kind, written: written, file: s.file}
		return false, nil
	}
	// A name written as the bytes the server keeps of an earlier name is that
	// earlier object, as the server reads it; any other second spelling stored
	// the same way is a second object.
	if previous.written != written && written != stored {
		return true, storedNameCollision(previous, kind, written, scopeLabel(space, owner), stored, rule)
	}
	if rule == skipExisting {
		return true, nil
	}
	// A schema directory refuses an object of one kind that a later file
	// declares again, in words of its own; a name taken across kinds, or within
	// one file, is this check's.
	if previous.file != s.file && previous.kind == kind {
		return true, nil
	}
	if s.duplicate == nil {
		s.duplicate = fmt.Errorf("%w: %s %q in %s takes the name of %s %q, and the server refuses the second declaration",
			ErrNameInUse, kind, written, scopeLabel(space, owner), previous.kind, previous.written)
	}
	return true, nil
}

// claimDerived enters a name the server derives for an object the statement
// leaves unnamed, such as a serial column's sequence or an unnamed primary
// key's index. The name was chosen free in its namespace, as the server
// chooses it, and records no cut: the file wrote no name.
func (s *storedNames) claimDerived(space, owner, kind, name string) {
	key := spaceKey(space, owner)
	if s.spaces[key] == nil {
		s.spaces[key] = make(map[string]storedEntry)
	}
	if _, found := s.spaces[key][name]; !found {
		s.spaces[key][name] = storedEntry{kind: kind, written: name, file: s.file}
	}
}

// taken reports whether a namespace holds a name, for choosing a derived one.
func (s *storedNames) taken(space, owner string) func(string) bool {
	return func(name string) bool {
		_, found := s.spaces[spaceKey(space, owner)][name]
		return found
	}
}

// keyNameTaken reports whether the derived name of a constraint that builds an
// index is taken in schema: by a relation, or by a constraint of any table of
// the schema, which is where the server looks when it picks one.
func (s *storedNames) keyNameTaken(schema string) func(string) bool {
	prefix := spaceKey(constraintSpace, schema+".")
	return func(name string) bool {
		if s.taken(relationSpace, schema)(name) {
			return true
		}
		for key, names := range s.spaces {
			if _, found := names[name]; found && strings.HasPrefix(key, prefix) {
				return true
			}
		}
		return false
	}
}

// claimDerivedNames claims the names the statement followed last leaves to the
// server, in the order it writes the objects.
func (s *storedNames) claimDerivedNames() {
	for _, claim := range s.derived {
		claim()
	}
	s.derived = nil
}

// storedNameCollision is the refusal of written, which the server stores as
// the name an earlier declaration took. What the server does with the second
// statement depends on its guard, and the refusal says which.
func storedNameCollision(previous storedEntry, kind, written, scope, stored string, rule onExisting) error {
	outcome := "the server refuses the second"
	if rule == skipExisting {
		outcome = "the server keeps only one of them"
	}
	return fmt.Errorf("%w: %s %q and %s %q in %s are both stored as %q, and %s",
		ErrStoredNameCollision, previous.kind, previous.written, kind, written, scope, stored, outcome)
}

func (s *storedNames) drop(space, owner, raw string) {
	s.dropName(space, owner, foldedIdentifierPart(s.sourcePlatform, strings.TrimSpace(raw)))
}

// dropName is [storedNames.drop] for a name already read.
func (s *storedNames) dropName(space, owner, written string) {
	delete(s.spaces[spaceKey(space, owner)], storedIdentifier(s.sourcePlatform, written))
}

func (s *storedNames) rename(space, owner, kind, from, to string) error {
	s.drop(space, owner, from)
	_, err := s.declare(space, owner, kind, to, refuseExisting)
	return err
}

// renameName is [storedNames.rename] from a name already read.
func (s *storedNames) renameName(space, owner, kind, from, to string) error {
	s.dropName(space, owner, from)
	_, err := s.declare(space, owner, kind, to, refuseExisting)
	return err
}

// repeatsIndexConstraint reports whether a PRIMARY KEY or UNIQUE constraint is
// written, name and definition alike, by one that comes before it in the same
// CREATE TABLE. The server keeps one of the two, measured on PostgreSQL 15.19;
// it does that only for the constraints that build an index, and it refuses
// two definitions under one name.
func repeatsIndexConstraint(before []*ast.ConstraintNode, constraint *ast.ConstraintNode) bool {
	if constraint == nil || (constraint.Type != ast.PrimaryKeyConstraint && constraint.Type != ast.UniqueConstraint) {
		return false
	}
	return slices.ContainsFunc(before, func(earlier *ast.ConstraintNode) bool {
		return earlier != nil && reflect.DeepEqual(*earlier, *constraint)
	})
}

// dropTable forgets a table and everything declared on it.
func (s *storedNames) dropTable(table relation) {
	delete(s.spaces[spaceKey(relationSpace, table.schema)], table.stored)
	delete(s.spaces, spaceKey(constraintSpace, table.tableKey()))
	delete(s.spaces, spaceKey(columnSpace, table.tableKey()))
}

// follow records the names one statement declares, drops or renames, and
// then the names the server derives for what it leaves unnamed.
func (s *storedNames) follow(statement ast.Node) error {
	s.derived = nil
	if err := s.followWritten(statement); err != nil {
		return err
	}
	s.claimDerivedNames()
	return nil
}

// followWritten records the names one statement writes.
func (s *storedNames) followWritten(statement ast.Node) error {
	switch node := statement.(type) {
	case *ast.CreateTableNode:
		return s.createTable(node)
	case *ast.IndexNode:
		return s.index(node)
	case *ast.AlterTableNode:
		return s.alterTable(node)
	case *ast.CreateViewNode:
		_, err := s.declareRelation("view", node.Name, existingRule[node.Replace])
		return err
	case *ast.CreateMaterializedViewNode:
		rule := existingRule[node.IfNotExists]
		return s.skipIfExisted(node, rule)(s.declareRelation("materialized view", node.Name, rule))
	case *ast.CreateSequenceNode:
		rule := existingRule[node.IfNotExists]
		return s.skipIfExisted(node, rule)(s.declareRelation("sequence", schemaQualified(node.Schema, node.Name), rule))
	case *ast.CreateFunctionNode:
		return s.routine(string(ast.RoutineKindFunction), node.Name, node.Parameters, node.Replace)
	case *ast.RoutineNode:
		return s.routine(strings.ToLower(string(node.Kind)), node.Name, node.Parameters, node.Replace)
	case *ast.CreateTriggerNode:
		table := s.relation(node.Table)
		_, err := s.declare(triggerSpace, table.tableKey(), "trigger", node.Name, existingRule[node.Replace])
		return err
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
		return s.renameName(relationSpace, index.schema, "index", index.written, node.NewName)
	case *ast.DropIndexNode:
		s.dropRelation(node.Name)
	case *ast.DropViewNode:
		s.dropRelation(node.Name)
	case *ast.DropMaterializedViewNode:
		s.dropRelation(node.Name)
	case *ast.DropSequenceNode:
		s.dropRelation(schemaQualified(node.Schema, node.Name))
	}
	return nil
}

// schemaQualified is name as a statement writes it, with the schema the node
// holds apart in front when there is one.
func schemaQualified(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

func (s *storedNames) declareRelation(kind, raw string, rule onExisting) (existed bool, err error) {
	name := s.relation(raw)
	return s.declareName(relationSpace, name.schema, kind, name.written, rule)
}

// skipIfExisted records statement as one the server skips when its name was
// already in use and rule skips such a statement, as IF NOT EXISTS does, and
// passes the error on.
func (s *storedNames) skipIfExisted(statement ast.Node, rule onExisting) func(existed bool, err error) error {
	return func(existed bool, err error) error {
		if err == nil && existed && rule == skipExisting {
			s.skipped[statement] = true
		}
		return err
	}
}

// routine records a function or a procedure, which pg_proc keeps unique per
// schema, name and argument types: an overload with other arguments is
// another routine, and a function and a procedure of one signature collide.
func (s *storedNames) routine(kind, raw, parameters string, replace bool) error {
	name := s.relation(raw)
	owner := name.schema + "\x00" + routineargs.InputTypes(parameters)
	_, err := s.declareName(routineSpace, owner, kind, name.written, existingRule[replace])
	return err
}

func (s *storedNames) dropRelation(raw string) {
	name := s.relation(raw)
	s.dropName(relationSpace, name.schema, name.written)
}

func (s *storedNames) createTable(node *ast.CreateTableNode) error {
	table := s.relation(node.Name)
	existed, err := s.declareName(relationSpace, table.schema, "table", table.written, existingRule[node.IfNotExists])
	if err != nil || existed {
		// A table declared again declares nothing new on it: the server either
		// skipped the statement or refused it, and the reader decides which.
		return s.skipIfExisted(node, existingRule[node.IfNotExists])(existed, err)
	}
	for _, column := range node.Columns {
		if err := s.column(table, column, refuseExisting); err != nil {
			return err
		}
	}
	for i, constraint := range node.Constraints {
		if repeatsIndexConstraint(node.Constraints[:i], constraint) {
			continue
		}
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
	s.derived = append(s.derived, func() { s.claimColumnDerivedNames(table, column) })
	return nil
}

// claimColumnDerivedNames enters the names the server gives what a column
// declares without naming it: the sequence of a serial or identity column,
// and the index of a column PRIMARY KEY or UNIQUE. They take a place in the
// schema's relation namespace, so a later CREATE ... IF NOT EXISTS of the name
// is skipped, as the server skips it.
func (s *storedNames) claimColumnDerivedNames(table relation, column *ast.ColumnNode) {
	stored := storedIdentifier(s.sourcePlatform, foldedIdentifierPart(s.sourcePlatform, strings.TrimSpace(column.Name)))
	if column.IdentityGeneration != "" || isSerialType(column.Type) {
		s.claimDerived(relationSpace, table.schema, "sequence", pgname.Constraint(table.stored, []string{stored}, "seq", s.taken(relationSpace, table.schema)))
	}
	taken := s.keyNameTaken(table.schema)
	if column.Primary {
		s.claimIndexConstraintName(table, "primary key", pgname.PrimaryKey(table.stored, taken))
	}
	if column.Unique {
		s.claimIndexConstraintName(table, "unique constraint", pgname.Unique(table.stored, []string{stored}, nil, taken))
	}
}

// claimIndexConstraintName enters the derived name of an unnamed constraint
// that builds an index, on the table and in the schema.
func (s *storedNames) claimIndexConstraintName(table relation, kind, name string) {
	s.claimDerived(constraintSpace, table.tableKey(), kind, name)
	s.claimDerived(relationSpace, table.schema, "index of "+kind, name)
}

// isSerialType reports whether a column type is one of PostgreSQL's serial
// shorthands, which create a sequence named for the column.
func isSerialType(columnType string) bool {
	switch strings.ToLower(strings.TrimSpace(columnType)) {
	case "serial", "serial4", "bigserial", "serial8", "smallserial", "serial2":
		return true
	default:
		return false
	}
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
		if strings.TrimSpace(constraint.Name) == "" {
			s.derived = append(s.derived, func() { s.claimIndexConstraintName(table, kind, s.derivedKeyName(table, constraint)) })
			return nil
		}
		return s.indexConstraint(table, kind, constraint.Name)
	}
	_, err := s.declare(constraintSpace, table.tableKey(), kind, constraint.Name, refuseExisting)
	return err
}

// indexConstraint records a constraint that builds an index of its own name,
// which lives in the schema's relation namespace as well as on the table. A cut
// name is the constraint's, reported once.
func (s *storedNames) indexConstraint(table relation, kind, name string) error {
	if _, err := s.declare(constraintSpace, table.tableKey(), kind, name, refuseExisting); err != nil {
		return err
	}
	_, err := s.claim(relationSpace, table.schema, "index of "+kind, name, refuseExisting)
	return err
}

// derivedKeyName is the name the server gives an unnamed table PRIMARY KEY or
// UNIQUE constraint.
func (s *storedNames) derivedKeyName(table relation, constraint *ast.ConstraintNode) string {
	taken := s.keyNameTaken(table.schema)
	if constraint.Type == ast.PrimaryKeyConstraint {
		return pgname.PrimaryKey(table.stored, taken)
	}
	columns := make([]string, 0, len(constraint.Columns))
	for _, column := range constraint.Columns {
		columns = append(columns, storedIdentifier(s.sourcePlatform, foldedIdentifierPart(s.sourcePlatform, strings.TrimSpace(column))))
	}
	include := make([]string, 0, len(constraint.IncludeColumns))
	for _, column := range constraint.IncludeColumns {
		include = append(include, storedIdentifier(s.sourcePlatform, foldedIdentifierPart(s.sourcePlatform, strings.TrimSpace(column))))
	}
	return pgname.Unique(table.stored, columns, include, taken)
}

func (s *storedNames) index(node *ast.IndexNode) error {
	if strings.TrimSpace(node.Name) == "" {
		return nil
	}
	// An index lives in the schema of the table it is on.
	table := s.relation(node.Table)
	rule := existingRule[node.IfNotExists]
	return s.skipIfExisted(node, rule)(s.declare(relationSpace, table.schema, "index", node.Name, rule))
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
	s.dropName(relationSpace, table.schema, table.written)
	if _, err := s.declareName(relationSpace, table.schema, "table", renamed.written, refuseExisting); err != nil {
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

// seed enters the relation names of base, the earlier documents a document
// was handed rather than read itself, so a statement of this document finds
// them in use as the server would.
func (s *storedNames) seed(base *schemamodel.Database) {
	if base == nil {
		return
	}
	schemas := map[string]bool{"": true}
	for _, table := range base.Tables {
		schemas[table.Schema] = true
	}
	for _, sequence := range base.Sequences {
		schemas[sequence.Schema] = true
	}
	for schema := range schemas {
		owner := schema
		if owner == "" {
			owner = s.defaultSchema
		}
		for name := range pgname.RelationNames([]*schemamodel.Database{base}, schema) {
			s.claimDerived(relationSpace, owner, "relation", name)
		}
	}
}

// followStoredNames checks the names statements declare against the names the
// document already holds and reports each name it cuts to notice. It runs
// before the conversion, which records only the stored names and could not
// tell a collision from a redeclaration, and refuses a collision that cutting
// causes there. A name the file declares again is returned as pending, for
// the caller to refuse once the conversion has run.
//
// The namespaces are PostgreSQL's catalog, measured on 15.19. The check runs
// for the dialects whose descriptor says their server cuts long names the way
// PostgreSQL's lexer does, PostgreSQL and YugabyteDB, which keep that catalog.
// CockroachDB keeps a long name whole and names an index per table, so it is
// not one of them.
func (d *Document) followStoredNames(statements *ast.StatementList, dialect string, notice func(StoredName)) (pending, err error) {
	if statements == nil || !cutsNames(dialect) {
		return nil, nil
	}
	if d.names == nil || d.names.sourcePlatform != dialect {
		d.names = newStoredNames(dialect)
		d.names.seed(d.base)
	}
	d.names.duplicate = nil
	d.names.file++
	reported := len(d.names.cut)
	for _, statement := range statements.Statements {
		if err := d.names.follow(statement); err != nil {
			return nil, err
		}
	}
	if notice != nil {
		for _, name := range d.names.cut[reported:] {
			notice(name)
		}
	}
	return d.names.duplicate, nil
}
