package sqlschema

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
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
	// server, claimed once its written names are. That is the server's order
	// for the CHECK and NOT NULL constraints a CREATE TABLE writes, which it
	// creates before any index; an index a key builds takes its name at its
	// own turn instead, so a later key written under that name is refused by
	// the server, and by the conversion for one CREATE TABLE.
	derived []func()
}

type storedEntry struct {
	kind    string
	written string
	// file numbers the file of a schema directory that declared the name.
	file int
	ownership
}

// ownership is what removes a name with something else: the table whose DROP
// TABLE drops the index on it, the index of one of its keys, or a serial or
// identity sequence, and the columns whose DROP COLUMN drops it too, such as
// a column's own key, sequence and constraints. An index or a table key over
// a column needs no column here: the conversion refuses a DROP COLUMN that
// one still reads, as it models no cascade, so one a DROP COLUMN leaves is
// one the server keeps.
type ownership struct {
	table   string
	columns []string
}

// droppedWith reports whether a DROP COLUMN of column on table removes the
// name.
func (o ownership) droppedWith(table, column string) bool {
	return o.table == table && slices.Contains(o.columns, column)
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
	// A name written as the bytes the server keeps is the object stored under
	// them, whichever of the two spellings comes first; any other second
	// spelling stored the same way is a second object.
	if previous.written != written && written != stored && previous.written != stored {
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
func (s *storedNames) claimDerived(space, owner, kind, name string, owned ownership) {
	key := spaceKey(space, owner)
	if s.spaces[key] == nil {
		s.spaces[key] = make(map[string]storedEntry)
	}
	if _, found := s.spaces[key][name]; !found {
		s.spaces[key][name] = storedEntry{kind: kind, written: name, file: s.file, ownership: owned}
	}
}

// own ties the name a declaration has just entered to what removes it with
// it. A name the namespace held before keeps the owner it had.
func (s *storedNames) own(space, owner, raw string, existed bool, owned ownership) {
	if existed || strings.TrimSpace(raw) == "" {
		return
	}
	key := spaceKey(space, owner)
	stored := storedIdentifier(s.sourcePlatform, foldedIdentifierPart(s.sourcePlatform, strings.TrimSpace(raw)))
	if entry, found := s.spaces[key][stored]; found && entry.table == "" {
		entry.ownership = owned
		s.spaces[key][stored] = entry
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

// dropTable forgets a table and everything declared on it: its constraints,
// columns and triggers, and the indexes and sequences it owns. A sequence it
// does not own, such as one a column default draws from, stays.
func (s *storedNames) dropTable(table relation) {
	relations := s.spaces[spaceKey(relationSpace, table.schema)]
	delete(relations, table.stored)
	for name, entry := range relations {
		if entry.table == table.tableKey() {
			delete(relations, name)
		}
	}
	for _, space := range []string{constraintSpace, columnSpace, triggerSpace} {
		delete(s.spaces, spaceKey(space, table.tableKey()))
	}
}

// dropColumn forgets a column of table and the names the server drops with
// it: the keys and indexes over it, and its own sequence and constraints.
func (s *storedNames) dropColumn(table relation, raw string) {
	s.drop(columnSpace, table.tableKey(), raw)
	column := s.storedColumn(raw)
	for _, key := range []string{spaceKey(relationSpace, table.schema), spaceKey(constraintSpace, table.tableKey())} {
		for name, entry := range s.spaces[key] {
			if entry.droppedWith(table.tableKey(), column) {
				delete(s.spaces[key], name)
			}
		}
	}
}

// storedColumn is a column name as the server stores it.
func (s *storedNames) storedColumn(raw string) string {
	return storedIdentifier(s.sourcePlatform, foldedIdentifierPart(s.sourcePlatform, strings.TrimSpace(raw)))
}

// storedColumns is [storedNames.storedColumn] for each of columns.
func (s *storedNames) storedColumns(columns []string) []string {
	stored := make([]string, 0, len(columns))
	for _, column := range columns {
		stored = append(stored, s.storedColumn(column))
	}
	return stored
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
		if _, err := s.column(table, column, refuseExisting); err != nil {
			return err
		}
	}
	keys := s.tableKeys(node)
	for i, constraint := range node.Constraints {
		if constraint == nil || repeatsIndexConstraint(node.Constraints[:i], constraint) {
			continue
		}
		if !buildsIndex(constraint) {
			if err := s.constraint(table, constraint); err != nil {
				return err
			}
			continue
		}
		// A key folded into another, or one left unnamed, declares no name
		// here; the key that names its index declares it.
		if key := keys.namedBy(constraint); key != nil {
			if err := s.indexConstraint(table, key.kind(), key.name, key.ownership(table)); err != nil {
				return err
			}
		}
	}
	for _, key := range keys {
		if key.name == "" {
			s.derived = append(s.derived, func() {
				s.claimIndexConstraintName(table, key.kind(), s.foldedKeyName(table, key), key.ownership(table))
			})
		}
	}
	for _, owned := range node.OwnedConstraints {
		if err := s.indexConstraint(table, "constraint", owned.Name, ownership{table: table.tableKey()}); err != nil {
			return err
		}
	}
	for _, index := range node.Indexes {
		existed, err := s.declare(relationSpace, table.schema, "index", index.Name, existingRule[index.IfNotExists])
		if err != nil {
			return err
		}
		s.own(relationSpace, table.schema, index.Name, existed, ownership{table: table.tableKey()})
	}
	return nil
}

// foldedKey is the keys of one CREATE TABLE the server builds one index for.
// transformIndexConstraints puts the primary key first and folds each later
// key whose columns, INCLUDE columns, NULLS clause and deferral equal an
// earlier one's into it; when the earlier key has no name, it takes the later
// key's. Measured on PostgreSQL 15.19 and 18.6: `id int PRIMARY KEY, CONSTRAINT
// k UNIQUE (id)` builds one index, the primary key named k, and `CONSTRAINT k1
// UNIQUE (a), CONSTRAINT k2 UNIQUE (a)` builds k1 alone. ALTER TABLE folds no
// keys: `ADD UNIQUE (a), ADD UNIQUE (a)` builds t_a_key and t_a_key1.
type foldedKey struct {
	primary bool
	// name is the name the index takes, empty for one the server derives,
	// and namedBy the constraint that wrote it.
	name      string
	namedBy   *ast.ConstraintNode
	columns   []string
	include   []string
	signature string
}

func (k *foldedKey) kind() string {
	if k.primary {
		return "primary key"
	}
	return "unique constraint"
}

func (k *foldedKey) ownership(table relation) ownership {
	return ownership{table: table.tableKey(), columns: append(slices.Clone(k.columns), k.include...)}
}

type foldedKeys []*foldedKey

// namedBy is the key whose name constraint writes, or nil for a constraint
// whose key took no name from it.
func (keys foldedKeys) namedBy(constraint *ast.ConstraintNode) *foldedKey {
	for _, key := range keys {
		if key.namedBy == constraint {
			return key
		}
	}
	return nil
}

// tableKeys folds the keys a CREATE TABLE writes as the server does. The
// parser lifts every named key, and every key with a clause of its own, into
// the table's constraints in the order the statement writes them, so the
// columns' own keys are unnamed and plain, and where they stand among the rest
// changes no name.
func (s *storedNames) tableKeys(node *ast.CreateTableNode) foldedKeys {
	var written []foldedKey
	for _, column := range node.Columns {
		columns := []string{s.storedColumn(column.Name)}
		if column.Primary {
			written = append(written, foldedKey{primary: true, columns: columns})
		}
		if column.Unique {
			written = append(written, foldedKey{columns: columns})
		}
	}
	for i, constraint := range node.Constraints {
		if constraint == nil || !buildsIndex(constraint) || repeatsIndexConstraint(node.Constraints[:i], constraint) {
			continue
		}
		key := foldedKey{
			primary: constraint.Type == ast.PrimaryKeyConstraint,
			name:    strings.TrimSpace(constraint.Name),
			columns: s.storedColumns(constraint.Columns),
			include: s.storedColumns(constraint.IncludeColumns),
		}
		if key.name != "" {
			key.namedBy = constraint
		}
		key.signature = keyClauses(constraint)
		written = append(written, key)
	}
	slices.SortStableFunc(written, func(a, b foldedKey) int {
		return cmp.Compare(boolRank(b.primary), boolRank(a.primary))
	})
	var keys foldedKeys
	for _, key := range written {
		key.signature = strings.Join(key.columns, "\x00") + "\x01" + strings.Join(key.include, "\x00") + "\x01" + key.signature
		index := slices.IndexFunc(keys, func(earlier *foldedKey) bool { return earlier.signature == key.signature })
		if index < 0 {
			keys = append(keys, &key)
			continue
		}
		if keys[index].name == "" && key.name != "" {
			keys[index].name, keys[index].namedBy = key.name, key.namedBy
		}
	}
	return keys
}

// keyClauses is what a key writes beyond its columns that keeps it apart from
// an equal one: its NULLS clause and its deferral. A column's own key writes
// neither.
func keyClauses(constraint *ast.ConstraintNode) string {
	var clauses []string
	if constraint.NullsDistinct != nil && !*constraint.NullsDistinct {
		clauses = append(clauses, "nulls not distinct")
	}
	if constraint.Deferrable {
		clauses = append(clauses, "deferrable "+strings.ToLower(cmp.Or(constraint.Initially, "immediate")))
	}
	return strings.Join(clauses, " ")
}

func boolRank(value bool) int {
	if value {
		return 1
	}
	return 0
}

// buildsIndex reports whether a constraint builds an index of its own name.
func buildsIndex(constraint *ast.ConstraintNode) bool {
	return constraint.Type == ast.PrimaryKeyConstraint || constraint.Type == ast.UniqueConstraint
}

// foldedKeyName is the name the server derives for an unnamed key.
func (s *storedNames) foldedKeyName(table relation, key *foldedKey) string {
	taken := s.keyNameTaken(table.schema)
	if key.primary {
		return pgname.PrimaryKey(table.stored, taken)
	}
	return pgname.Unique(table.stored, key.columns, key.include, taken)
}

// column records a column and the names it writes for its own constraints,
// and leaves the name of its sequence to be derived. Its PRIMARY KEY and
// UNIQUE are the caller's: a CREATE TABLE folds them with the table's keys.
func (s *storedNames) column(table relation, column *ast.ColumnNode, rule onExisting) (existed bool, err error) {
	existed, err = s.declare(columnSpace, table.tableKey(), "column", column.Name, rule)
	if err != nil || existed {
		return existed, err
	}
	owner := table.tableKey()
	owned := ownership{table: owner, columns: []string{s.storedColumn(column.Name)}}
	names := []struct{ kind, name string }{{"check", column.CheckName}, {"not-null constraint", column.NotNullConstraintName}}
	if column.ForeignKey != nil {
		names = append(names, struct{ kind, name string }{"foreign key", column.ForeignKey.Name})
	}
	for _, named := range names {
		existed, err := s.declare(constraintSpace, owner, named.kind, named.name, refuseExisting)
		if err != nil {
			return false, err
		}
		s.own(constraintSpace, owner, named.name, existed, owned)
	}
	s.derived = append(s.derived, func() { s.claimColumnSequence(table, column) })
	return false, nil
}

// claimColumnSequence enters the name of the sequence a serial or identity
// column builds, which the column owns. An identity column's SEQUENCE NAME is
// that name; one in an unwritten schema is left out, because the server puts
// it in the first schema of the search path, which a file does not state.
func (s *storedNames) claimColumnSequence(table relation, column *ast.ColumnNode) {
	stored := s.storedColumn(column.Name)
	owned := ownership{table: table.tableKey(), columns: []string{stored}}
	if written, named := identitySequenceName(column.IdentityOptions); named {
		if written == "" {
			return
		}
		sequence := s.relation(written)
		if len(splitSQLIdentifier(written)) > 1 || sequence.schema == table.schema {
			s.claimDerived(relationSpace, sequence.schema, "sequence", sequence.stored, owned)
		}
		return
	}
	if column.IdentityGeneration != "" || isSerialType(column.Type) {
		name := pgname.Constraint(table.stored, []string{stored}, "seq", s.taken(relationSpace, table.schema))
		s.claimDerived(relationSpace, table.schema, "sequence", name, owned)
	}
}

// identitySequenceName reads the SEQUENCE NAME an identity column's options
// write. named reports the clause; name is empty for one it cannot read.
func identitySequenceName(options string) (name string, named bool) {
	at := sequenceNameClause.FindStringIndex(options)
	if at == nil {
		return "", false
	}
	rest := strings.TrimSpace(options[at[1]:])
	end, quoted := 0, false
	for end < len(rest) {
		character := rest[end]
		switch {
		case character == '"':
			quoted = !quoted
		case !quoted && (character == ' ' || character == '\t' || character == '\n' || character == ')' || character == ','):
			return rest[:end], true
		}
		end++
	}
	if quoted {
		return "", true
	}
	return rest, true
}

// sequenceNameClause is the SEQUENCE NAME clause of identity options.
var sequenceNameClause = regexp.MustCompile(`(?i)\bSEQUENCE\s+NAME\s+`)

// claimColumnKeys enters the names of the keys a column ALTER TABLE ADD
// COLUMN adds writes for itself. One column's PRIMARY KEY and UNIQUE build one
// index, as within a CREATE TABLE.
func (s *storedNames) claimColumnKeys(table relation, column *ast.ColumnNode) {
	stored := s.storedColumn(column.Name)
	owned := ownership{table: table.tableKey(), columns: []string{stored}}
	taken := s.keyNameTaken(table.schema)
	switch {
	case column.Primary:
		s.claimIndexConstraintName(table, "primary key", pgname.PrimaryKey(table.stored, taken), owned)
	case column.Unique:
		s.claimIndexConstraintName(table, "unique constraint", pgname.Unique(table.stored, []string{stored}, nil, taken), owned)
	}
}

// claimIndexConstraintName enters the derived name of an unnamed constraint
// that builds an index, on the table and in the schema.
func (s *storedNames) claimIndexConstraintName(table relation, kind, name string, owned ownership) {
	s.claimDerived(constraintSpace, table.tableKey(), kind, name, owned)
	s.claimDerived(relationSpace, table.schema, "index of "+kind, name, owned)
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
	owned := ownership{table: table.tableKey(), columns: append(s.storedColumns(constraint.Columns), s.storedColumns(constraint.IncludeColumns)...)}
	if buildsIndex(constraint) {
		if strings.TrimSpace(constraint.Name) == "" {
			s.derived = append(s.derived, func() { s.claimIndexConstraintName(table, kind, s.derivedKeyName(table, constraint), owned) })
			return nil
		}
		return s.indexConstraint(table, kind, constraint.Name, owned)
	}
	if constraint.Type != ast.ForeignKeyConstraint {
		// A CHECK may read any column, and which ones is not followed here, so
		// it goes with its table alone.
		owned.columns = nil
	}
	existed, err := s.declare(constraintSpace, table.tableKey(), kind, constraint.Name, refuseExisting)
	s.own(constraintSpace, table.tableKey(), constraint.Name, existed || err != nil, owned)
	return err
}

// indexConstraint records a constraint that builds an index of its own name,
// which lives in the schema's relation namespace as well as on the table. A cut
// name is the constraint's, reported once.
func (s *storedNames) indexConstraint(table relation, kind, name string, owned ownership) error {
	existed, err := s.declare(constraintSpace, table.tableKey(), kind, name, refuseExisting)
	if err != nil {
		return err
	}
	s.own(constraintSpace, table.tableKey(), name, existed, owned)
	existed, err = s.claim(relationSpace, table.schema, "index of "+kind, name, refuseExisting)
	s.own(relationSpace, table.schema, name, existed || err != nil, owned)
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
	existed, err := s.declare(relationSpace, table.schema, "index", node.Name, rule)
	if err := s.skipIfExisted(node, rule)(existed, err); err != nil {
		return err
	}
	s.own(relationSpace, table.schema, node.Name, existed, ownership{table: table.tableKey()})
	return nil
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
		existed, err := s.column(*table, op.Column, existingRule[op.IfNotExists])
		if err == nil && !existed {
			added := *table
			s.derived = append(s.derived, func() { s.claimColumnKeys(added, op.Column) })
		}
		return err
	case *ast.AddConstraintOperation:
		return s.constraint(*table, op.Constraint)
	case *ast.AddIndexOperation:
		if op.Index != nil && strings.TrimSpace(op.Index.Name) != "" {
			existed, err := s.declare(relationSpace, table.schema, "index", op.Index.Name, existingRule[op.Index.IfNotExists])
			if err == nil {
				s.own(relationSpace, table.schema, op.Index.Name, existed, ownership{table: table.tableKey()})
			}
			return err
		}
	case *ast.DropConstraintOperation:
		s.drop(constraintSpace, owner, op.ConstraintName)
		if !op.ForeignKey && !op.Check {
			s.drop(relationSpace, table.schema, op.ConstraintName)
		}
	case *ast.DropColumnOperation:
		s.dropColumn(*table, op.ColumnName)
	case *ast.RenameColumnOperation:
		s.renameOwnedColumn(owner, s.storedColumn(op.OldName), s.storedColumn(op.NewName))
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
	for _, space := range []string{constraintSpace, columnSpace, triggerSpace} {
		if names, ok := s.spaces[spaceKey(space, table.tableKey())]; ok {
			delete(s.spaces, spaceKey(space, table.tableKey()))
			s.spaces[spaceKey(space, renamed.tableKey())] = names
		}
	}
	for _, names := range s.spaces {
		for name, entry := range names {
			if entry.table == table.tableKey() {
				entry.table = renamed.tableKey()
				names[name] = entry
			}
		}
	}
	*table = renamed
	return nil
}

// renameOwnedColumn moves the names a column of table owns to its new name.
func (s *storedNames) renameOwnedColumn(table, from, to string) {
	for _, names := range s.spaces {
		for name, entry := range names {
			if entry.table == table && slices.Contains(entry.columns, from) {
				entry.columns = slices.Clone(entry.columns)
				for i, column := range entry.columns {
					if column == from {
						entry.columns[i] = to
					}
				}
				names[name] = entry
			}
		}
	}
}

// seed enters the relation names of base, the earlier documents a document
// was handed rather than read itself, so a statement of this document finds
// them in use as the server would: its relations, the indexes of its named
// keys, and the index of a primary key it left unnamed, which no fold
// removes. The derived name of a UNIQUE is left out, because a fold may have
// removed the key. An index or key goes with its table.
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
		owner := cmp.Or(schema, s.defaultSchema)
		for name := range pgname.RelationNames([]*schemamodel.Database{base}, schema) {
			s.claimDerived(relationSpace, owner, "relation", name, ownership{})
		}
		s.seedKeys(base, schema, owner)
	}
	for _, index := range base.Indexes {
		table := s.relation(cmp.Or(index.TableName, tableNameOf(base, index.StructName)))
		if entry, found := s.spaces[spaceKey(relationSpace, table.schema)][index.Name]; found && entry.table == "" {
			entry.ownership = ownership{table: table.tableKey()}
			s.spaces[spaceKey(relationSpace, table.schema)][index.Name] = entry
		}
	}
}

// seedKeys enters the index names of the keys base gives the tables of schema.
func (s *storedNames) seedKeys(base *schemamodel.Database, schema, owner string) {
	for _, table := range base.Tables {
		if table.Schema != schema {
			continue
		}
		stored := relation{schema: owner, written: table.Name, stored: table.Name}
		owned := ownership{table: stored.tableKey()}
		switch {
		case table.PrimaryKeyName != "":
			s.claimIndexConstraintName(stored, "primary key", table.PrimaryKeyName, owned)
		case hasPrimaryKey([]*schemamodel.Database{base}, table):
			s.claimIndexConstraintName(stored, "primary key", pgname.PrimaryKey(table.Name, s.keyNameTaken(owner)), owned)
		}
		for _, constraint := range base.Constraints {
			kind, builds := keyKinds[strings.ToUpper(constraint.Type)]
			if builds && constraint.Name != "" && constraint.StructName == table.StructName {
				s.claimIndexConstraintName(stored, kind, constraint.Name, owned)
			}
		}
	}
}

// keyKinds names the model's constraint types that build an index.
var keyKinds = map[string]string{"PRIMARY KEY": "primary key", "UNIQUE": "unique constraint"}

// tableNameOf is the qualified name of the table base declares under
// structName, or empty.
func tableNameOf(base *schemamodel.Database, structName string) string {
	for _, table := range base.Tables {
		if table.StructName == structName {
			return table.QualifiedName()
		}
	}
	return ""
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
