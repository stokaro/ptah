// Package devseed decides what a dev database rehearsal recreates beyond the
// state its plan compared.
//
// A rehearsal recreates the target's current state on the dev database and
// then runs the plan there. A comparison narrowed by --include, --exclude,
// --schemas or a URL bound to one schema leaves out objects the target still
// holds, and the statements the dev database runs can name them: a policy
// that calls auth.uid() with the auth schema excluded, a foreign key to a
// table the selection does not hold, a trigger that executes a function in a
// schema the URL does not reach. The target applies such a statement, and a
// dev database recreated from the narrowed state alone refuses it.
//
// The comparison stays narrowed. [Closure] adds to the recreated state the
// objects of the target that the statements need, each whole and as the
// target holds it, and then what those objects need in turn, until the
// statements name nothing more the target holds. What nothing names stays
// out: an object a selection leaves out is often one the dev database cannot
// build, such as a schema an extension of another server owns.
//
// Whether a statement needs an object is read off its text: a name in its
// code, qualified by the object's schema, or left unqualified and resolved
// along the dev session's search path. The body of a routine written in SQL or
// PL/pgSQL is read as code too, though its statement quotes it. Any other name
// inside a string literal or a comment names nothing, so a reference spelled
// as text, such as nextval('app.ids'), is not found here, and the dev database
// reports the object it lacks.
//
// Roles are not added. A role belongs to the server, not to one database, and
// the rehearsal relies on the dev server holding the roles the target's do,
// as it does for the roles the narrowed state itself names.
package devseed

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/internal/deporder"
)

// Object is one object of the target that [Closure] added to the recreated
// state.
type Object struct {
	// Kind names the object's family as a message names it: schema, table,
	// view, materialized view, function, procedure, sequence, enum, or the
	// last segment of a feature owner's kind, such as domain or extension.
	Kind string
	// Schema is the schema the object is in, empty for an object of the
	// connection's own schema and for an object that belongs to the whole
	// database, such as an extension.
	Schema string
	// Name is the object's own name.
	Name string
}

// String names the object as a message names it: `function "auth.uid"`.
func (o Object) String() string {
	if o.Schema == "" {
		return fmt.Sprintf("%s %q", o.Kind, o.Name)
	}
	return fmt.Sprintf("%s %q", o.Kind, o.Schema+"."+o.Name)
}

// Render returns the statements that recreate state on the dev database.
type Render func(state *catalog.Database) ([]string, error)

// Target reads the target's state across the schemas named, each folded to
// lower case, and the schemas the caller's own scope covers. A dialect whose
// connection reaches one schema whatever is named reads that schema.
type Target func(schemas []string) (*catalog.Database, error)

// Options says how the statements are read and where their names resolve.
type Options struct {
	// Dialect selects the lexical rules the statements are read with.
	Dialect string
	// DefaultSchema is the schema of an object a read leaves unqualified.
	DefaultSchema string
	// SearchPath is where the dev session resolves a name a statement leaves
	// unqualified. Empty resolves it in DefaultSchema alone.
	SearchPath []string
}

// Result is the state a rehearsal recreates and what it took to get there.
type Result struct {
	// State is the narrowed state with the added objects.
	State *catalog.Database
	// Statements are what [Render] returned for State.
	Statements []string
	// Added are the objects of the target State holds beyond the narrowed
	// state, in the order they were added.
	Added []Object
}

// Closure returns seed, the narrowed state a plan compared, with every object
// of the target it lacks that the statements the dev database runs need:
// executed, which run after the recreated state, and the statements render
// returns for the state itself, which grow with each object added.
//
// What the state's own objects reference is followed before the state is
// rendered, since a render can refuse a state that holds a reference to an
// object it lacks: the table a foreign key references, the type of a column,
// and the names in the body of an SQL or PL/pgSQL routine.
//
// A nil target adds nothing: the comparison was not narrowed, so seed is
// everything the target holds. seed is not changed.
//
// A render or read that fails returns its error. Once an object was added,
// the error names what was added, since the failure may be one of them.
func Closure(seed *catalog.Database, executed []string, render Render, target Target, opts Options) (Result, error) {
	if target == nil || seed == nil {
		statements, err := render(seed)
		if err != nil {
			return Result{}, err
		}
		return Result{State: seed, Statements: statements}, nil
	}
	c := &closure{state: seed, target: target, opts: opts}
	var statements []string
	rendered := false
	for {
		grow := c.grow
		if !rendered {
			grow = c.growPast
		}
		grew, err := grow(slices.Concat(executed, statements))
		if err != nil {
			return Result{}, WithAdded(err, c.added)
		}
		if rendered && !grew {
			return Result{State: c.state, Statements: statements, Added: c.added}, nil
		}
		if statements, err = render(c.state); err != nil {
			return Result{}, WithAdded(err, c.added)
		}
		rendered = true
	}
}

// closure is the state [Closure] grows and what it read of the target.
type closure struct {
	state       *catalog.Database
	target      Target
	opts        Options
	added       []Object
	read        *catalog.Database
	readSchemas []string
}

// grow adds to the state every object of the target that statements, or the
// state's own objects, name and the state lacks, until they name nothing more
// it lacks. It reports whether anything was added.
func (c *closure) grow(statements []string) (bool, error) {
	return c.growWhile(statements, func(names) bool { return true })
}

// growPast is grow before the first render: the target is read only while a
// qualified name reaches past the state. An unqualified one is most often a
// column or a keyword, and the render that follows names every object the
// state needs, so reading the target for it would read the search path twice.
func (c *closure) growPast(statements []string) (bool, error) {
	return c.growWhile(statements, func(n names) bool { return n.reachesPast(c.state, c.opts) })
}

// growWhile is grow that stops once worth reports that the names are not
// worth a read.
func (c *closure) growWhile(statements []string, worth func(names) bool) (bool, error) {
	grew := false
	for {
		names := readNames(slices.Concat(statements, routineBodies(c.state)), c.opts)
		names.addReferences(c.state, c.opts)
		if !worth(names) {
			return grew, nil
		}
		if wanted := union(c.readSchemas, names.schemaNames()); c.read == nil || len(wanted) > len(c.readSchemas) {
			read, err := c.target(wanted)
			if err != nil {
				return grew, fmt.Errorf("read the target outside the compared scope: %w", err)
			}
			c.read, c.readSchemas = read, wanted
		}
		more, err := needed(c.state, c.read, names, c.opts)
		if err != nil {
			return grew, err
		}
		if len(more) == 0 {
			return grew, nil
		}
		state, schemas, err := with(c.state, c.read, more, c.opts)
		if err != nil {
			return grew, err
		}
		c.state, grew = state, true
		for _, candidate := range more {
			c.added = append(c.added, candidate.object)
		}
		c.added = append(c.added, schemas...)
	}
}

// AddedError reports that recreating the state failed after [Closure] added
// objects from outside the compared scope, and names them.
type AddedError struct {
	// Added are the objects added, in the order they were added.
	Added []Object
	Err   error
}

func (e *AddedError) Error() string {
	return fmt.Sprintf("%v; the rehearsal recreates these objects from outside the compared scope because the planned statements need them: %s",
		e.Err, Names(e.Added))
}

func (e *AddedError) Unwrap() error { return e.Err }

// Names lists objects as a message names them, comma-separated.
func Names(objects []Object) string {
	names := make([]string, 0, len(objects))
	for _, object := range objects {
		names = append(names, object.String())
	}
	return strings.Join(names, ", ")
}

// WithAdded wraps err with the objects added, as [Closure] does, in an
// [*AddedError]; err is returned as it is when none were.
func WithAdded(err error, added []Object) error {
	if err == nil || len(added) == 0 {
		return err
	}
	return &AddedError{Added: slices.Clone(added), Err: err}
}

// routineBodies are the bodies of state's routines written in SQL or
// PL/pgSQL, read as code. A rendered CREATE FUNCTION quotes its body as a
// string, which names nothing, yet PostgreSQL resolves the names in an SQL
// body when the function is created and in a PL/pgSQL body when it runs. A
// body in another language is not SQL, and its words name nothing.
func routineBodies(state *catalog.Database) []string {
	var bodies []string
	for _, routine := range state.Functions {
		switch fold(routine.Language) {
		case "", "sql", "plpgsql":
			bodies = append(bodies, routine.Body)
		}
	}
	return bodies
}

// key identifies an object by its schema and name, both folded.
type key struct {
	schema, name string
}

func fold(name string) string { return strings.ToLower(strings.TrimSpace(name)) }

// effective is the schema an object is in: its own, or the default schema
// for one a read left unqualified.
func effective(schema string, opts Options) string {
	if strings.TrimSpace(schema) == "" {
		return fold(opts.DefaultSchema)
	}
	return fold(schema)
}

// names is what the statements name: each qualified pair of parts as a
// schema and a name, each unqualified name, and each part that qualifies
// another.
type names struct {
	qualified   map[key]bool
	unqualified map[string]bool
	schemas     map[string]bool
	searchPath  []string
}

func readNames(statements []string, opts Options) names {
	found := names{qualified: make(map[key]bool), unqualified: make(map[string]bool), schemas: make(map[string]bool)}
	for _, statement := range statements {
		for _, parts := range deporder.CodeNames(statement, opts.Dialect) {
			if len(parts) == 1 {
				found.unqualified[parts[0]] = true
				continue
			}
			// A name of three parts may be schema.table.column or
			// database.schema.table, so every adjacent pair is a candidate.
			for i := 0; i+1 < len(parts); i++ {
				if parts[i] == "" || parts[i+1] == "" {
					continue
				}
				found.qualified[key{schema: parts[i], name: parts[i+1]}] = true
				found.schemas[parts[i]] = true
			}
		}
	}
	for _, schema := range opts.SearchPath {
		if folded := fold(schema); folded != "" && folded != "$user" {
			found.searchPath = append(found.searchPath, folded)
		}
	}
	if len(found.searchPath) == 0 && fold(opts.DefaultSchema) != "" {
		found.searchPath = []string{fold(opts.DefaultSchema)}
	}
	return found
}

// addReferences adds the objects state's own objects reference by catalog
// field rather than by text: the table each foreign key references, and the
// type each column has. A reference left without a schema is in the default
// schema, as a read leaves it.
func (n names) addReferences(state *catalog.Database, opts Options) {
	add := func(schema, name string) {
		if strings.TrimSpace(name) == "" {
			return
		}
		k := key{schema: effective(schema, opts), name: fold(name)}
		n.qualified[k] = true
		if k.schema != "" {
			n.schemas[k.schema] = true
		}
	}
	for _, constraint := range state.Constraints {
		if constraint.ForeignTable != nil {
			add(constraint.ForeignSchema, *constraint.ForeignTable)
		}
	}
	for _, table := range state.Tables {
		for _, column := range table.Columns {
			if column.UDTSchema != "" {
				add(column.UDTSchema, column.UDTName)
			}
		}
	}
}

// reachesPast reports whether a qualified name names an object state does not
// hold, in any family.
func (n names) reachesPast(state *catalog.Database, opts Options) bool {
	has := present(state, opts)
	for k := range n.qualified {
		held := false
		for _, family := range has {
			held = held || family[k]
		}
		if !held {
			return true
		}
	}
	return false
}

// schemaNames are the schemas the target is read at for these names: every
// schema a name is qualified by, and the search path.
func (n names) schemaNames() []string {
	out := slices.Collect(func(yield func(string) bool) {
		for schema := range n.schemas {
			if !yield(schema) {
				return
			}
		}
	})
	return union(out, n.searchPath)
}

// names reports whether an object of schema s named name is named.
func (n names) named(schema, name string) bool {
	if n.qualified[key{schema: schema, name: name}] {
		return true
	}
	return n.unqualified[name] && slices.Contains(n.searchPath, schema)
}

func union(a, b []string) []string {
	out := slices.Concat(a, b)
	slices.Sort(out)
	return slices.Compact(out)
}

// candidate is one object of the read the state lacks and the names reach.
type candidate struct {
	object Object
	key    key
	// family decides which collection of the read the object is taken from.
	family family
	// feature is the feature object, for familyFeature.
	feature schemaext.Object
}

type family int

const (
	familyTable family = iota
	familyView
	familyMatView
	familyFunction
	familySequence
	familyEnum
	familyFeature
)

// needed returns the objects of read that state lacks and names reach, in
// family and name order so a run is deterministic.
func needed(state, read *catalog.Database, n names, opts Options) ([]candidate, error) {
	if read == nil {
		return nil, nil
	}
	has := present(state, opts)
	var out []candidate
	consider := func(f family, kind, schema, name string) {
		k := key{schema: effective(schema, opts), name: fold(name)}
		if has[f][k] || !n.named(k.schema, k.name) {
			return
		}
		has[f][k] = true
		out = append(out, candidate{object: Object{Kind: kind, Schema: schema, Name: name}, key: k, family: f})
	}
	for _, table := range read.Tables {
		consider(familyTable, "table", table.Schema, table.Name)
	}
	for _, view := range read.Views {
		consider(familyView, "view", view.Schema, view.Name)
	}
	for _, view := range read.MatViews {
		consider(familyMatView, "materialized view", view.Schema, view.Name)
	}
	for _, function := range read.Functions {
		kind := "function"
		if strings.EqualFold(function.Kind, "procedure") {
			kind = "procedure"
		}
		consider(familyFunction, kind, function.Schema, function.Name)
	}
	for _, sequence := range read.Sequences {
		consider(familySequence, "sequence", sequence.Schema, sequence.Name)
	}
	for _, enum := range read.Enums {
		consider(familyEnum, "enum", enum.Schema, enum.Name)
	}
	features, err := neededFeatures(state, read, n, opts)
	if err != nil {
		return nil, err
	}
	out = append(out, features...)
	slices.SortStableFunc(out, func(a, b candidate) int {
		return cmp.Or(cmp.Compare(a.family, b.family), cmp.Compare(a.key.schema, b.key.schema), cmp.Compare(a.key.name, b.key.name))
	})
	return out, nil
}

// neededFeatures returns the feature objects of read that state lacks and the
// names reach: a schema-level object by its schema and name, and an object
// the database installed into a schema, such as an extension, when a name is
// qualified by that schema, since its members are named that way. An object a
// table owns, such as a policy, is never needed on its own.
func neededFeatures(state, read *catalog.Database, n names, opts Options) ([]candidate, error) {
	objects, err := read.FeatureObjects.All()
	if err != nil {
		return nil, err
	}
	var out []candidate
	for _, object := range objects {
		ref := object.Ref
		if ref.Parent.Normalized != "" {
			continue
		}
		if _, found, err := state.FeatureObjects.Get(ref); err != nil || found {
			if err != nil {
				return nil, err
			}
			continue
		}
		kind := string(ref.Kind)
		if slash := strings.LastIndex(kind, "/"); slash >= 0 {
			kind = kind[slash+1:]
		}
		if installation, ok := object.Value.(schemaext.SchemaInstallation); ok {
			schema := fold(installation.InstalledSchema())
			if schema != "" && n.schemas[schema] {
				out = append(out, candidate{
					object:  Object{Kind: kind, Name: ref.Name.Source},
					key:     key{schema: schema, name: fold(ref.Name.Source)},
					family:  familyFeature,
					feature: object,
				})
			}
			continue
		}
		if ref.Schema.Source == "" || ref.Name.Source == "" {
			continue
		}
		k := key{schema: effective(ref.Schema.Source, opts), name: fold(ref.Name.Source)}
		if n.named(k.schema, k.name) {
			out = append(out, candidate{
				object:  Object{Kind: kind, Schema: ref.Schema.Source, Name: ref.Name.Source},
				key:     k,
				family:  familyFeature,
				feature: object,
			})
		}
	}
	return out, nil
}

// present is what state holds, by family and key.
func present(state *catalog.Database, opts Options) map[family]map[key]bool {
	has := map[family]map[key]bool{
		familyTable: {}, familyView: {}, familyMatView: {}, familyFunction: {},
		familySequence: {}, familyEnum: {}, familyFeature: {},
	}
	mark := func(f family, schema, name string) {
		has[f][key{schema: effective(schema, opts), name: fold(name)}] = true
	}
	for _, table := range state.Tables {
		mark(familyTable, table.Schema, table.Name)
	}
	for _, view := range state.Views {
		mark(familyView, view.Schema, view.Name)
	}
	for _, view := range state.MatViews {
		mark(familyMatView, view.Schema, view.Name)
	}
	for _, function := range state.Functions {
		mark(familyFunction, function.Schema, function.Name)
	}
	for _, sequence := range state.Sequences {
		mark(familySequence, sequence.Schema, sequence.Name)
	}
	for _, enum := range state.Enums {
		mark(familyEnum, enum.Schema, enum.Name)
	}
	return has
}

// with returns state with the candidates taken from read: a table with its
// columns, indexes and constraints, a materialized view with its indexes,
// every overload of a function, and the schema of each object state does not
// hold, which it returns as objects of their own. The default schema is never
// added: the dev database has it. A table's triggers, policies and grants stay
// out: a statement that names the table needs it to exist, not to behave as it
// does on the target.
func with(state, read *catalog.Database, more []candidate, opts Options) (*catalog.Database, []Object, error) {
	out := *state
	is := wantedOf(more, opts)
	out.Tables = takeFrom(state.Tables, read.Tables, func(t catalog.Table) bool { return is(familyTable, t.Schema, t.Name) })
	out.Indexes = takeFrom(state.Indexes, read.Indexes, func(i catalog.Index) bool {
		return is(familyTable, i.Schema, i.TableName) || is(familyMatView, i.Schema, i.TableName)
	})
	out.Constraints = takeFrom(state.Constraints, read.Constraints, func(c catalog.Constraint) bool {
		return is(familyTable, c.Schema, c.TableName)
	})
	out.Views = takeFrom(state.Views, read.Views, func(v catalog.View) bool { return is(familyView, v.Schema, v.Name) })
	out.MatViews = takeFrom(state.MatViews, read.MatViews, func(v catalog.MaterializedView) bool {
		return is(familyMatView, v.Schema, v.Name)
	})
	out.Functions = takeFrom(state.Functions, read.Functions, func(f catalog.Function) bool {
		return is(familyFunction, f.Schema, f.Name)
	})
	out.Sequences = takeFrom(state.Sequences, read.Sequences, func(q catalog.Sequence) bool {
		return is(familySequence, q.Schema, q.Name)
	})
	out.Enums = takeFrom(state.Enums, read.Enums, func(e catalog.Enum) bool { return is(familyEnum, e.Schema, e.Name) })
	var features []schemaext.Object
	for _, c := range more {
		if c.family == familyFeature {
			features = append(features, c.feature)
		}
	}
	if len(features) > 0 {
		added, err := schemaext.NewObjects(features...)
		if err != nil {
			return nil, nil, err
		}
		if out.FeatureObjects, err = state.FeatureObjects.Merge(added); err != nil {
			return nil, nil, err
		}
	}
	var addedSchemas []Object
	out.Schemas, addedSchemas = withSchemas(state.Schemas, read.Schemas, more, opts)
	return &out, addedSchemas, nil
}

// wantedOf reports whether an object of family f is one of more.
func wantedOf(more []candidate, opts Options) func(f family, schema, name string) bool {
	wanted := make(map[family]map[key]bool)
	for _, c := range more {
		if wanted[c.family] == nil {
			wanted[c.family] = make(map[key]bool)
		}
		wanted[c.family][c.key] = true
	}
	return func(f family, schema, name string) bool {
		return wanted[f][key{schema: effective(schema, opts), name: fold(name)}]
	}
}

// takeFrom returns held, copied, with each object of read that take accepts.
func takeFrom[T any](held, read []T, take func(T) bool) []T {
	out := slices.Clone(held)
	for _, object := range read {
		if take(object) {
			out = append(out, object)
		}
	}
	return out
}

// withSchemas returns held with the schema of each of more that it lacks,
// taken from read, and those schemas as objects. The default schema is never
// added: the dev database has it.
func withSchemas(held, read []catalog.Schema, more []candidate, opts Options) ([]catalog.Schema, []Object) {
	wanted := make(map[string]bool)
	for _, c := range more {
		if c.key.schema != "" && c.key.schema != fold(opts.DefaultSchema) {
			wanted[c.key.schema] = true
		}
	}
	have := make(map[string]bool)
	for _, schema := range held {
		have[fold(schema.Name)] = true
	}
	out := slices.Clone(held)
	var added []Object
	for _, schema := range read {
		if folded := fold(schema.Name); wanted[folded] && !have[folded] {
			out = append(out, schema)
			added = append(added, Object{Kind: "schema", Name: schema.Name})
			have[folded] = true
		}
	}
	return out, added
}
