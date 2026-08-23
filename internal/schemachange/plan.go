package schemachange

import (
	"errors"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemastate"
)

// ErrBlocked reports an attempt to render a change the target cannot host.
var ErrBlocked = errors.New("change is blocked")

// ErrNotRendered reports a change in a family this prototype compares but does
// not render.
//
// Policies, grants and roles are read into the canonical state and compared
// there, because that is where their identity and their coverage rules had to
// be settled. Rendering them is the shipping path's job until their own
// migration lands, and a planner that quietly emitted nothing for them would
// report a successful plan that changes none of them.
var ErrNotRendered = errors.New("change is in a family this planner does not render")

// Operation is one rendered statement and the change it came from.
//
// The change is carried rather than referenced by name, so a rendered statement
// traces back to the semantic change and the source facts that produced it.
// That is the #1350 property a renderer cannot satisfy when it receives only a
// list of names.
type PlannedOperation struct {
	Change Change
	Node   ast.Node
	SQL    string
}

// Plan turns ordered changes into rendered operations.
//
// It takes no schema description. That is the measurable difference from
// `GenerateSchemaDiffAST(diff, generated, dialect)`: every fact rendering needs
// is on the change, so there is nothing for a second parameter to supply and
// nothing for the two parameters to disagree about.
func Plan(changes []Change, profile schemastate.Profile) ([]PlannedOperation, error) {
	operations := make([]PlannedOperation, 0, len(changes))
	problems := make([]error, 0)
	for _, change := range changes {
		if change.Status != Planned {
			problems = append(problems, fmt.Errorf("%w: %s", ErrBlocked, change.Diagnostic))
			continue
		}
		nodes, err := nodesFor(change, profile)
		if err != nil {
			problems = append(problems, err)
			continue
		}
		for _, node := range nodes {
			sql, renderErr := renderer.RenderSQL(profile.Dialect, node)
			if renderErr != nil {
				problems = append(problems, fmt.Errorf("%s: %w", change, renderErr))
				continue
			}
			operations = append(operations, PlannedOperation{Change: change, Node: node, SQL: sql})
		}
	}
	if err := errors.Join(problems...); err != nil {
		return nil, err
	}
	return operations, nil
}

// nodesFor renders one change into the statements it needs.
//
// A modification is two statements on every target Ptah supports: no engine
// alters a foreign key's referential actions in place. It stays ONE change with
// two statements rather than becoming two changes, so the drop and the add
// cannot be separated by a later stage that sees only one of them.
func nodesFor(change Change, profile schemastate.Profile) ([]ast.Node, error) {
	switch change.ID.Kind {
	case objectidentity.KindConstraint:
		return constraintNodes(change, profile)
	case objectidentity.KindTable:
		return tableNodes(change)
	case objectidentity.KindColumn:
		return columnNodes(change, profile)
	default:
		return nil, fmt.Errorf("%s: %w", change, ErrNotRendered)
	}
}

func constraintNodes(change Change, profile schemastate.Profile) ([]ast.Node, error) {
	switch change.Operation {
	case Add:
		return []ast.Node{addNode(change, profile)}, nil
	case Remove:
		return []ast.Node{dropNode(change, profile)}, nil
	case Modify:
		return []ast.Node{dropNode(change, profile), addNode(change, profile)}, nil
	default:
		return nil, fmt.Errorf("%s: unknown operation %q", change, change.Operation)
	}
}

// tableNodes renders a whole table, columns included.
//
// The columns ride inside the CREATE rather than arriving as separate ALTERs,
// which is the same decision [compareTables] makes: a column of a table that
// does not exist yet is not a separate statement, and splitting it would let a
// later stage order the two apart.
func tableNodes(change Change) ([]ast.Node, error) {
	switch change.Operation {
	case Add:
		return []ast.Node{createTableNode(change)}, nil
	case Remove:
		// IF EXISTS and CASCADE, which is what the shipping planner emits for
		// the same change: a table with a dependent view or foreign key cannot
		// be dropped without CASCADE at all, and the guard keeps a plan
		// replayed against a database that already lost the table from
		// failing. The difference the canonical model makes to this change is
		// on the CHANGE -- irreversible, data loss -- not on the statement.
		return []ast.Node{&ast.DropTableNode{
			Name: tableName(change.ID), IfExists: true, Cascade: true,
		}}, nil
	default:
		return nil, fmt.Errorf("%s: unknown operation %q", change, change.Operation)
	}
}

// createTableNode builds the CREATE for a table the desired schema declares.
//
// A single-column key rides on its column and a composite one becomes a
// table-level constraint, which is what the shipping planner writes and what
// the column syntax can express: `PRIMARY KEY` on two columns declares two
// keys, not one over both.
func createTableNode(change Change) ast.Node {
	table := change.After.Table
	columns := make([]*ast.ColumnNode, 0, len(table.Columns))
	keyColumns := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		keyColumns = appendPrimaryKeyColumn(keyColumns, column)
	}
	composite := len(keyColumns) > 1
	for _, column := range table.Columns {
		columns = append(columns, columnNode(column, composite))
	}
	node := &ast.CreateTableNode{
		Name:    tableName(change.ID),
		Columns: columns,
		Options: tableOptions(*table),
	}
	return withCompositeKey(node, keyColumns, composite)
}

// appendPrimaryKeyColumn keeps the loop above free of a branch the repository's
// lint rules would rather see named.
func appendPrimaryKeyColumn(columns []string, column schemastate.Column) []string {
	return map[bool][]string{
		true:  append(columns, column.ID.Name.Source),
		false: columns,
	}[column.PrimaryKey]
}

// withCompositeKey attaches the table-level key constraint a multi-column key
// needs. A single-column key is already on its column.
func withCompositeKey(node *ast.CreateTableNode, keyColumns []string, composite bool) ast.Node {
	return map[bool]func() ast.Node{
		true: func() ast.Node {
			node.Constraints = append(node.Constraints, ast.NewPrimaryKeyConstraint(keyColumns...))
			return node
		},
		false: func() ast.Node { return node },
	}[composite]()
}

// tableOptions translates the table's typed options into the keys the AST's
// option map spells them with.
//
// The map is the renderer's contract and the translation belongs here rather
// than in the model: a canonical state carrying "STRICT" as a string key would
// make every consumer parse one to ask a yes-or-no question, and a renderer for
// a target with different options would still have to be taught the keys.
func tableOptions(table schemastate.Table) map[string]string {
	options := make(map[string]string)
	for key, set := range map[string]bool{
		"STRICT":        table.Strict,
		"WITHOUT_ROWID": table.WithoutRowID,
	} {
		options = withOption(options, key, set)
	}
	return options
}

// withOption records an option only when the table declares it, so a table with
// none carries an empty map rather than a map of falses a renderer has to read.
func withOption(options map[string]string, key string, set bool) map[string]string {
	return map[bool]func() map[string]string{
		false: func() map[string]string { return options },
		true: func() map[string]string {
			options[key] = "true"
			return options
		},
	}[set]()
}

// columnNodes renders one column's change against a table that already exists.
func columnNodes(change Change, profile schemastate.Profile) ([]ast.Node, error) {
	switch change.Operation {
	case Add:
		return []ast.Node{alterColumn(change, &ast.AddColumnOperation{
			Column: columnNode(*change.After.Column, false),
		})}, nil
	case Remove:
		// CASCADE, which is what the shipping planner emits: a column with a
		// dependent view or index cannot be dropped without it. The canonical
		// model's contribution to this change is that it says it destroys data
		// and cannot be undone, not that it renders a different statement.
		return []ast.Node{alterColumn(change, &ast.DropColumnOperation{
			ColumnName: change.ID.Name.Source,
			Cascade:    true,
		})}, nil
	case Modify:
		return []ast.Node{alterColumn(change, modifyColumnOperation(change, profile))}, nil
	default:
		return nil, fmt.Errorf("%s: unknown operation %q", change, change.Operation)
	}
}

// modifyColumnOperation states the new definition and what it replaces.
//
// The previous values are metadata most renderers ignore, and the two that
// cannot are why they are carried: safety analysis tells a narrowing change
// from a widening one by them, and Oracle's MODIFY states the WHOLE new column
// definition, so a cleared default has to be spelled out or the old one stays
// and the migration reports success (stokaro/ptah#1885).
func modifyColumnOperation(change Change, _ schemastate.Profile) ast.AlterOperation {
	before := *change.Before.Column
	return &ast.ModifyColumnOperation{
		Column:              columnNode(*change.After.Column, false),
		PreviousType:        before.Type,
		PreviousNullable:    before.Nullable,
		HasPreviousNullable: true,
		PreviousDefault:     before.Default,
		HasPreviousDefault:  before.HasDefault,
	}
}

// alterColumn wraps one column operation in the ALTER TABLE that carries it.
func alterColumn(change Change, operation ast.AlterOperation) ast.Node {
	return &ast.AlterTableNode{
		Name:       qualify(change.ID.Schema, change.ID.Parent),
		Operations: []ast.AlterOperation{operation},
	}
}

// columnNode renders one column definition.
//
// Type is the SOURCE spelling and never the folded one. The fold is what
// comparison decided on -- a declared `int` and a catalog `integer` are one
// type -- and writing it into DDL would put Ptah's vocabulary in the operator's
// database (ADR 0001 invariant 2).
func columnNode(column schemastate.Column, compositeKey bool) *ast.ColumnNode {
	node := &ast.ColumnNode{
		Name:     column.ID.Name.Source,
		Type:     column.Type,
		Nullable: column.Nullable,
		Primary:  column.PrimaryKey && !compositeKey,
		// A primary key already declares its column unique, and every source
		// sets both flags for one, so emitting UNIQUE beside PRIMARY KEY would
		// write a second constraint the author never asked for.
		Unique:              column.Unique && !column.PrimaryKey,
		Check:               column.Check,
		CheckName:           column.CheckName,
		GeneratedExpression: column.GeneratedExpression,
		GeneratedKind:       column.GeneratedKind,
		IdentityGeneration:  column.IdentityGeneration,
		AutoInc:             column.AutoIncrement,
	}
	return withDefault(node, column)
}

// withDefault attaches the default a column declares, in the kind it declares
// it: a literal is quoted when it is written back and an expression is not.
func withDefault(node *ast.ColumnNode, column schemastate.Column) *ast.ColumnNode {
	return map[bool]func() *ast.ColumnNode{
		false: func() *ast.ColumnNode { return node },
		true: func() *ast.ColumnNode {
			node.Default = defaultValue(column)
			return node
		},
	}[column.HasDefault]()
}

func defaultValue(column schemastate.Column) *ast.DefaultValue {
	return map[bool]*ast.DefaultValue{
		true:  {Expression: column.Default},
		false: {Value: column.Default, ValueSet: true},
	}[column.DefaultIsExpression]
}

// tableName renders a table identity for DDL, qualified the way the identity
// carries it.
func tableName(id objectidentity.ID) string {
	return qualify(id.Schema, id.Name)
}

// qualify joins a schema and an object name, omitting a schema the source did
// not write.
//
// A DEFAULTED schema is omitted for the reason [referencedTableName] omits it
// and the reason a folded type is never emitted: the target filled it in, the
// author did not write it, and putting it back writes `"public"."users"` into
// DDL somebody wrote as `users` (ADR 0001 invariant 2).
func qualify(schema, name objectidentity.Part) string {
	return map[bool]string{
		true:  name.Source,
		false: schema.Source + "." + name.Source,
	}[schema.Empty() || schema.Defaulted]
}

func addNode(change Change, profile schemastate.Profile) ast.Node {
	key := change.After.ForeignKey
	return &ast.AlterTableNode{
		Name: change.ID.Parent.Source,
		Operations: []ast.AlterOperation{
			&ast.AddConstraintOperation{
				Constraint: ast.NewForeignKeyConstraint(change.ID.Name.Source, key.Columns, &ast.ForeignKeyRef{
					Table:   referencedTableName(key.ReferencedTable, profile),
					Column:  firstOrEmpty(key.ReferencedColumns),
					Columns: key.ReferencedColumns,
					Name:    change.ID.Name.Source,
					// Source, not Normalized: emitting the folded value would
					// write `ON DELETE NO ACTION` into DDL an author wrote
					// without it (ADR 0001 invariant 2).
					OnDelete: key.OnDelete.Source,
					OnUpdate: key.OnUpdate.Source,
				}),
			},
		},
	}
}

func dropNode(change Change, profile schemastate.Profile) ast.Node {
	return &ast.AlterTableNode{
		Name: change.ID.Parent.Source,
		Operations: []ast.AlterOperation{
			&ast.DropConstraintOperation{
				ConstraintName: change.ID.Name.Source,
				// MySQL and MariaDB require the dedicated DROP FOREIGN KEY
				// spelling; the flag exists so the renderer decides rather than
				// this planner assembling dialect SQL.
				ForeignKey: true,
				IfExists:   supportsIfExists(profile),
			},
		},
	}
}

// supportsIfExists reports whether the target accepts a guarded drop.
//
// MySQL and MariaDB accept no IF EXISTS on a constraint drop at all, so a
// guarded drop there is a syntax error rather than a safer statement.
func supportsIfExists(profile schemastate.Profile) bool {
	switch platform.NormalizeDialect(profile.Dialect) {
	case platform.MySQL, platform.MariaDB:
		return false
	default:
		return true
	}
}

// referencedTableName renders the referenced table the way the source spelled
// it, qualified when the source qualified it.
//
// It emits Source and never Normalized: the folded form is what comparison
// decided on, and putting it in DDL would write Ptah's casing into the
// operator's database (ADR 0001 invariant 2).
func referencedTableName(id objectidentity.ID, profile schemastate.Profile) string {
	if id.Schema.Empty() || id.Schema.Normalized == profile.Semantics.DefaultSchema {
		return id.Name.Source
	}
	return id.Schema.Source + "." + id.Name.Source
}

func firstOrEmpty(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// Statements returns the rendered SQL of a plan, which is what a differential
// test compares against the existing path.
func Statements(operations []PlannedOperation) []string {
	out := make([]string, 0, len(operations))
	for _, operation := range operations {
		out = append(out, operation.SQL)
	}
	return out
}

// Explain renders a plan as the lines an operator reads: what changes, why, and
// what it costs.
//
// It exists because "the new path is deterministic and explainable" is a #1350
// definition-of-done item, and explainability that has no output is a claim
// nobody can check.
func Explain(operations []PlannedOperation) string {
	lines := make([]string, 0, len(operations))
	for _, operation := range operations {
		change := operation.Change
		lines = append(lines, fmt.Sprintf("%s: %s [risk %s, %s] from %s -- %s",
			change, operation.SQL, change.Risk, change.Reversibility,
			provenanceOf(change), change.Evidence))
	}
	return strings.Join(lines, "\n")
}

func provenanceOf(change Change) string {
	if change.Provenance.Location == "" {
		return string(change.Provenance.Source)
	}
	return string(change.Provenance.Source) + " " + change.Provenance.Location
}
