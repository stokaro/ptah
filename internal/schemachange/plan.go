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

func addNode(change Change, profile schemastate.Profile) ast.Node {
	key := change.After
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
		return change.Provenance.Source
	}
	return change.Provenance.Source + " " + change.Provenance.Location
}
