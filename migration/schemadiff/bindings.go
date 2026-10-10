package schemadiff

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/featureselect"
	"ptah.run/migration/schemadiff/difftypes"
)

// refuseDroppedBindings refuses a comparison whose plan drops a table, a
// column or a function that an object of the effective desired feature state
// still binds, as its owner's relation discovery reports it (ADR 0020). The
// effective state holds the declared objects, the observed ones a description
// could not express, and every object whose comparison stayed undecided, so an
// unchanged object counts as much as a changed one.
//
// Dropping a table does not authorize dropping the other bindings of an
// object that binds it: the owner plans the change that removes the binding,
// which leaves the object no longer binding the table here, or the comparison
// refuses. SQL Server refuses DROP TABLE, DROP COLUMN and DROP FUNCTION while a
// security policy binds the object (Msg 3729 and 5074, measured on SQL Server
// 2025), and a policy without schema binding lets a column go and then fails
// every read of its table.
//
// An object whose owner could not list every reference, such as a predicate
// argument that calls functions of its own, is refused when the plan drops or
// replaces any function, since it may call that one. Function changes count
// only on a target whose plans write them: one without capability.Functions,
// such as SQL Server, leaves its routines to their author and drops none.
func refuseDroppedBindings(ctx context.Context, runtime any, target string, semantics identifier.Semantics, caps capability.Capabilities,
	desired schemaext.FeatureState, diff *difftypes.SchemaDiff,
) error {
	dropped, functions := droppedReferences(diff, semantics, caps)
	if target == "" || len(dropped) == 0 {
		return ctx.Err()
	}
	relations, _ := runtime.(featureselect.RelationRuntime)
	bindings, err := featureselect.CaptureBindings(ctx, relations, target, semantics,
		featureselect.Side{Representation: schemaext.Desired, Objects: desired.Objects, Coverage: desired.Coverage})
	if err != nil {
		return err
	}
	var problems []error
	for _, bound := range bindings.Objects() {
		name := fmt.Sprintf("%s %s.%s", bound.Object.Kind, bound.Object.Schema.Source, bound.Object.Name.Source)
		for _, reference := range bound.References {
			if target, found := dropped[referenceKey(reference)]; found {
				problems = append(problems, fmt.Errorf("%w: %s binds %s %s, which the plan %s; change it so it no longer binds the %s, "+
					"or keep the %s", ptaherr.ErrInvalidSchemaDiff, name, reference.Kind, target.name, target.verb, reference.Kind, reference.Kind))
			}
		}
		if bound.Incomplete != "" && len(functions) != 0 {
			problems = append(problems, fmt.Errorf("%w: %s may reference functions its owner cannot list (%s), and the plan drops or "+
				"replaces %s; apply the function change in a migration of its own", ptaherr.ErrInvalidSchemaDiff, name, bound.Incomplete,
				strings.Join(functions, ", ")))
		}
	}
	if len(problems) == 0 {
		return ctx.Err()
	}
	return &RefusalError{cause: errors.Join(problems...)}
}

// droppedTarget is an object a plan drops or replaces, as a refusal names it.
type droppedTarget struct {
	name, verb string
}

// droppedReferences names the tables, columns and functions a diff drops or
// replaces, keyed as relation discovery reports them, and the functions among
// them. Functions count only when the target's plans write routines.
func droppedReferences(diff *difftypes.SchemaDiff, semantics identifier.Semantics, caps capability.Capabilities) (map[objectidentity.Key]droppedTarget, []string) {
	builder := objectidentity.NewBuilder(semantics)
	dropped := make(map[objectidentity.Key]droppedTarget)
	for _, removal := range diff.TablesRemoved {
		if !removal.Current.HasTable() {
			continue
		}
		table := builder.TableParts(removal.Current.Table.Schema, removal.Current.Table.Name)
		dropped[referenceKey(table)] = droppedTarget{name: qualified(table), verb: "drops"}
	}
	for _, table := range diff.TablesModified {
		if !table.Current.HasTable() {
			continue
		}
		owner := builder.TableParts(table.Current.Table.Schema, table.Current.Table.Name)
		for _, column := range table.ColumnsRemoved {
			ref := builder.ColumnParts(table.Current.Table.Schema, table.Current.Table.Name, column.Name)
			dropped[referenceKey(ref)] = droppedTarget{name: qualified(owner) + "." + column.Name, verb: "drops"}
		}
	}
	var functions []string
	if !caps.Has(capability.Functions) {
		return dropped, functions
	}
	addFunction := func(name, verb string) {
		ref := builder.Function(name, "")
		dropped[referenceKey(ref)] = droppedTarget{name: qualified(ref), verb: verb}
		functions = append(functions, qualified(ref))
	}
	for _, function := range diff.FunctionsRemoved {
		addFunction(function.Name, "drops")
	}
	for _, function := range diff.FunctionsModified {
		addFunction(function.FunctionName, "replaces")
	}
	return dropped, functions
}

// referenceKey is a reference's identity without a routine signature, which
// relation discovery does not report and a removal may carry.
func referenceKey(ref objectidentity.ID) objectidentity.Key {
	ref.Signature = ""
	return ref.Key()
}

func qualified(ref objectidentity.ID) string {
	if ref.Schema.Source == "" {
		return ref.Name.Source
	}
	return ref.Schema.Source + "." + ref.Name.Source
}
