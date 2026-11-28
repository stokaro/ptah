package schemadiff

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/identifiervalidation"
)

// namespaceObject is an object of a shared namespace in a schema, as
// [refuseCreatesBesideKeptNamesakes] reads it: the noun that names its kind,
// its name with the schema written out, the table a constraint or a trigger
// belongs to, and the keys the target's rules compare its name by.
type namespaceObject struct {
	noun     string
	name     string
	host     string
	namesake identifiervalidation.Namesake
}

// phrase is how a message names the object: `view dbo.REPORT`, or
// `constraint dbo.Report on dbo.t1`.
func (o namespaceObject) phrase() string {
	if o.host == "" {
		return o.noun + " " + o.name
	}
	return o.noun + " " + o.name + " on " + o.host
}

// refuseCreatesBesideKeptNamesakes refuses a diff that creates an object whose
// name the target may read as the name of a constraint that the database
// holds and the diff does not drop, where the target keeps constraints and the
// created kind in one namespace in a schema
// ([ptah.run/core/platform/descriptor.Comparison.SharedObjectNamespace]).
//
// The declaration check compares only what the schema declares, and a
// constraint the schema leaves unnamed is paired with the one the database
// holds, which keeps the name the server holds it under. Measured on SQL
// Server 2025 under a case-insensitive collation: on a database whose table
// dbo.t1 holds the primary key Report, the declaration of that table with an
// unnamed primary key beside the view dbo.REPORT plans CREATE VIEW
// [dbo].[REPORT], which fails with error 2714 (stokaro/ptah#6767). A constraint
// the diff drops, alone or with its table, is the planner's to judge, beside
// the other objects a plan drops. A constraint the diff creates is compared
// with the kept constraints too, since one the database keeps is never one
// the plan creates: measured on SQL Server 2022, the primary key pk of a
// created dbo.t2 beside the primary key pk of dbo.t1 fails with error 2714. A trigger is always declared by its name, so the one
// the database keeps is the declaration check's.
func refuseCreatesBesideKeptNamesakes(diff *difftypes.SchemaDiff, current *catalog.Database, target string, semantics identifier.Semantics) error {
	described, _ := platform.Lookup(target)
	namespace := described.Comparison.SharedObjectNamespace
	if diff == nil || current == nil || !namespace.Constraints {
		return nil
	}
	conflict := indexscope.ConflictSemantics(semantics)
	object := func(noun, name, host string) namespaceObject {
		return namespaceObject{
			noun: noun, name: name, host: host,
			namesake: identifiervalidation.NamesakeOf(name, conflict),
		}
	}
	hosted := func(noun string, hosted identifiervalidation.Hosted) namespaceObject {
		return object(noun, hosted.QualifiedName(semantics.DefaultSchema), qualifiedObjectName(hosted.Table, semantics.DefaultSchema))
	}
	dropped := make(map[string]bool, len(diff.ConstraintsRemoved))
	for _, removed := range diff.ConstraintsRemoved {
		dropped[identifiervalidation.Hosted{Name: removed.Name, Table: removed.TableName}.QualifiedName(semantics.DefaultSchema)] = true
	}
	var kept []namespaceObject
	for _, constraint := range current.Constraints {
		candidate := hosted("constraint", identifiervalidation.Hosted{
			Name: constraint.Name, Table: tableref.Canonical(constraint.Schema, constraint.TableName),
		})
		if !dropped[candidate.name] {
			kept = append(kept, candidate)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	var created []namespaceObject
	add := func(shared bool, noun string, names ...string) {
		for _, name := range names {
			if shared {
				created = append(created, object(noun, qualifiedObjectName(name, semantics.DefaultSchema), ""))
			}
		}
	}
	add(namespace.Tables, "table", diff.TablesAdded.Names()...)
	add(namespace.Views, "view", diff.ViewsAdded.Names()...)
	add(namespace.MaterializedViews, "materialized view", diff.MaterializedViewsAdded.Names()...)
	add(namespace.Sequences, "sequence", diff.SequencesAdded.Names()...)
	for _, routine := range diff.FunctionsAdded {
		noun := "function"
		if routine.IsProcedure() {
			noun = "procedure"
		}
		add(namespace.Routines, noun, routine.Name)
	}
	for _, trigger := range diff.TriggersAdded {
		if namespace.Triggers {
			created = append(created, hosted("trigger", identifiervalidation.HostedTrigger(trigger.TriggerName, trigger.TableName)))
		}
	}
	for _, constraint := range identifiervalidation.CreatedConstraints(diff) {
		created = append(created, hosted("constraint", constraint))
	}
	for _, making := range created {
		for _, holding := range kept {
			if making.namesake.MayBe(holding.namesake) {
				return keptNamesakeError(making, holding, target)
			}
		}
	}
	return nil
}

// keptNamesakeError names an object a plan creates and a constraint the
// database keeps that target reads, or may read, as one name in one
// namespace.
func keptNamesakeError(created, kept namespaceObject, target string) error {
	reading := "which it may read as one name"
	if created.name == kept.name {
		reading = "which are one name"
	}
	return &RefusalError{cause: fmt.Errorf(
		"%w: the plan creates %s, and the database keeps %s, %s; %s, "+
			"so the create would fail while the %s exists; give the %s another name, or rename or drop the %s first",
		ptaherr.ErrInvalidSchemaDiff, created.phrase(), kept.phrase(), reading,
		identifiervalidation.NamespaceClause(created.noun, kept.noun, target), kept.noun, created.noun, kept.noun,
	)}
}

// qualifiedObjectName is name with the schema it is in written out, as a
// refusal names it: `t1` is dbo.t1 where the default schema is dbo.
func qualifiedObjectName(name, defaultSchema string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return name
	}
	if !ref.Qualified {
		ref.Schema = defaultSchema
	}
	return tableref.Canonical(ref.Schema, ref.Name)
}
