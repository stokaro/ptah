package compare

import (
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// ObjectComments records the comment transitions of the views, materialized
// views, sequences, domains, composite, range and enum types, extensions,
// functions, procedures, triggers, policies, schemas and roles both sides
// hold.
//
// A kind is compared only where caps says the target stores its comment and
// reads it back. Everywhere else the reader reports no comment whatever was
// written, so a declared one would be a difference no plan can close, planned
// again on every run (stokaro/ptah#3627).
//
// An object only one side holds is not compared: a created object takes its
// comment from the statement that creates it, and a dropped one takes its
// comment with it.
//
// An extension's comment is compared only when the declaration states one.
// CREATE EXTENSION gives the extension the comment its control file carries,
// so an extension declared without a comment has one the moment it exists,
// and reading the declaration as a request to remove it would plan that
// removal against every extension. The version follows the same rule.
func ObjectComments(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	opts *config.CompareOptions,
	caps capability.Capabilities,
	semantics identifier.Semantics,
) {
	var changes []difftypes.ObjectCommentChange
	if caps.Has(capability.ViewComments) {
		changes = append(changes, viewComments(desired, database, semantics)...)
	}
	if caps.Has(capability.SequenceComments) {
		changes = append(changes, sequenceComments(desired, database, semantics)...)
	}
	if caps.Has(capability.TypeComments) {
		changes = append(changes, enumComments(desired, database, semantics)...)
	}
	if caps.Has(capability.MaterializedViewComments) {
		changes = append(changes, materializedViewComments(desired, database, semantics)...)
	}
	changes = append(changes, routineComments(desired, database, opts.Dialect, semantics, caps)...)
	if caps.Has(capability.TriggerComments) {
		changes = append(changes, triggerComments(desired, database, semantics)...)
	}
	if caps.Has(capability.PolicyComments) {
		changes = append(changes, policyComments(desired, database, semantics)...)
	}
	if caps.Has(capability.SchemaComments) {
		changes = append(changes, schemaComments(desired, database, semantics)...)
	}
	if caps.Has(capability.RoleComments) {
		changes = append(changes, roleComments(desired, database)...)
	}
	sort.SliceStable(changes, func(i, j int) bool {
		if changes[i].Kind != changes[j].Kind {
			return changes[i].Kind < changes[j].Kind
		}
		if changes[i].Table != changes[j].Table {
			return changes[i].Table < changes[j].Table
		}
		return changes[i].Name < changes[j].Name
	})
	diff.ObjectCommentsChanged = changes
}

// commentTransition answers the change between two comments, or false where
// they agree. Surrounding whitespace is not part of a comment: the model trims
// it on the way in, and a server stores what it is given.
func commentTransition(
	kind difftypes.CommentedObjectKind, name, desired, current string,
) (difftypes.ObjectCommentChange, bool) {
	desired = strings.TrimSpace(desired)
	current = strings.TrimSpace(current)
	if desired == current {
		return difftypes.ObjectCommentChange{}, false
	}
	return difftypes.ObjectCommentChange{Kind: kind, Name: name, Current: current, Desired: desired}, true
}

// schemaComments compares the comment of each schema the declaration names
// and the read reports. A read reports the schemas it was asked to read, so a
// declared schema it was not asked about is not compared, and one the plan
// creates takes its comment from CREATE SCHEMA.
//
// A schema's comment is compared only when the declaration states one, as an
// extension's is. A declaration that names a schema says little about its
// comment: an Atlas HCL document has to declare `schema "public" {}` before
// any of its tables can name the schema, and PostgreSQL creates every
// database with `public` commented "standard public schema". Read as a
// request to remove that comment, the block planned COMMENT ON SCHEMA "public"
// IS NULL against every PostgreSQL database. A comment the declaration states
// is set; one it leaves out is left as the server holds it.
func schemaComments(
	desired *schemamodel.Database, database *catalog.Database, semantics identifier.Semantics,
) []difftypes.ObjectCommentChange {
	reported := make(map[string]catalog.Schema, len(database.Schemas))
	for _, schema := range database.Schemas {
		reported[semantics.TableIdentityKey(schema.Name)] = schema
	}
	var changes []difftypes.ObjectCommentChange
	for _, schema := range desired.Schemas {
		current, exists := reported[semantics.TableIdentityKey(schema.Name)]
		if !exists || strings.TrimSpace(schema.Comment) == "" {
			continue
		}
		if change, changed := commentTransition(
			difftypes.CommentedSchema, schema.Name, schema.Comment, current.Comment,
		); changed {
			changes = append(changes, change)
		}
	}
	return changes
}

// roleComments compares the comment of each declared role the server has. A
// role is matched the way [Roles] matches it, against every role the reader
// manages whether or not its description named the role, so the two never
// disagree about which roles exist.
func roleComments(desired *schemamodel.Database, database *catalog.Database) []difftypes.ObjectCommentChange {
	reported := managedRoles(database)
	var changes []difftypes.ObjectCommentChange
	for _, role := range desired.Roles {
		current, exists := reported[role.Name]
		if !exists {
			continue
		}
		if change, changed := commentTransition(
			difftypes.CommentedRole, role.Name, role.Comment, current.Comment,
		); changed {
			changes = append(changes, change)
		}
	}
	return changes
}

func viewComments(
	desired *schemamodel.Database, database *catalog.Database, semantics identifier.Semantics,
) []difftypes.ObjectCommentChange {
	reported := make(map[objectIdentity]catalog.View, len(database.Views))
	for _, view := range database.Views {
		reported[newObjectIdentity(objectidentity.KindView, view.Schema, view.Name, semantics)] = view
	}
	var changes []difftypes.ObjectCommentChange
	for _, view := range desired.Views {
		current, exists := reported[newQualifiedObjectIdentity(objectidentity.KindView, view.Name, semantics)]
		if !exists {
			continue
		}
		if change, changed := commentTransition(
			difftypes.CommentedView, view.Name, view.Comment, current.Comment,
		); changed {
			changes = append(changes, change)
		}
	}
	return changes
}

func sequenceComments(
	desired *schemamodel.Database, database *catalog.Database, semantics identifier.Semantics,
) []difftypes.ObjectCommentChange {
	reported := make(map[objectIdentity]catalog.Sequence, len(database.Sequences))
	for _, sequence := range database.Sequences {
		reported[newObjectIdentity(objectidentity.KindSequence, sequence.Schema, sequence.Name, semantics)] = sequence
	}
	var changes []difftypes.ObjectCommentChange
	for _, sequence := range desired.Sequences {
		identity := newObjectIdentity(objectidentity.KindSequence, sequence.Schema, sequence.Name, semantics)
		current, exists := reported[identity]
		if !exists {
			continue
		}
		if change, changed := commentTransition(
			difftypes.CommentedSequence, sequence.QualifiedName(), sequence.Comment, current.Comment,
		); changed {
			changes = append(changes, change)
		}
	}
	return changes
}

func enumComments(
	desired *schemamodel.Database, database *catalog.Database, semantics identifier.Semantics,
) []difftypes.ObjectCommentChange {
	dbByIdentity := make(map[objectIdentity]catalog.Enum, len(database.Enums))
	dbByName := make(map[string][]catalog.Enum, len(database.Enums))
	for _, enum := range database.Enums {
		schema, name := enumParts(enum.Schema, enum.Name)
		dbByIdentity[newObjectIdentity(objectidentity.KindEnum, schema, name, semantics)] = enum
		dbByName[semantics.TableIdentityKey(name)] = append(dbByName[semantics.TableIdentityKey(name)], enum)
	}
	var changes []difftypes.ObjectCommentChange
	for _, enum := range desired.Enums {
		current, exists := findDatabaseEnum(enum, dbByIdentity, dbByName, semantics)
		if !exists {
			continue
		}
		if change, changed := commentTransition(
			difftypes.CommentedEnumType, enum.QualifiedName(), enum.Comment, current.Comment,
		); changed {
			changes = append(changes, change)
		}
	}
	return changes
}

func materializedViewComments(
	desired *schemamodel.Database, database *catalog.Database, semantics identifier.Semantics,
) []difftypes.ObjectCommentChange {
	reported := make(map[objectIdentity]catalog.MaterializedView, len(database.MatViews))
	for _, view := range database.MatViews {
		reported[newObjectIdentity(objectidentity.KindMatView, view.Schema, view.Name, semantics)] = view
	}
	var changes []difftypes.ObjectCommentChange
	for _, view := range desired.MaterializedViews {
		current, exists := reported[newQualifiedObjectIdentity(objectidentity.KindMatView, view.Name, semantics)]
		if !exists {
			continue
		}
		if change, changed := commentTransition(
			difftypes.CommentedMatView, view.Name, view.Comment, current.Comment,
		); changed {
			changes = append(changes, change)
		}
	}
	return changes
}

// routineComments records the comment transitions of the functions and
// procedures a definition comparison pairs, each kind where the target
// stores its comment. The argument list is the one the database records,
// which is what addresses one overload among several.
func routineComments(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
	caps capability.Capabilities,
) []difftypes.ObjectCommentChange {
	functions, procedures := caps.Has(capability.FunctionComments), caps.Has(capability.ProcedureComments)
	if !functions && !procedures {
		return nil
	}
	groups := groupRoutines(desired, database, dialect, semantics.Normalize(""))
	var changes []difftypes.ObjectCommentChange
	for identity, declared := range groups.declared {
		pairs, _, _ := pairRoutineOverloads(declared, groups.recorded[identity])
		for _, pair := range pairs {
			kind, stored := difftypes.CommentedFunction, functions
			if identity.Kind() == objectidentity.KindProcedure {
				kind, stored = difftypes.CommentedProcedure, procedures
			}
			if !stored {
				continue
			}
			change, changed := commentTransition(kind, pair.declared.Name, pair.declared.Comment, pair.recorded.Comment)
			if !changed {
				continue
			}
			change.Arguments = pair.recorded.DropIdentity()
			changes = append(changes, change)
		}
	}
	return changes
}

func triggerComments(
	desired *schemamodel.Database, database *catalog.Database, semantics identifier.Semantics,
) []difftypes.ObjectCommentChange {
	matched, _ := pairTriggers(desired, database, semantics.Normalize(""))
	var changes []difftypes.ObjectCommentChange
	for position, declared := range desired.Triggers {
		if matched[position] < 0 {
			continue
		}
		change, changed := commentTransition(
			difftypes.CommentedTrigger, declared.Name, declared.Comment, database.Triggers[matched[position]].Comment,
		)
		if !changed {
			continue
		}
		change.Table = declared.Table
		changes = append(changes, change)
	}
	return changes
}

func policyComments(
	desired *schemamodel.Database, database *catalog.Database, semantics identifier.Semantics,
) []difftypes.ObjectCommentChange {
	reported := make(map[tableMemberKey]catalog.RLSPolicy, len(database.RLSPolicies))
	for _, policy := range database.RLSPolicies {
		reported[newTableMemberKey(policy.Table, policy.Name, semantics)] = policy
	}
	var changes []difftypes.ObjectCommentChange
	for _, policy := range desired.RLSPolicies {
		current, exists := reported[newTableMemberKey(policy.Table, policy.Name, semantics)]
		if !exists {
			continue
		}
		change, changed := commentTransition(difftypes.CommentedPolicy, policy.Name, policy.Comment, current.Comment)
		if !changed {
			continue
		}
		change.Table = policy.Table
		changes = append(changes, change)
	}
	return changes
}
