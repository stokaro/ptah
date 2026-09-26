package compare

import (
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/objectidentity"
	"ptah.run/migration/schemadiff/difftypes"
)

// ObjectComments records the comment transitions of the views, sequences,
// domains, composite and range types, and extensions both sides hold.
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
	if caps.Has(capability.DomainComments) {
		changes = append(changes, domainComments(desired, database, semantics)...)
	}
	if caps.Has(capability.TypeComments) {
		changes = append(changes, compositeComments(desired, database, semantics)...)
		changes = append(changes, rangeComments(desired, database, semantics)...)
	}
	if caps.Has(capability.ExtensionComments) {
		changes = append(changes, extensionComments(desired, database, opts)...)
	}
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].Kind != changes[j].Kind {
			return changes[i].Kind < changes[j].Kind
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

func domainComments(
	desired *schemamodel.Database, database *catalog.Database, semantics identifier.Semantics,
) []difftypes.ObjectCommentChange {
	reported := make(map[objectIdentity]catalog.Domain, len(database.Domains))
	for _, domain := range database.Domains {
		reported[newObjectIdentity(objectidentity.KindDomain, domain.Schema, domain.Name, semantics)] = domain
	}
	var changes []difftypes.ObjectCommentChange
	for _, domain := range desired.Domains {
		current, exists := reported[newObjectIdentity(objectidentity.KindDomain, domain.Schema, domain.Name, semantics)]
		if !exists {
			continue
		}
		if change, changed := commentTransition(
			difftypes.CommentedDomain, domain.QualifiedName(), domain.Comment, current.Comment,
		); changed {
			changes = append(changes, change)
		}
	}
	return changes
}

func compositeComments(
	desired *schemamodel.Database, database *catalog.Database, semantics identifier.Semantics,
) []difftypes.ObjectCommentChange {
	reported := make(map[objectIdentity]catalog.CompositeType, len(database.Composites))
	for _, composite := range database.Composites {
		identity := newObjectIdentity(objectidentity.KindComposite, composite.Schema, composite.Name, semantics)
		reported[identity] = composite
	}
	var changes []difftypes.ObjectCommentChange
	for _, composite := range desired.CompositeTypes {
		identity := newObjectIdentity(objectidentity.KindComposite, composite.Schema, composite.Name, semantics)
		current, exists := reported[identity]
		if !exists {
			continue
		}
		if change, changed := commentTransition(
			difftypes.CommentedCompositeType, composite.QualifiedName(), composite.Comment, current.Comment,
		); changed {
			changes = append(changes, change)
		}
	}
	return changes
}

func rangeComments(
	desired *schemamodel.Database, database *catalog.Database, semantics identifier.Semantics,
) []difftypes.ObjectCommentChange {
	reported := make(map[objectIdentity]catalog.Range, len(database.Ranges))
	for _, rangeType := range database.Ranges {
		reported[newObjectIdentity(objectidentity.KindRange, rangeType.Schema, rangeType.Name, semantics)] = rangeType
	}
	var changes []difftypes.ObjectCommentChange
	for _, rangeType := range desired.Ranges {
		identity := newObjectIdentity(objectidentity.KindRange, rangeType.Schema, rangeType.Name, semantics)
		current, exists := reported[identity]
		if !exists {
			continue
		}
		if change, changed := commentTransition(
			difftypes.CommentedRangeType, rangeType.QualifiedName(), rangeType.Comment, current.Comment,
		); changed {
			changes = append(changes, change)
		}
	}
	return changes
}

func extensionComments(
	desired *schemamodel.Database, database *catalog.Database, opts *config.CompareOptions,
) []difftypes.ObjectCommentChange {
	reported := make(map[string]catalog.Extension, len(database.Extensions))
	for _, extension := range database.Extensions {
		reported[extension.Name] = extension
	}
	var changes []difftypes.ObjectCommentChange
	for _, extension := range desired.Extensions {
		current, exists := reported[extension.Name]
		if !exists || strings.TrimSpace(extension.Comment) == "" || opts.IsExtensionIgnored(extension.Name) {
			continue
		}
		var currentComment string
		if current.Comment != nil {
			currentComment = *current.Comment
		}
		if change, changed := commentTransition(
			difftypes.CommentedExtension, extension.Name, extension.Comment, currentComment,
		); changed {
			changes = append(changes, change)
		}
	}
	return changes
}
