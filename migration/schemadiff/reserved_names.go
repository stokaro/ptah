package schemadiff

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/featureselect"
	"ptah.run/internal/foreignkeyscope"
)

// Reservations are observations of occupied names, not hidden desired state.
// Refuse explicit intent before preparation can turn it into a create or alter.
func refuseReservedNames(desired *schemamodel.Database, current *catalog.Database, target string, semantics identifier.Semantics) error {
	if len(current.ReservedNames) == 0 {
		return nil
	}
	builder := objectidentity.NewBuilder(semantics)
	reserved, err := reservedNamesIndex(current, target, semantics)
	if err != nil {
		return err
	}
	check := func(ref objectidentity.ID) error {
		if record, exists := reserved[ref.Key()]; exists {
			return reservedNameError(record)
		}
		return nil
	}
	for _, table := range desired.Tables {
		if err := check(builder.TableParts(table.Schema, table.Name)); err != nil {
			return err
		}
	}
	for _, table := range current.Tables {
		if err := check(builder.TablePartsVerbatim(table.Schema, table.Name)); err != nil {
			return err
		}
	}
	for _, ref := range append(desired.FeatureObjects.Refs(), current.FeatureObjects.Refs()...) {
		if err := check(ref); err != nil {
			return err
		}
	}
	for _, reference := range foreignkeyscope.References(*desired) {
		if err := check(builder.Table(reference.Table)); err != nil {
			return err
		}
	}
	return nil
}

func reservedNamesIndex(current *catalog.Database, target string, semantics identifier.Semantics) (map[objectidentity.Key]catalog.ReservedName, error) {
	builder := objectidentity.NewBuilder(semantics)
	reserved := make(map[objectidentity.Key]catalog.ReservedName, len(current.ReservedNames))
	for _, record := range current.ReservedNames {
		if record.ProviderID == "" || record.Engine != target || record.Identity.Name.Source == "" || record.Identity.Name.Normalized == "" || record.Identity.Kind == "" {
			return nil, fmt.Errorf("%w: invalid provider storage reservation", ptaherr.ErrInvalidSchemaDiff)
		}
		switch record.Purpose {
		case "history", "lock", "log", "tag":
		default:
			return nil, fmt.Errorf("%w: unknown provider storage purpose", ptaherr.ErrInvalidSchemaDiff)
		}
		if record.Identity.Kind == objectidentity.KindTable && record.Identity.Key() != builder.TablePartsVerbatim(record.Identity.Schema.Source, record.Identity.Name.Source).Key() {
			return nil, fmt.Errorf("%w: reserved table normalization differs from comparison", ptaherr.ErrInvalidSchemaDiff)
		}
		if _, exists := reserved[record.Identity.Key()]; exists {
			return nil, fmt.Errorf("%w: duplicate provider storage reservation", ptaherr.ErrInvalidSchemaDiff)
		}
		reserved[record.Identity.Key()] = record
	}
	return reserved, nil
}

func refuseComparisonChanges(ctx context.Context, runtime any, target string, semantics identifier.Semantics, caps capability.Capabilities,
	desired schemaext.FeatureState, current *catalog.Database, diff *difftypes.SchemaDiff,
) error {
	if err := refuseReservedChanges(ctx, runtime, target, semantics, desired, current, diff); err != nil {
		return err
	}
	return refuseDroppedBindings(ctx, runtime, target, semantics, caps, desired, diff)
}

func reservedNameError(record catalog.ReservedName) error {
	return fmt.Errorf("%w: %s is reserved for %s storage by provider %q", ptaherr.ErrInvalidSchemaDiff, record.Identity, record.Purpose, record.ProviderID)
}

func refuseReservedChanges(ctx context.Context, runtime any, target string, semantics identifier.Semantics,
	desired schemaext.FeatureState, current *catalog.Database, diff *difftypes.SchemaDiff,
) error {
	if len(current.ReservedNames) == 0 {
		return ctx.Err()
	}
	for _, removed := range diff.SchemasRemoved {
		for _, record := range current.ReservedNames {
			namespace := record.Identity.Schema.Normalized
			if namespace == "" {
				namespace = record.Identity.Database.Normalized
			}
			if semantics.TableIdentityKey(removed) == namespace {
				return reservedNameError(record)
			}
		}
	}
	if desired.Objects.Len() == 0 {
		return ctx.Err()
	}
	relations, _ := runtime.(featureselect.RelationRuntime)
	bindings, err := featureselect.CaptureBindings(ctx, relations, target, semantics,
		featureselect.Side{Representation: schemaext.Desired, Objects: desired.Objects, Coverage: desired.Coverage})
	if err != nil {
		return err
	}
	reserved := make(map[objectidentity.Key]catalog.ReservedName, len(current.ReservedNames))
	for _, record := range current.ReservedNames {
		reserved[record.Identity.Key()] = record
	}
	for _, bound := range bindings.Objects() {
		if record, exists := reserved[bound.Attachment.Key()]; exists {
			return reservedNameError(record)
		}
		if bound.Incomplete != "" {
			return fmt.Errorf("%w: cannot exclude references to reserved provider storage: %s", ptaherr.ErrInvalidSchemaDiff, bound.Incomplete)
		}
		for _, reference := range bound.References {
			if reference.Kind == objectidentity.KindColumn {
				reference.Kind, reference.Name, reference.Parent = objectidentity.KindTable, reference.Parent, objectidentity.Part{}
			}
			if record, exists := reserved[reference.Key()]; exists {
				return reservedNameError(record)
			}
		}
	}
	return ctx.Err()
}
