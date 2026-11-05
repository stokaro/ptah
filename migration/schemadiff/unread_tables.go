package schemadiff

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/foreignkeyscope"
)

// refuseUnreadTables refuses a schema that declares a table at a table the
// read found and did not describe, or a foreign key that references one, in
// the reader's words. Such a table exists, so without this the comparison
// takes it for one the database lacks and plans a CREATE TABLE the database
// rejects when it runs. It runs before preparation, beside the reserved
// names, and changes nothing else: an unread table is not one the plan
// drops or alters.
//
// The refusal is the error the built-in YDB engine gives for its replica
// tables: a capability error for the target, which reads as the table and the
// reason alone.
func refuseUnreadTables(desired *schemamodel.Database, current *catalog.Database, target string, semantics identifier.Semantics) error {
	if len(current.UnreadTables) == 0 {
		return nil
	}
	builder := objectidentity.NewBuilder(semantics)
	unread := make(map[objectidentity.Key]catalog.UnreadTable, len(current.UnreadTables))
	for _, table := range current.UnreadTables {
		if table.Identity.Kind != objectidentity.KindTable || table.Reason == "" ||
			table.Identity.Key() != builder.TablePartsVerbatim(table.Identity.Schema.Source, table.Identity.Name.Source).Key() {
			return fmt.Errorf("%w: invalid unread table", ptaherr.ErrInvalidSchemaDiff)
		}
		unread[table.Identity.Key()] = table
	}
	check := func(ref objectidentity.ID) error {
		if table, found := unread[ref.Key()]; found {
			subject := table.Identity.String()
			return &ptaherr.CapabilityError{Dialect: target, Feature: subject, Err: ptaherr.ErrUnsupportedFeature, Message: subject + ": " + table.Reason}
		}
		return nil
	}
	for _, table := range desired.Tables {
		if err := check(builder.TableParts(table.Schema, table.Name)); err != nil {
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
