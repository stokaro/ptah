package schemadiff

import (
	"context"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/migration/internal/tableidentity"
	"ptah.run/migration/schemadiff/difftypes"
)

func prepareComparisonTables(ctx context.Context, desired *schemamodel.Database, current *catalog.Database,
	target string, semantics identifier.Semantics, caps capability.Capabilities, runtime schemapreparation.Runtime,
) (*schemapreparation.Capture, map[objectidentity.Key]schemapreparation.Table, error) {
	if target == "" {
		return nil, nil, nil
	}
	observed := make(map[objectidentity.Key]catalog.Table, len(current.Tables))
	for _, table := range current.Tables {
		observed[tableidentity.Subject(table.Schema, table.Name, target, semantics).Key()] = table
	}
	request := schemapreparation.Request{Target: target, Identifiers: semantics, Capabilities: caps}
	for _, table := range desired.Tables {
		subject := tableidentity.Subject(table.Schema, table.Name, target, semantics)
		captured := schemapreparation.Table{
			Subject: subject, Desired: difftypes.TableDeclarationFor(desired, table, semantics),
		}
		if found, exists := observed[subject.Key()]; exists {
			captured.Current = difftypes.TableObservationFor(current, found, target, semantics)
			captured.CurrentKnowledge.State = schemaext.Complete
		} else if current.NotDescribed.DescribesSchema(table.Schema) {
			captured.CurrentKnowledge.State = schemaext.Absent
		}
		request.Tables = append(request.Tables, captured)
	}
	source := request.Clone()
	result, err := runtime.PrepareTables(ctx, request)
	if err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	capture, err := schemapreparation.Accept(source, result)
	if err != nil {
		return nil, nil, err
	}
	prepared := make(map[objectidentity.Key]schemapreparation.Table, len(capture.Prepared))
	for _, table := range capture.Prepared {
		prepared[table.Subject.Key()] = table.Clone()
	}
	return &capture, prepared, nil
}
