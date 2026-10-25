package providerhost

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
)

func (t *target) validateOwnedSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	profile, err := (modelComparer{target: t}).request(request.Target, request.Identifiers, request.Capabilities, nil)
	if err != nil {
		return schemavalidation.Result{}, err
	}
	profile.TargetFeatures = featureSet(t.engine.GetFeatures())
	capture := planningCapture{comparisonCapture: comparisonCapture{target: t, request: profile}, identifiers: request.Identifiers}
	remote, err := capture.validation(request.Schema)
	if err != nil {
		return schemavalidation.Result{}, err
	}
	remote.NoSkipped = request.NoSkipped
	checked, err := t.host.client.ValidateSchema(ctx, remote)
	if err != nil {
		return schemavalidation.Result{}, err
	}
	result := schemavalidation.Result{Complete: true}
	for _, diagnostic := range checked.Diagnostics {
		code, err := schemaDiagnosticCode(diagnostic.Code)
		if err != nil {
			return schemavalidation.Result{}, err
		}
		result.Diagnostics = append(result.Diagnostics, schemavalidation.Diagnostic{Code: code, Kind: diagnostic.Kind, Object: diagnostic.Object, Feature: diagnostic.Feature, Message: diagnostic.Message})
	}
	if err := ctx.Err(); err != nil {
		return schemavalidation.Result{}, err
	}
	return result, nil
}

func schemaDiagnosticCode(code protocol.SchemaDiagnostic_Code) (schemavalidation.Code, error) {
	switch code {
	case protocol.SchemaDiagnostic_INVALID_SCHEMA:
		return schemavalidation.InvalidSchema, nil
	case protocol.SchemaDiagnostic_UNSUPPORTED_FEATURE:
		return schemavalidation.UnsupportedFeature, nil
	case protocol.SchemaDiagnostic_OMITTED_DECLARATION:
		return schemavalidation.OmittedDeclaration, nil
	default:
		return "", fmt.Errorf("%w: unknown provider schema diagnostic", schemavalidation.ErrInvalidResult)
	}
}

func (c *planningCapture) validation(source *schemamodel.Database) (*protocol.ValidateSchemaRequest, error) {
	if source == nil {
		return nil, fmt.Errorf("provider validation requires a schema")
	}
	// Declared rows are not schema: the provider types and writes them through
	// the data services, so validation leaves ManagedData to them.
	if err := onlyFields(*source, "Tables", "Fields", "Indexes", "Constraints", "Enums", "Triggers", "Schemas", "FeatureObjects", "FeatureCoverage", "NotDescribed", "Dependencies", "EmbeddedFields", "EmbeddedSources", "ManagedData"); err != nil {
		return nil, err
	}
	if err := validateCapturedChildren(source); err != nil {
		return nil, err
	}
	remote := &protocol.ValidateSchemaRequest{Engine: c.request.Engine, Identifiers: c.request.Identifiers, Capabilities: c.request.Capabilities}
	builder := objectidentity.NewBuilder(c.identifiers)
	for _, table := range source.Tables {
		subject := builder.TableParts(table.Schema, table.Name)
		declaration, err := c.declaration(subject, schemacapture.DeclareTable(source, table, c.identifiers))
		if err != nil {
			return nil, err
		}
		remote.Tables = append(remote.Tables, &protocol.ValidationTable{Subject: comparisonIdentity(subject), Declaration: declaration})
	}
	var facets []schemaext.FacetRecord
	for _, namespace := range source.Schemas {
		if err := onlyFields(namespace, "Name", "Comment", "Charset", "Collate", "Facets"); err != nil {
			return nil, err
		}
		subject := objectidentity.ID{Kind: objectidentity.KindSchema, Name: builder.TableParts("", namespace.Name).Name}
		remote.Namespaces = append(remote.Namespaces, &protocol.DeclaredNamespace{Subject: comparisonIdentity(subject), Name: namespace.Name, Comment: namespace.Comment, Charset: namespace.Charset, Collation: namespace.Collate})
		if err := addPlanningFacet(&facets, subject, namespace.Facets); err != nil {
			return nil, err
		}
	}
	var err error
	remote.Enums, err = mapPlanningDefinitions(source.Enums, declaredPlanningEnum)
	if err != nil {
		return nil, err
	}
	// Whole-schema feature state includes standalone objects and table-owned
	// children. Table captures retain their own slots for local interpretation.
	remote.Features, err = c.features(schemaext.Desired, source.FeatureObjects, source.FeatureCoverage, facets)
	if err != nil {
		return nil, err
	}
	for _, limit := range source.NotDescribed.Objects {
		remote.NotDescribed = append(remote.NotDescribed, &protocol.SchemaCoverageLimit{Kind: string(limit.Kind), Name: limit.Name, Reason: string(limit.Reason), Provenance: string(limit.Provenance)})
	}
	remote.Dependencies = make(map[string]*protocol.SchemaDependencies, len(source.Dependencies))
	for name, dependencies := range source.Dependencies {
		remote.Dependencies[name] = &protocol.SchemaDependencies{Tables: slices.Clone(dependencies)}
	}
	remote.TargetFeatures = slices.Clone(c.request.TargetFeatures)
	return remote, nil
}

// A table-only traversal can lose orphaned or ambiguously attached children.
// Require exactly one owner for every source child before reporting validation.
func validateCapturedChildren(source *schemamodel.Database) error {
	for _, field := range schemamodel.ProcessEmbeddedFields(source.EmbeddedFields, source.Fields) {
		if err := requireValidationOwner(source.Tables, func(table schemamodel.Table) bool { return field.StructName == table.StructName }); err != nil {
			return err
		}
	}
	for _, constraint := range source.Constraints {
		if err := requireValidationOwner(source.Tables, func(table schemamodel.Table) bool { return schemacapture.Owns(table, constraint) }); err != nil {
			return err
		}
	}
	for _, index := range source.Indexes {
		owner := schemamodel.Constraint{StructName: index.StructName, Table: index.TableName}
		if err := requireValidationOwner(source.Tables, func(table schemamodel.Table) bool { return schemacapture.Owns(table, owner) }); err != nil {
			return err
		}
	}
	for _, trigger := range source.Triggers {
		if err := requireValidationOwner(source.Tables, func(table schemamodel.Table) bool {
			return trigger.Table == table.Name || trigger.Table == table.QualifiedName()
		}); err != nil {
			return err
		}
	}
	return nil
}

func requireValidationOwner(tables []schemamodel.Table, owns func(schemamodel.Table) bool) error {
	count := 0
	for _, table := range tables {
		if owns(table) {
			count++
		}
	}
	if count != 1 {
		return unsupported("schema validation requires one owner per declared child")
	}
	return nil
}
