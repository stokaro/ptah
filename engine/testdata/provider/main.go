// This program is copied into an independent module by the external-provider
// test. All Ptah imports are public contracts, and no built-in target is linked.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing/fstest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/migration/importer"
)

type widget struct {
	Name   string   `json:"name"`
	Levels []string `json:"levels"`
}

func (*widget) Kind() schemaext.Kind { return "example.org/widget/model" }
func (v *widget) Clone() schemaext.Value {
	return &widget{Name: v.Name, Levels: slices.Clone(v.Levels)}
}
func (v *widget) Equal(other schemaext.Value) bool {
	w, ok := other.(*widget)
	return ok && v.Name == w.Name && slices.Equal(v.Levels, w.Levels)
}

type addWidget struct{ Name string }

func (*addWidget) Kind() schemaext.Kind                   { return "example.org/widget/add" }
func (p *addWidget) CloneExtension() ast.ExtensionPayload { return &addWidget{Name: p.Name} }

type service struct{ extensions renderer.Extensions }

func (s service) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	result := renderer.Result{Complete: true}
	for index, node := range request.Nodes {
		if err := ctx.Err(); err != nil {
			return renderer.Result{}, err
		}
		statement, ok := node.(*ast.ExtensionStatement)
		if !ok {
			return renderer.Result{Complete: true, Diagnostics: []renderer.Diagnostic{{
				Problem: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: "node", Feature: "statement", Message: fmt.Sprintf("unsupported node %T", node)},
				Input:   new(index),
			}}}, nil
		}
		sql, err := s.extensions.Render(renderer.ExtensionContext{Target: request.Target}, ast.StatementExtension, statement.Payload)
		if err != nil {
			return renderer.Result{}, err
		}
		fragment := ""
		for _, text := range sql {
			fragment += text + "\n"
		}
		result.Fragments = append(result.Fragments, fragment)
	}
	return result, nil
}

func codec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload) }
	return schemaext.Codec{
		Prototype: &widget{}, Representation: schemaext.Desired, Version: 1,
		Definition: json.RawMessage(`{"name":"string","levels":"ordered string list"}`),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, ok := payload.(*widget)
			if !ok {
				return nil, fmt.Errorf("unexpected model %T", payload)
			}
			return value.Clone(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*widget](data) },
	}
}

func main() {
	if err := verify(); err != nil {
		panic(err)
	}
	fmt.Println("external provider conformance passed")
}

func verify() error {
	handlers, err := renderer.NewExtensions(renderer.TypedHandler(&addWidget{}, ast.StatementExtension,
		func(renderer.ExtensionContext, *addWidget) error { return nil },
		func(_ renderer.ExtensionContext, operation *addWidget) ([]string, error) {
			return []string{"CREATE WIDGET " + operation.Name}, nil
		}))
	if err != nil {
		return err
	}
	runtime, err := engine.New(engine.Provider{ID: "example.org/widget", Codecs: []schemaext.Codec{codec()},
		Targets: []engine.Target{{Name: "widget", Rendering: service{extensions: handlers}, Validation: service{}, SchemaRendering: service{}, Creations: service{}}}})
	if err != nil {
		return err
	}
	result, err := runtime.Render(context.Background(), renderer.Request{Target: "widget", Nodes: []ast.Node{
		&ast.ExtensionStatement{Payload: &addWidget{Name: "sample"}},
	}})
	if err != nil {
		return err
	}
	if result.SQL() != "CREATE WIDGET sample\n" {
		return fmt.Errorf("wrong custom render: %q", result.SQL())
	}
	if err := verifyBatchRefusal(runtime); err != nil {
		return err
	}
	_, err = runtime.Render(context.Background(), renderer.Request{Target: "postgres"})
	if !errors.Is(err, ptaherr.ErrUnsupportedDialect) {
		return fmt.Errorf("unselected target was not refused: %v", err)
	}
	original := &widget{Name: "sample", Levels: []string{"two", "one"}}
	facets, err := schemaext.NewFacets(original)
	if err != nil {
		return err
	}
	encoded, err := runtime.Codecs().EncodeFacets(context.Background(), schemaext.Desired, facets)
	if err != nil {
		return err
	}
	decoded, err := runtime.Codecs().DecodeFacets(context.Background(), schemaext.Desired, encoded)
	if err != nil {
		return err
	}
	value, found, err := schemaext.FacetAs[*widget](decoded, original.Kind())
	if err != nil {
		return err
	}
	if !found || !value.Equal(original) {
		return fmt.Errorf("typed round trip lost the value")
	}
	if err := verifyValidation(runtime, decoded); err != nil {
		return err
	}
	if err := verifySchemaRendering(runtime, decoded); err != nil {
		return err
	}
	if err := verifyTargetScope(decoded); err != nil {
		return err
	}
	if err := verifyCreationProjection(runtime); err != nil {
		return err
	}
	if err := verifyImport(); err != nil {
		return err
	}
	value.Levels[0] = "changed"
	if original.Levels[0] != "two" {
		return fmt.Errorf("round trip exposed aliased state")
	}
	return nil
}

func verifyBatchRefusal(runtime *engine.Runtime) error {
	node := ast.NewRawSQL("unsupported")
	result, err := runtime.Render(context.Background(), renderer.Request{Target: "widget", Nodes: []ast.Node{
		&ast.ExtensionStatement{Payload: &addWidget{Name: "prefix"}}, node,
	}})
	refused, ok := errors.AsType[*renderer.BatchRefusalError](err)
	if !ok || !errors.Is(err, ptaherr.ErrUnsupportedFeature) || len(refused.Diagnostics) != 1 ||
		refused.Diagnostics[0].Input == nil || *refused.Diagnostics[0].Input != 1 || result.Complete || len(result.Fragments) != 0 {
		return fmt.Errorf("completed batch refusal lost its data or exposed output: %+v, %v", result, err)
	}
	rendering, ok := errors.AsType[*ptaherr.RenderError](err)
	if !ok || rendering.Node != node {
		return fmt.Errorf("completed batch refusal lost caller input provenance: %v", err)
	}
	result, err = runtime.Render(context.Background(), renderer.Request{Target: "widget"})
	if err != nil || !result.Complete || len(result.Fragments) != 0 {
		return fmt.Errorf("empty batch was not completed: %+v, %v", result, err)
	}
	return nil
}

type importService struct{ omit bool }

func (s importService) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	result := renderer.Result{Complete: true}
	for _, node := range request.Nodes {
		if err := ctx.Err(); err != nil {
			return renderer.Result{}, err
		}
		switch node := node.(type) {
		case *ast.CreateTableNode:
			result.Fragments = append(result.Fragments, "CREATE WIDGET TABLE "+node.Name+";")
		case *ast.DropTableNode:
			result.Fragments = append(result.Fragments, "DROP WIDGET TABLE "+node.Name+";")
		default:
			return renderer.Result{}, fmt.Errorf("unsupported import node %T", node)
		}
	}
	if s.omit {
		result.Omissions = []renderer.Omission{{Dialect: request.Target, Kind: "table", Name: "items", Reason: "unsupported", Property: "storage"}}
	}
	return result, nil
}

func verifyImport() error {
	ctx := context.Background()
	parser, err := importer.ParserByName("liquibase")
	if err != nil {
		return err
	}
	source := fstest.MapFS{"changelog.xml": {Data: []byte(`<databaseChangeLog><changeSet id="1" author="test"><createTable tableName="items"><column name="id" type="int"/></createTable></changeSet></databaseChangeLog>`)}}
	for _, omit := range []bool{false, true} {
		runtime, err := engine.New(engine.Provider{ID: "example.org/import", Targets: []engine.Target{{Name: "custom", Rendering: importService{omit: omit}}}})
		if err != nil {
			return err
		}
		configured, err := importer.WithRendering(parser, "custom", nil, runtime)
		if err != nil {
			return err
		}
		parsed, err := configured.Parse(ctx, source)
		if omit {
			if err == nil || parsed != nil || !strings.Contains(err.Error(), "cannot carry the whole change") {
				return fmt.Errorf("selected importer lost an omission: %v, %v", parsed, err)
			}
			continue
		}
		if err != nil {
			return err
		}
		if len(parsed.Migrations) != 1 || parsed.Migrations[0].UpSQL != "CREATE WIDGET TABLE items;" || parsed.Migrations[0].DownSQL != "DROP WIDGET TABLE items;" {
			return fmt.Errorf("selected importer lost provider SQL: %+v", parsed)
		}
	}
	return nil
}

func (service) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	result := schemavalidation.Result{Complete: true}
	for _, facets := range request.Schema.FacetSlots() {
		if err := ctx.Err(); err != nil {
			return schemavalidation.Result{}, err
		}
		value, found, err := schemaext.FacetAs[*widget](*facets, (&widget{}).Kind())
		if err != nil {
			return schemavalidation.Result{}, err
		}
		if !found {
			continue
		}
		if strings.TrimSpace(value.Name) == "" || len(value.Levels) == 0 {
			result.Diagnostics = append(result.Diagnostics, schemavalidation.Diagnostic{
				Code: schemavalidation.InvalidSchema, Kind: "widget", Object: value.Name,
				Message: "a widget requires a name and at least one level",
			})
		}
	}
	return result, nil
}

func verifyValidation(runtime *engine.Runtime, decoded schemaext.Facets) error {
	ctx := context.Background()
	request := schemavalidation.Request{Target: "widget", Schema: &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "first", Facets: decoded}, {Name: "second", Facets: decoded}},
	}}
	result, err := runtime.ValidateSchema(ctx, request)
	if err != nil {
		return err
	}
	if err := result.Err("widget"); err != nil {
		return err
	}
	invalid, err := schemaext.NewFacets(&widget{Name: "invalid"})
	if err != nil {
		return err
	}
	request.Schema.Tables[1].Facets = invalid
	result, err = runtime.ValidateSchema(ctx, request)
	if err != nil {
		return err
	}
	if !errors.Is(result.Err("widget"), ptaherr.ErrInvalidSchemaDiff) || len(result.Diagnostics) != 1 || result.Diagnostics[0].Object != "invalid" {
		return fmt.Errorf("whole-schema validation lost the second declaration's refusal: %+v", result)
	}
	request.Target = "postgres"
	_, err = runtime.ValidateSchema(ctx, request)
	if !errors.Is(err, ptaherr.ErrUnsupportedDialect) {
		return fmt.Errorf("unselected validation target was not refused: %v", err)
	}
	return nil
}

func (s service) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	validation, err := s.ValidateSchema(ctx, schemavalidation.Request{
		Target: request.Target, Schema: request.Schema, Capabilities: request.Capabilities, Identifiers: request.Identifiers,
	})
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	result := renderer.SchemaResult{Complete: true, Diagnostics: validation.Diagnostics}
	if len(result.Diagnostics) != 0 {
		return result, nil
	}
	for _, table := range request.Schema.Tables {
		value, found, err := schemaext.FacetAs[*widget](table.Facets, (&widget{}).Kind())
		if err != nil {
			return renderer.SchemaResult{}, err
		}
		if found {
			result.Statements = append(result.Statements, fmt.Sprintf("CREATE WIDGET %q.%q;\n", table.Name, value.Name))
		}
	}
	return result, nil
}

func verifySchemaRendering(runtime *engine.Runtime, decoded schemaext.Facets) error {
	request := renderer.SchemaRequest{Target: "widget", Schema: &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "first", Facets: decoded}, {Name: "second", Facets: decoded}},
	}}
	result, err := runtime.RenderSchema(context.Background(), request)
	if err != nil {
		return err
	}
	if !slices.Equal(result.Statements, []string{"CREATE WIDGET \"first\".\"sample\";\n", "CREATE WIDGET \"second\".\"sample\";\n"}) {
		return fmt.Errorf("whole-schema rendering lost declaration order: %+v", result)
	}
	invalid, err := schemaext.NewFacets(&widget{Name: "invalid"})
	if err != nil {
		return err
	}
	request.Schema.Tables[1].Facets = invalid
	result, err = runtime.RenderSchema(context.Background(), request)
	if !errors.Is(err, ptaherr.ErrInvalidSchemaDiff) || len(result.Statements) != 0 {
		return fmt.Errorf("refused rendering exposed SQL: %+v, %v", result, err)
	}
	request.Target = "postgres"
	_, err = runtime.RenderSchema(context.Background(), request)
	if !errors.Is(err, ptaherr.ErrUnsupportedDialect) {
		return fmt.Errorf("unselected schema renderer was not refused: %v", err)
	}
	return nil
}

type scopedService struct{}

func (scopedService) ValidateSchema(_ context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	if request.Target != "custom" || len(request.Schema.CompositeTypes) != 1 || request.Schema.CompositeTypes[0].Name != "local" {
		return schemavalidation.Result{}, fmt.Errorf("custom scope was not applied before dispatch")
	}
	return schemavalidation.Result{Complete: true}, nil
}

func (s scopedService) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	_, err := s.ValidateSchema(ctx, schemavalidation.Request{Target: request.Target, Schema: request.Schema})
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	return renderer.SchemaResult{Complete: true, Statements: []string{"CREATE TYPE local;"}}, nil
}

func verifyTargetScope(foreign schemaext.Facets) error {
	// No codecs are registered here: the excluded declaration belongs to a
	// different target and must be removed before its payload is inspected.
	runtime, err := engine.New(engine.Provider{ID: "example.org/scoped", Targets: []engine.Target{{
		Name: "custom", Aliases: []string{"custom+wire"}, Validation: scopedService{}, SchemaRendering: scopedService{},
	}}})
	if err != nil {
		return err
	}
	schema := &schemamodel.Database{CompositeTypes: []schemamodel.CompositeType{
		{Name: "foreign", Dialects: []string{"postgres"}, Facets: foreign},
		{Name: "local", Dialects: []string{" CUSTOM+WIRE "}},
	}}
	ctx := context.Background()
	validation := schemavalidation.Request{Target: "custom+wire", Schema: schema}
	if _, err := runtime.ValidateSchema(ctx, validation); err != nil {
		return err
	}
	rendering := renderer.SchemaRequest{Target: "custom+wire", Schema: schema}
	result, err := runtime.RenderSchema(ctx, rendering)
	if err != nil {
		return err
	}
	if !slices.Equal(result.Statements, []string{"CREATE TYPE local;"}) || len(schema.CompositeTypes) != 2 {
		return fmt.Errorf("custom target scope changed source data or lost selected output")
	}
	schema.CompositeTypes[0].Dialects = nil
	_, validationErr := runtime.ValidateSchema(ctx, validation)
	_, renderingErr := runtime.RenderSchema(ctx, rendering)
	if !errors.Is(validationErr, schemaext.ErrUnknownCodec) || !errors.Is(renderingErr, schemaext.ErrUnknownCodec) {
		return fmt.Errorf("unscoped unknown payload was not refused: %v, %v", validationErr, renderingErr)
	}
	return verifyFacetTargetScope(runtime, foreign)
}

func verifyFacetTargetScope(runtime *engine.Runtime, foreign schemaext.Facets) error {
	source, err := foreign.WithTargetScope((&widget{}).Kind(), "postgres")
	if err != nil {
		return err
	}
	target, err := runtime.ResolveTarget("custom+wire")
	if err != nil {
		return err
	}
	selected, err := source.ForTarget(target)
	if err != nil {
		return err
	}
	ctx := context.Background()
	encoded, err := runtime.Codecs().EncodeFacets(ctx, schemaext.Desired, selected)
	if err != nil {
		return err
	}
	decoded, err := runtime.Codecs().DecodeFacets(ctx, schemaext.Desired, encoded)
	if err != nil {
		return err
	}
	if decoded.Len() != 0 || decoded.IsZero() || !slices.Equal(decoded.TargetScope((&widget{}).Kind()), []string{"postgres"}) {
		return fmt.Errorf("facet source binding was lost in an excluded capture")
	}
	schema := &schemamodel.Database{CompositeTypes: []schemamodel.CompositeType{{Name: "local", Facets: decoded}}}
	_, err = runtime.RenderSchema(ctx, renderer.SchemaRequest{Target: "custom+wire", Schema: schema})
	return err
}

func (service) ProjectTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	result := schemaprojection.TableCreationResult{Complete: true}
	for _, table := range request.Tables {
		if err := ctx.Err(); err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		values, err := schemaext.NewFacets(&widget{Name: table.Declaration.Table.Name, Levels: []string{"default"}})
		if err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		result.Tables = append(result.Tables, schemaprojection.TableCreation{Subject: table.Subject, Facets: values})
	}
	return result, nil
}

func verifyCreationProjection(runtime *engine.Runtime) error {
	semantics := identifier.ForDialect("widget")
	subject := objectidentity.NewBuilder(semantics).TableParts("", "sample")
	request := schemaprojection.TableCreationRequest{Target: "widget", Identifiers: semantics,
		Tables: []schemaprojection.TableCreationInput{{Subject: subject, Declaration: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "sample"}}}},
	}
	result, err := runtime.ProjectTableCreations(context.Background(), request)
	if err != nil {
		return err
	}
	if !result.Complete || len(result.Tables) != 1 {
		return fmt.Errorf("creation prediction did not complete")
	}
	value, found, err := schemaext.FacetAs[*widget](result.Tables[0].Facets, (&widget{}).Kind())
	if err != nil {
		return err
	}
	if !found || value.Name != "sample" || !slices.Equal(value.Levels, []string{"default"}) {
		return fmt.Errorf("creation prediction lost provider defaults")
	}
	if !request.Tables[0].Declaration.Table.Facets.IsZero() {
		return fmt.Errorf("creation prediction changed source intent")
	}
	return nil
}
